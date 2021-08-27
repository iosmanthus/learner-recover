package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os/exec"
	"sort"
	"time"

	"github.com/goccy/go-yaml"
	log "github.com/sirupsen/logrus"
)

var (
	config string
)

func init() {
	flag.StringVar(&config, "config", "./config.yaml", "path of config.yaml")
}

type Config struct {
	Voters      []string `yaml:"voters"`
	Learners    []string `yaml:"learners"`
	TikvCtlPath string   `yaml:"tikv-ctl"`
}

type RegionId uint64

type RegionInfos struct {
	stateMap map[RegionId]*RegionState
}

type Result struct {
	*RegionInfos
	Error error
}

func NewRegionInfos() *RegionInfos {
	return &RegionInfos{stateMap: make(map[RegionId]*RegionState)}
}

func (r *RegionInfos) StateMap() map[RegionId]*RegionState {
	return r.stateMap
}

func (r *RegionInfos) UnmarshalJSON(data []byte) error {
	tmp := make(map[string]map[RegionId]*RegionState)

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	if _, ok := tmp["region_infos"]; !ok {
		return fmt.Errorf("missing region_infos field")
	}

	r.stateMap = tmp["region_infos"]
	for id := range r.stateMap {
		r.stateMap[id].ApplyState.Timestamp = time.Now()
	}
	return nil
}

func (r *RegionInfos) Merge(other *RegionInfos) {
	for id, info := range other.stateMap {
		if v, has := r.stateMap[id]; has {
			if info.ApplyState.AppliedIndex > v.ApplyState.AppliedIndex {
				r.stateMap[id] = info
			}
		} else {
			r.stateMap[id] = info
		}
	}
}

type RegionState struct {
	RegionId   RegionId `json:"region_id"`
	ApplyState struct {
		AppliedIndex uint64 `json:"applied_index"`
		Timestamp    time.Time
	} `json:"raft_apply_state"`
}

type ApplyHistory struct {
	history map[RegionId][]*RegionState
	birth   time.Time
}

func NewApplyHistory() *ApplyHistory {
	return &ApplyHistory{
		history: make(map[RegionId][]*RegionState),
		birth:   time.Now(),
	}
}

func (h *ApplyHistory) Update(infos *RegionInfos) {
	for id, state := range infos.StateMap() {
		history := h.history[id]
		if len(history) == 0 || history[len(history)-1].ApplyState.AppliedIndex != state.ApplyState.AppliedIndex {
			h.history[id] = append(h.history[id], state)
		} else {
			h.history[id][len(history)-1] = state
		}
	}
}

func (h *ApplyHistory) RPOQuery(state *RegionState) time.Duration {
	history := h.history[state.RegionId]
	if len(history) == 0 {
		return state.ApplyState.Timestamp.Sub(h.birth)
	}
	i := sort.Search(len(history), func(i int) bool {
		return history[i].ApplyState.AppliedIndex <= state.ApplyState.AppliedIndex
	})
	h.history[state.RegionId] = history[i:]
	return state.ApplyState.Timestamp.Sub(history[i].ApplyState.Timestamp)
}

func NewConfig(path string) (*Config, error) {
	config := &Config{}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

type Fetcher interface {
	Fetch(ctx context.Context, hosts []string) (*RegionInfos, error)
}

type InfoFetcher struct {
	controller string
}

func NewInfoFetcher(controller string) Fetcher {
	return &InfoFetcher{controller}
}

func (f *InfoFetcher) Fetch(ctx context.Context, hosts []string) (*RegionInfos, error) {
	ch := make(chan Result, len(hosts))

	infos := NewRegionInfos()
	for _, host := range hosts {
		go func(host string) {
			cmd := exec.CommandContext(ctx, f.controller, "--host", host, "raft", "region", "--all-regions")
			resp, err := cmd.Output()
			if err != nil {
				ch <- Result{nil, err}
				return
			}

			infos := &RegionInfos{}
			err = json.Unmarshal(resp, infos)
			if err != nil {
				ch <- Result{nil, err}
				return
			}
			ch <- Result{infos, nil}
		}(host)
	}
	for i := 0; i < len(hosts); i++ {
		result := <-ch
		if err := result.Error; err != nil {
			return nil, err
		}
		infos.Merge(result.RegionInfos)
	}
	close(ch)
	return infos, nil
}

type UpdateWorker struct {
	controller string
	hosts      []string
	interval   time.Duration
}

func NewUpdateWorker(controller string, hosts []string, interval time.Duration) *UpdateWorker {
	return &UpdateWorker{controller, hosts, interval}
}

func (w *UpdateWorker) Run(ctx context.Context, ch chan<- Result) {
	fetcher := NewInfoFetcher(w.controller)
	for {
		select {
		case <-ctx.Done():
			ch <- Result{nil, ctx.Err()}
			return
		default:
			infos, err := fetcher.Fetch(ctx, w.hosts)
			if err != nil {
				ch <- Result{nil, err}
				break
			}
			ch <- Result{infos, nil}
		}

		time.Sleep(w.interval)
	}
}

func main() {
	flag.Parse()
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})

	cfg, err := NewConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	history := NewApplyHistory()

	votersInfoUpdater := NewUpdateWorker(cfg.TikvCtlPath, cfg.Voters, time.Second*5)
	learnerInfosUpdater := NewUpdateWorker(cfg.TikvCtlPath, cfg.Learners, time.Second*5)

	voterCh := make(chan Result)
	learnerCh := make(chan Result)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()

	go votersInfoUpdater.Run(ctx, voterCh)
	go learnerInfosUpdater.Run(ctx, learnerCh)

	for {
		select {
		case <-ctx.Done():
			return
		case result := <-voterCh:
			if err := result.Error; err != nil {
				log.Error(err)
				break
			}
			history.Update(result.RegionInfos)
		case result := <-learnerCh:
			if err := result.Error; err != nil {
				log.Error(err)
				break
			}
			region20 := result.StateMap()[20]
			rpo := history.RPOQuery(region20)
			log.Infof("%d: %v", region20.RegionId, rpo)
		}
	}
}
