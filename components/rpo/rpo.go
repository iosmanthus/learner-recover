package rpo

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os/exec"
	"time"

	"github.com/iosmanthus/learner-recover/common"

	log "github.com/sirupsen/logrus"
)

type ApplyHistory struct {
	History map[common.RegionId][]*common.RegionState `json:"history"`
	Birth   time.Time                                 `json:"birth"`
}

func NewApplyHistory() *ApplyHistory {
	return &ApplyHistory{
		History: make(map[common.RegionId][]*common.RegionState),
		Birth:   time.Now(),
	}
}

func FromFile(path string) (*ApplyHistory, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	history := &ApplyHistory{}
	if err = json.Unmarshal(data, history); err != nil {
		return nil, err
	}

	return history, nil
}

func (h *ApplyHistory) Update(infos *common.RegionInfos) {
	for id, state := range infos.StateMap {
		history := h.History[id]
		if len(history) == 0 || history[len(history)-1].ApplyState.AppliedIndex != state.ApplyState.AppliedIndex {
			h.History[id] = append(h.History[id], state)
		} else {
			h.History[id][len(history)-1] = state
		}
	}
}

func (h *ApplyHistory) RPOQuery(q *common.RegionState) time.Time {
	history := h.History[q.RegionId]
	if len(history) == 0 {
		return h.Birth
	}

	var index int
	for i, state := range history {
		index = i
		if state.ApplyState.AppliedIndex >= q.ApplyState.AppliedIndex {
			break
		}
	}
	h.History[q.RegionId] = history[index:]
	return history[index].ApplyState.Timestamp
}

func (h *ApplyHistory) Save(path string) error {
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, data, 0644)
}

type MaxApplyIndex struct{}

func (m MaxApplyIndex) Merge(a *common.RegionInfos, b *common.RegionInfos) *common.RegionInfos {
	for id, info := range b.StateMap {
		if v, has := a.StateMap[id]; has {
			if info.ApplyState.AppliedIndex > v.ApplyState.AppliedIndex {
				a.StateMap[id] = info
			}
		} else {
			a.StateMap[id] = info
		}
	}
	return a
}

type LocalTiKVCtl struct {
	controller string
	host       string
}

func NewLocalTiKVCtl(controller, host string) *LocalTiKVCtl {
	return &LocalTiKVCtl{controller, host}
}

func (f *LocalTiKVCtl) Fetch(ctx context.Context) (*common.RegionInfos, error) {
	applyTS := time.Now()
	cmd := exec.CommandContext(ctx, f.controller, "--host", f.host, "raft", "region", "--all-regions")
	resp, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	infos := &common.RegionInfos{}
	if err = json.Unmarshal(resp, infos); err != nil {
		return nil, err
	}

	for id := range infos.StateMap {
		infos.StateMap[id].ApplyState.Timestamp = applyTS
	}

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

func (w *UpdateWorker) Run(ctx context.Context, ch chan<- common.Result) {
	collector := common.NewRegionCollector()
	for {
		select {
		case <-ctx.Done():
			ch <- common.Result{Error: ctx.Err()}
			return
		default:
			var fetchers []common.Fetcher
			for _, host := range w.hosts {
				fetcher := NewLocalTiKVCtl(w.controller, host)
				fetchers = append(fetchers, fetcher)
			}

			infos, err := collector.Collect(ctx, fetchers, MaxApplyIndex{})
			if err != nil {
				ch <- common.Result{Error: err}
				break
			}
			ch <- common.Result{RegionInfos: infos}
			time.Sleep(w.interval)
		}
	}
}

type Generator struct {
	config  *Config
	history *ApplyHistory
}

func NewGenerator(config *Config) *Generator {
	var (
		history *ApplyHistory
		err     error
	)
	if history, err = FromFile(config.HistoryPath); err != nil {
		history = NewApplyHistory()
	}
	return &Generator{config, history}
}

type RPO struct {
	Lag      time.Duration `json:"lag"`
	SafeTime time.Time     `json:"safe-time"`
}

func (g *Generator) Gen(ctx context.Context) error {
	config := g.config
	votersInfoUpdater := NewUpdateWorker(config.TikvCtlPath, config.Voters, time.Millisecond*500)
	learnerInfosUpdater := NewUpdateWorker(config.TikvCtlPath, config.Learners, time.Second*2)

	voterCh := make(chan common.Result)
	learnerCh := make(chan common.Result)
	persistCh := make(chan struct{})

	ctx, cancel := context.WithTimeout(ctx, config.LastFor)
	defer cancel()

	go votersInfoUpdater.Run(ctx, voterCh)
	go learnerInfosUpdater.Run(ctx, learnerCh)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				persistCh <- struct{}{}
				time.Sleep(time.Second * 1)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case result := <-voterCh:
			if err := result.Error; err != nil {
				log.Error(err)
				break
			}
			g.history.Update(result.RegionInfos)
		case result := <-learnerCh:
			if err := result.Error; err != nil {
				log.Error(err)
				break
			}

			max := time.Duration(0)
			var safeTime time.Time
			for _, info := range result.StateMap {
				ts := g.history.RPOQuery(info)
				if lag := info.ApplyState.Timestamp.Sub(ts); lag >= max {
					max = lag
					safeTime = ts
				}
			}

			rpo := &RPO{max, safeTime}
			data, err := json.Marshal(rpo)
			if err != nil {
				log.Error(err)
				break
			}

			ioutil.WriteFile(config.Save, data, 0644)
			log.WithFields(map[string]interface{}{
				"lag":      rpo.Lag,
				"safeTime": rpo.SafeTime,
			}).Info("RPO updated")
		case <-persistCh:
			if err := g.history.Save(config.HistoryPath); err != nil {
				log.Error(err)
			}
		}
	}
}
