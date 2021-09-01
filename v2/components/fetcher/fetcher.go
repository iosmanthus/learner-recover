package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"time"

	"github.com/iosmanthus/learner-recover/common"

	"github.com/pingcap/tiup/pkg/cluster/spec"
	prom "github.com/prometheus/client_golang/api"
	promapi "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
	"gopkg.in/resty.v1"
)

type Fetcher interface {
	Fetch(ctx context.Context) (*common.RecoverInfo, error)
}

type RecoverInfoFetcher struct {
	pdServers    []*spec.PDSpec
	promDriver   promapi.API
	masterLabels map[string]string
	timeout      time.Duration
}

func NewRecoverInfoFetcher(
	topology *spec.Specification, masterLabels map[string]string, timeout time.Duration,
) (*RecoverInfoFetcher, error) {
	if len(topology.PDServers) == 0 || len(topology.TiKVServers) == 0 || len(topology.Monitors) == 0 {
		return nil, errors.New("invalid topology")
	}

	monitor := topology.Monitors[0]
	client, err := prom.NewClient(prom.Config{
		Address: fmt.Sprintf("http://%s:%v", monitor.Host, monitor.Port),
	})
	if err != nil {
		return nil, err
	}

	return &RecoverInfoFetcher{
		pdServers:    topology.PDServers,
		promDriver:   promapi.NewAPI(client),
		masterLabels: masterLabels,
		timeout:      timeout,
	}, nil
}

type getStores struct {
	Stores []struct {
		ID     uint64
		Labels map[string]string
	}
}

func (g *getStores) UnmarshalJSON(data []byte) error {
	type _getStores struct {
		Stores []struct {
			Store struct {
				ID     uint64 `json:"id"`
				Labels []struct {
					Key   string `json:"key"`
					Value string `json:"value"`
				} `json:"labels"`
			} `json:"store"`
		} `json:"stores"`
	}

	t := &_getStores{}
	if err := json.Unmarshal(data, t); err != nil {
		return err
	}

	stores := t.Stores
	for i := range stores {
		labelsMap := make(map[string]string)
		for _, label := range stores[i].Store.Labels {
			labelsMap[label.Key] = label.Value
		}
		g.Stores = append(g.Stores, struct {
			ID     uint64
			Labels map[string]string
		}{ID: stores[i].Store.ID, Labels: labelsMap})
	}

	return nil
}

func (f *RecoverInfoFetcher) fetchStoreIDs(ctx context.Context) ([]uint64, error) {
	client := resty.New()
	firstPD := f.pdServers[0]

	resp, err := client.R().
		SetContext(ctx).Get(fmt.Sprintf("http://%s:%v/pd/api/v1/stores", firstPD.Host, firstPD.ClientPort))

	if err != nil {
		return nil, err
	}

	stores := &getStores{}
	if err = json.Unmarshal(resp.Body(), stores); err != nil {
		return nil, err
	}

	storeIDs := make([]uint64, 0, len(stores.Stores))
	for _, store := range stores.Stores {
		if common.IsLabelsMatch(f.masterLabels, store.Labels) {
			storeIDs = append(storeIDs, store.ID)
		}
	}

	return storeIDs, nil
}

func (f *RecoverInfoFetcher) fetchClusterID(ctx context.Context) (string, error) {
	q := "pd_cluster_metadata"
	value, _, err := f.promDriver.Query(ctx, q, time.Now())
	if err != nil {
		return "", err
	}

	if samples, ok := value.(model.Vector); ok && len(samples) > 0 {
		idString := string(samples[len(samples)-1].Metric["type"])
		return idString[len("cluster"):], nil
	}

	return "", nil
}

func (f *RecoverInfoFetcher) fetchAllocID(ctx context.Context) (uint64, error) {
	q := "pd_cluster_id"
	value, _, err := f.promDriver.Query(ctx, q, time.Now())
	if err != nil {
		return 0, err
	}

	if samples, ok := value.(model.Vector); ok && len(samples) > 0 {
		return uint64(samples[len(samples)-1].Value) + math.MaxUint32, nil
	}

	return 0, nil
}

type Error struct {
	Errors []error
}

func (e *Error) Append(err error) {
	e.Errors = append(e.Errors, err)
}

func (e Error) Error() string {
	s := ""
	for _, e := range e.Errors {
		s += e.Error() + " "
	}

	return s
}
func (f *RecoverInfoFetcher) Fetch(ctx context.Context) (*common.RecoverInfo, error) {
	e := Error{}
	storeIDs, err := f.fetchStoreIDs(ctx)
	if err != nil {
		e.Append(err)
	}

	clusterID, err := f.fetchClusterID(ctx)
	if err != nil {
		e.Append(err)
	}

	allocID, err := f.fetchAllocID(ctx)
	if err != nil {
		e.Append(err)
	}

	if len(e.Errors) > 0 {
		err = e
	}

	return &common.RecoverInfo{
		StoreIDs:  storeIDs,
		ClusterID: clusterID,
		AllocID:   allocID,
	}, err
}

type Updater interface {
	Init() error
	Update(context.Context) error
}

type RecoverInfoUpdater struct {
	path    string
	state   common.RecoverInfo
	fetcher Fetcher

	repeat   int
	timeout  time.Duration
	interval time.Duration
}

func NewRecoverInfoUpdater(config *Config) (*RecoverInfoUpdater, error) {
	fetcher, err := NewRecoverInfoFetcher(config.Topology, config.MasterLabels, config.Timeout)
	if err != nil {
		return nil, err
	}

	return &RecoverInfoUpdater{
		state:    common.RecoverInfo{},
		path:     config.Save,
		repeat:   config.Repeat,
		timeout:  config.Timeout,
		interval: config.Interval,
		fetcher:  fetcher,
	}, nil
}

func (u *RecoverInfoUpdater) Init() error {
	data, err := ioutil.ReadFile(u.path)
	if err != nil {
		log.Warnf("%v is not exist", u.path)
		return nil
	}

	info := common.RecoverInfo{}
	if err = json.Unmarshal(data, &info); err != nil {
		return err
	}

	u.state = info
	return nil
}

func (u *RecoverInfoUpdater) Update(ctx context.Context) error {

	for i := 0; i < u.repeat; i++ {
		ctx, cancel := context.WithTimeout(ctx, u.timeout)
		info, err := u.fetcher.Fetch(ctx)
		if err != nil {
			log.Error(err)
		}

		if info.ClusterID != "" {
			u.state.ClusterID = info.ClusterID
		}
		if info.AllocID != 0 {
			u.state.AllocID = info.AllocID
		}
		if info.StoreIDs != nil {
			u.state.StoreIDs = info.StoreIDs
		}

		if !info.IsEmpty() {
			data, _ := json.Marshal(u.state)

			ioutil.WriteFile(u.path, data, 0644)
			log.Infof("sync recover info successfully, saved to %v", u.path)
		}

		cancel()
		time.Sleep(u.interval)
	}
	return nil
}
