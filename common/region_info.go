package common

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type RegionId uint64

type RegionInfos struct {
	StateMap map[RegionId]*RegionState
}

func NewRegionInfos() *RegionInfos {
	return &RegionInfos{StateMap: make(map[RegionId]*RegionState)}
}

type Result struct {
	*RegionInfos
	Error error
}

func (r *RegionInfos) UnmarshalJSON(data []byte) error {
	tmp := make(map[string]map[RegionId]*RegionState)

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	if _, ok := tmp["region_infos"]; !ok {
		return fmt.Errorf("missing region_infos field")
	}

	r.StateMap = tmp["region_infos"]

	return nil
}

type RegionState struct {
	RegionId   RegionId `json:"region_id"`
	Host       string
	DataDir    string
	ApplyState struct {
		AppliedIndex uint64    `json:"applied_index"`
		Timestamp    time.Time `json:"timestamp"`
	} `json:"raft_apply_state"`
	LocalSate struct {
		Region struct {
			StartKey    string `json:"start_key"`
			EndKey      string `json:"end_key"`
			RegionEpoch struct {
				Version int `json:"version"`
			} `json:"region_epoch"`
		} `json:"region"`
	} `json:"region_local_state"`
}

type Aggregator interface {
	Merge(a *RegionInfos, b *RegionInfos) *RegionInfos
}

type Fetcher interface {
	Fetch(ctx context.Context) (*RegionInfos, error)
}

type Collector interface {
	Collect(ctx context.Context, fetchers []Fetcher, m Aggregator) (*RegionInfos, error)
}

type RegionCollector struct{}

func NewRegionCollector() Collector {
	return &RegionCollector{}
}

func (f *RegionCollector) Collect(ctx context.Context, fetchers []Fetcher, m Aggregator) (*RegionInfos, error) {
	ch := make(chan Result, len(fetchers))

	infos := NewRegionInfos()
	for _, fetcher := range fetchers {
		go func(fetcher Fetcher) {
			i, err := fetcher.Fetch(ctx)
			if err != nil {
				ch <- Result{Error: err}
				return
			}
			ch <- Result{RegionInfos: i}
		}(fetcher)
	}

	for i := 0; i < len(fetchers); i++ {
		result := <-ch
		if err := result.Error; err != nil {
			return nil, err
		}
		infos = m.Merge(infos, result.RegionInfos)
	}

	return infos, nil
}
