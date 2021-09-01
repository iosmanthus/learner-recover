package recover

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/pingcap/tiup/pkg/cluster/spec"
	"os/exec"
	"strings"
	"sync"

	"github.com/iosmanthus/learner-recover/common"

	log "github.com/sirupsen/logrus"
)

type ResolveConflicts struct {
	conflicts []*common.RegionState
}

func NewResolveConflicts() *ResolveConflicts {
	return &ResolveConflicts{}
}

func isOverlap(a *common.RegionState, b *common.RegionState) bool {
	m, n := a.LocalSate.Region.StartKey, a.LocalSate.Region.EndKey
	p, q := b.LocalSate.Region.StartKey, b.LocalSate.Region.EndKey
	return (n == "" || strings.Compare(n, p) > 0) && (q == "" || strings.Compare(q, m) > 0)
}

func (c *ResolveConflicts) Merge(a *common.RegionInfos, b *common.RegionInfos) *common.RegionInfos {
	info := common.NewRegionInfos()
	for _, state := range a.StateMap {
		for _, other := range b.StateMap {
			if isOverlap(state, other) {
				if state.LocalSate.Region.RegionEpoch.Version > other.LocalSate.Region.RegionEpoch.Version ||
					state.ApplyState.AppliedIndex >= other.ApplyState.AppliedIndex {
					info.StateMap[state.RegionId] = state
					c.conflicts = append(c.conflicts, other)
				} else {
					info.StateMap[other.RegionId] = other
					c.conflicts = append(c.conflicts, state)
				}
			} else {
				info.StateMap[state.RegionId] = state
				info.StateMap[other.RegionId] = other
			}
		}
	}
	return info
}

func (r *ClusterRescuer) dropLogs(ctx context.Context) error {
	config := r.config

	ch := make(chan error, len(config.Nodes))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(len(config.Nodes))
	defer wg.Wait()

	for _, node := range config.Nodes {
		go func(node *spec.TiKVSpec) {
			log.Infof("Dropping raft logs of TiKV server on %s:%v", node.Host, node.Port)
			cmd := exec.CommandContext(ctx,
				"ssh", "-p", fmt.Sprintf("%v", config.SSHPort), fmt.Sprintf("%s@%s", config.User, node.Host),
				config.TiKVCtl.Dest, "--db", fmt.Sprintf("%s/db", node.DataDir), "unsafe-recover", "drop-unapplied-raftlog", "--all-regions")
			err := cmd.Run()
			ch <- err
		}(node)
	}

	for _, node := range config.Nodes {
		if err := <-ch; err != nil {
			log.Errorf("Fail to dropping raft logs of TiKV server on %s:%v", node.Host, node.Port)
			return err
		}
	}

	return nil
}

type RemoteTiKVCtl struct {
	Controller string
	DataDir    string
	User       string
	Host       string
	SSHPort    int
}

func (c *RemoteTiKVCtl) Fetch(ctx context.Context) (*common.RegionInfos, error) {
	cmd := exec.CommandContext(ctx,
		"ssh", "-p", fmt.Sprintf("%v", c.SSHPort), fmt.Sprintf("%s@%s", c.User, c.Host),
		c.Controller, "--data-dir", fmt.Sprintf("%s/db", c.DataDir), "raft", "region", "--all-regions")

	resp, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	infos := &common.RegionInfos{}
	if err = json.Unmarshal(resp, infos); err != nil {
		return nil, err
	}

	for id := range infos.StateMap {
		infos.StateMap[id].Host = c.Host
	}

	return nil, nil
}

func (r *ClusterRescuer) UnsafeRecover(ctx context.Context) error {
	c := r.config

	err := r.dropLogs(ctx)
	if err != nil {
		return err
	}

	collector := common.NewRegionCollector()

	var fetchers []common.Fetcher
	for _, node := range c.Nodes {
		fetcher := &RemoteTiKVCtl{
			Controller: c.TiKVCtl.Dest,
			DataDir:    node.DataDir,
			User:       c.User,
			Host:       node.Host,
			SSHPort:    c.SSHPort,
		}
		fetchers = append(fetchers, fetcher)
	}

	resolver := NewResolveConflicts()

	info, err := collector.Collect(ctx, fetchers, resolver)
	if err != nil {
		return err
	}

	spew.Dump(info)
	spew.Dump(resolver.conflicts)

	return nil
}
