package recover

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"

	"github.com/pingcap/tiup/pkg/cluster/spec"

	"github.com/iosmanthus/learner-recover/common"

	log "github.com/sirupsen/logrus"
)

type ResolveConflicts struct {
	conflicts []*common.RegionState
}

func NewResolveConflicts() *ResolveConflicts {
	return &ResolveConflicts{}
}

func (r *ResolveConflicts) ResolveConflicts(ctx context.Context, c *Config) error {
	for _, conflict := range r.conflicts {
		cmd := exec.CommandContext(ctx,
			"ssh", "-p", fmt.Sprintf("%v", c.SSHPort), fmt.Sprintf("%s@%s", c.User, conflict.Host),
			c.TiKVCtl.Dest, "--db", fmt.Sprintf("%s/db", conflict.DataDir), "tombstone", "--force", "-r", fmt.Sprintf("%v", conflict.RegionId))

		_, err := common.Run(cmd)

		if err != nil {
			return err
		}
	}
	return nil
}

func isOverlap(a *common.RegionState, b *common.RegionState) bool {
	m, n := a.LocalState.Region.StartKey, a.LocalState.Region.EndKey
	p, q := b.LocalState.Region.StartKey, b.LocalState.Region.EndKey
	return (n == "" || strings.Compare(n, p) > 0) && (q == "" || strings.Compare(q, m) > 0)
}

func (c *ResolveConflicts) Merge(a *common.RegionInfos, b *common.RegionInfos) *common.RegionInfos {
	if len(a.StateMap) == 0 {
		return b
	}

	info := common.NewRegionInfos()
	for _, state := range a.StateMap {
		for _, other := range b.StateMap {
			if isOverlap(state, other) {
				version1 := state.LocalState.Region.RegionEpoch.Version
				version2 := other.LocalState.Region.RegionEpoch.Version
				if version1 > version2 || (version1 == version2) && state.ApplyState.AppliedIndex >= other.ApplyState.AppliedIndex {
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
			defer wg.Done()

			path := fmt.Sprintf("%s/%s/db", node.DeployDir, node.DataDir)
			log.Infof("Dropping raft logs of TiKV server on %s:%v:%s", node.Host, node.Port, path)
			cmd := exec.CommandContext(ctx,
				"ssh", "-p", fmt.Sprintf("%v", config.SSHPort), fmt.Sprintf("%s@%s", config.User, node.Host),
				config.TiKVCtl.Dest, "--db", path, "unsafe-recover", "drop-unapplied-raftlog", "--all-regions")
			_, err := common.Run(cmd)
			ch <- err
		}(node)
	}

	for i := 0; i < len(config.Nodes); i++ {
		if err := <-ch; err != nil {
			return err
		}
	}

	return nil
}

func (r *ClusterRescuer) promoteLearner(ctx context.Context) error {
	config := r.config

	ch := make(chan error, len(config.Nodes))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(len(config.Nodes))
	defer wg.Wait()

	for _, node := range config.Nodes {
		go func(node *spec.TiKVSpec) {
			defer wg.Done()

			// remove-fail-stores --promote-learner --all-regions
			log.Infof("Promoting learners of TiKV server on %s:%v", node.Host, node.Port)

			var stores string
			for i, store := range config.RecoverInfoFile.StoreIDs {
				if i == 0 {
					stores += fmt.Sprintf("%v", store)
				} else {
					stores += fmt.Sprintf(",%v", store)
				}
			}

			cmd := exec.CommandContext(ctx,
				"ssh", "-p", fmt.Sprintf("%v", config.SSHPort), fmt.Sprintf("%s@%s", config.User, node.Host),
				config.TiKVCtl.Dest, "--db", fmt.Sprintf("%s/db", node.DataDir), "unsafe-recover",
				"remove-fail-stores", "--promote-learner", "--all-regions", "-s", stores)

			_, err := common.Run(cmd)
			ch <- err
		}(node)
	}

	for _, node := range config.Nodes {
		if err := <-ch; err != nil {
			log.Errorf("Fail to promote learners of TiKV server on %s:%v: %v", node.Host, node.Port, err)
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
	log.Infof("fetching region infos from: %s", c.Host)
	cmd := exec.CommandContext(ctx,
		"ssh", "-p", fmt.Sprintf("%v", c.SSHPort), fmt.Sprintf("%s@%s", c.User, c.Host),
		c.Controller, "--db", fmt.Sprintf("%s/db", c.DataDir), "raft", "region", "--all-regions")

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
		infos.StateMap[id].DataDir = c.DataDir
	}

	return infos, nil
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
			DataDir:    fmt.Sprintf("%s/%s", node.DeployDir, node.DataDir),
			User:       c.User,
			Host:       node.Host,
			SSHPort:    c.SSHPort,
		}
		fetchers = append(fetchers, fetcher)
	}

	log.Info("fetching region infos")
	resolver := NewResolveConflicts()

	_, err = collector.Collect(ctx, fetchers, resolver)
	if err != nil {
		return err
	}

	log.Warn("resolving region conflicts")
	err = resolver.ResolveConflicts(ctx, c)
	if err != nil {
		return err
	}

	return r.promoteLearner(ctx)
}
