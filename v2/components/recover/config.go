package recover

import (
	"encoding/json"
	"io/ioutil"

	"github.com/iosmanthus/learner-recover/common"

	"github.com/pingcap/tiup/pkg/cluster/spec"
	"gopkg.in/yaml.v3"
)

type Config struct {
	ClusterVersion  string
	ClusterName     string
	User            string
	SSHPort         int
	Nodes           []*spec.TiKVSpec
	NewTopology     string
	JoinTopology    string
	RecoverInfoFile *common.RecoverInfo
	TiKVCtl         struct {
		Src  string
		Dest string
	}
	PDRecoverPath string
}

func NewConfig(path string) (*Config, error) {
	type _Config struct {
		ClusterVersion  string            `yaml:"cluster-version"`
		ClusterName     string            `yaml:"cluster-name"`
		OldTopology     string            `yaml:"old-topology"`
		NewTopology     string            `yaml:"new-topology"`
		JoinTopology    string            `yaml:"join-topology"`
		RecoverInfoFile string            `yaml:"recover-info-file"`
		ZoneLabels      map[string]string `yaml:"zone-labels"`
		TiKVCtl         struct {
			Src  string `yaml:"src"`
			Dest string `yaml:"dest"`
		} `yaml:"tikv-ctl"`
		PDRecoverPath string `yaml:"pd-recover-path"`
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	c := &_Config{}
	if err = yaml.Unmarshal(data, c); err != nil {
		return nil, err
	}

	topo := &spec.Specification{}
	if err := spec.ParseTopologyYaml(c.OldTopology, topo); err != nil {
		return nil, err
	}

	data, err = ioutil.ReadFile(c.RecoverInfoFile)
	if err != nil {
		return nil, err
	}

	info := &common.RecoverInfo{}
	if err = json.Unmarshal(data, info); err != nil {
		return nil, err
	}

	var nodes []*spec.TiKVSpec
	for _, tikv := range topo.TiKVServers {
		serverLabels, err := tikv.Labels()
		if err != nil {
			return nil, err
		}
		if common.IsLabelsMatch(c.ZoneLabels, serverLabels) {
			nodes = append(nodes, tikv)
		}
	}

	return &Config{
		ClusterVersion:  c.ClusterVersion,
		ClusterName:     c.ClusterName,
		User:            topo.GlobalOptions.User,
		SSHPort:         topo.GlobalOptions.SSHPort,
		Nodes:           nodes,
		NewTopology:     c.NewTopology,
		JoinTopology:    c.JoinTopology,
		RecoverInfoFile: info,
		TiKVCtl: struct {
			Src  string
			Dest string
		}{
			Src:  c.TiKVCtl.Src,
			Dest: c.TiKVCtl.Dest,
		},
		PDRecoverPath: c.PDRecoverPath,
	}, nil
}
