package rpo

import (
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/iosmanthus/learner-recover/common"

	"github.com/pingcap/tiup/pkg/cluster/spec"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Voters      []string
	Learners    []string
	TikvCtlPath string
	HistoryPath string
	Save        string
	LastFor     time.Duration
}

func NewConfig(path string) (*Config, error) {
	type _Config struct {
		Topology      string            `yaml:"topology"`
		VoterLabels   map[string]string `yaml:"voter-labels"`
		LearnerLabels map[string]string `yaml:"learner-labels"`
		TikvCtlPath   string            `yaml:"tikv-ctl"`
		HistoryPath   string            `yaml:"history-path"`
		Save          string            `yaml:"save"`
		LastFor       string            `yaml:"last-for"`
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	c := &_Config{}
	if err := yaml.Unmarshal(data, c); err != nil {
		return nil, err
	}

	lastFor, err := time.ParseDuration(c.LastFor)
	if err != nil {
		return nil, err
	}

	topo := &spec.Specification{}
	if err = spec.ParseTopologyYaml(c.Topology, topo); err != nil {
		return nil, err
	}

	var (
		voters   []string
		learners []string
	)

	for _, node := range topo.TiKVServers {
		serverLabels, err := node.Labels()
		if err != nil {
			return nil, err
		}

		host := fmt.Sprintf("%s:%v", node.Host, node.Port)

		if common.IsLabelsMatch(c.VoterLabels, serverLabels) {
			voters = append(voters, host)
		} else if common.IsLabelsMatch(c.LearnerLabels, serverLabels) {
			learners = append(learners, host)
		}
	}

	if len(voters) == 0 {
		return nil, errors.New("no voters in the cluster, please check the topology file")
	}
	if len(learners) == 0 {
		return nil, errors.New("no learners in the cluster, please check the topology file")
	}

	return &Config{
		Voters:      voters,
		Learners:    learners,
		TikvCtlPath: c.TikvCtlPath,
		HistoryPath: c.HistoryPath,
		Save:        c.Save,
		LastFor:     lastFor,
	}, nil
}
