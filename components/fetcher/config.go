package fetcher

import (
	"io/ioutil"
	"time"

	"github.com/pingcap/tiup/pkg/cluster/spec"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Save          string
	Topology      *spec.Specification
	LearnerLabels map[string]string
	LastFor       time.Duration
	Interval      time.Duration
	Timeout       time.Duration
}

func NewConfig(path string) (*Config, error) {
	type _Config struct {
		Save          string            `yaml:"save"`
		Topology      string            `yaml:"topology"`
		LearnerLabels map[string]string `yaml:"learner-labels"`
		LastFor       string            `yaml:"last-for"`
		Interval      string            `yaml:"interval"`
		Timeout       string            `yaml:"timeout"`
	}

	c := &_Config{}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if err = yaml.Unmarshal(data, c); err != nil {
		return nil, err
	}

	lastFor, err := time.ParseDuration(c.LastFor)
	if err != nil {
		return nil, err
	}

	interval, err := time.ParseDuration(c.Interval)
	if err != nil {
		return nil, err
	}

	timeout, err := time.ParseDuration(c.Timeout)
	if err != nil {
		return nil, err
	}

	topo := &spec.Specification{}
	if err := spec.ParseTopologyYaml(c.Topology, topo); err != nil {
		return nil, err
	}

	return &Config{
		Save:          c.Save,
		Topology:      topo,
		LearnerLabels: c.LearnerLabels,
		LastFor:       lastFor,
		Interval:      interval,
		Timeout:       timeout,
	}, nil
}
