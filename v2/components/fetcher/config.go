package fetcher

import (
	"io/ioutil"
	"math"
	"time"

	"github.com/pingcap/tiup/pkg/cluster/spec"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Save         string
	Topology     *spec.Specification
	MasterLabels map[string]string
	Repeat       int
	Interval     time.Duration
	Timeout      time.Duration
}

func NewConfig(path string) (*Config, error) {
	type _Config struct {
		Save         string            `yaml:"save"`
		Topology     string            `yaml:"topology"`
		MasterLabels map[string]string `yaml:"master-labels"`
		Repeat       int               `yaml:"repeat"`
		Interval     string            `yaml:"interval"`
		Timeout      string            `yaml:"timeout"`
	}

	c := &_Config{}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if err = yaml.Unmarshal(data, c); err != nil {
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

	if c.Repeat == 0 {
		c.Repeat = math.MaxInt64
	}

	return &Config{
		Save:         c.Save,
		Topology:     topo,
		MasterLabels: c.MasterLabels,
		Repeat:       c.Repeat,
		Interval:     interval,
		Timeout:      timeout,
	}, nil
}
