package rpo

import (
	"io/ioutil"
	"time"

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
		Voters      []string `yaml:"voters"`
		Learners    []string `yaml:"learners"`
		TikvCtlPath string   `yaml:"tikv-ctl"`
		HistoryPath string   `yaml:"history-path"`
		Save        string   `yaml:"save"`
		LastFor     string   `yaml:"last-for"`
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

	return &Config{
		Voters:      c.Voters,
		Learners:    c.Learners,
		TikvCtlPath: c.TikvCtlPath,
		HistoryPath: c.HistoryPath,
		Save:        c.Save,
		LastFor:     lastFor,
	}, nil
}
