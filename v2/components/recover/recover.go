package recover

import (
	"context"
	"fmt"
	"os/exec"
	"sync"

	log "github.com/sirupsen/logrus"
)

type Recover interface {
	UnsafeRecover

	Execute(ctx context.Context) error
	Prepare(ctx context.Context) error
	Stop(ctx context.Context) error
	RecoverPD(ctx context.Context) error
	Finish(ctx context.Context) error
}

type UnsafeRecover interface {
	UnsafeRecover(ctx context.Context) error
}

type ClusterRescuer struct {
	config *Config
}

func NewClusterRescuer(config *Config) *ClusterRescuer {
	return &ClusterRescuer{config}
}

func (r *ClusterRescuer) Prepare(ctx context.Context) error {
	config := r.config

	ch := make(chan error, len(config.Nodes))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(len(config.Nodes))
	defer wg.Wait()

	for _, node := range config.Nodes {
		go func(host string) {
			defer wg.Done()
			path := fmt.Sprintf("%s@%s:%s", config.User, host, config.TiKVCtl.Dest)

			log.Infof("Sending tikv-ctl to %s", host)
			cmd := exec.CommandContext(ctx, "scp",
				"-P",
				fmt.Sprintf("%v", config.SSHPort),
				config.TiKVCtl.Src,
				path)
			err := cmd.Run()
			ch <- err
		}(node.Host)
	}

	for _, node := range config.Nodes {
		if err := <-ch; err != nil {
			log.Errorf("Fail to send tikv-ctl to %s", node.Host)
			return err
		}
	}

	return nil
}

func (r *ClusterRescuer) Stop(ctx context.Context) error {
	config := r.config

	ch := make(chan error, len(config.Nodes))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	wg.Add(len(config.Nodes))
	defer wg.Wait()

	for _, node := range config.Nodes {
		go func(host string, port int) {
			log.Infof("Stoping TiKV server on %s:%v", host, port)
			cmd := exec.CommandContext(ctx,
				"ssh", "-p", fmt.Sprintf("%v", config.SSHPort), fmt.Sprintf("%s@%s", config.User, host),
				"sudo", "systemctl", "disable", "--now", fmt.Sprintf("tikv-%v.serivce", port))
			err := cmd.Run()
			ch <- err
		}(node.Host, node.Port)
	}

	for _, node := range config.Nodes {
		if err := <-ch; err != nil {
			log.Errorf("Fail to stop TiKV server on %s:%v", node.Host, node.Port)
			return err
		}
	}

	return nil
}

func (r *ClusterRescuer) Execute(ctx context.Context) error {
	err := r.Prepare(ctx)
	if err != nil {
		return err
	}

	err = r.Stop(ctx)
	if err != nil {
		return err
	}

	err = r.UnsafeRecover(ctx)
	if err != nil {
		return err
	}

	return nil
}
