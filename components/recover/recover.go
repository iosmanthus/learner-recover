package recover

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/resty.v1"
)

type Recover interface {
	UnsafeRecover

	Execute(ctx context.Context) error
	Prepare(ctx context.Context) error
	Stop(ctx context.Context) error
	RebuildPD(ctx context.Context) error
	Finish(ctx context.Context) error
}

type UnsafeRecover interface {
	UnsafeRecover(ctx context.Context) error
}

type ClusterRescuer struct {
	config *Config
}

func NewClusterRescuer(config *Config) Recover {
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
			defer wg.Done()

			log.Infof("Stoping TiKV server on %s:%v", host, port)
			cmd := exec.CommandContext(ctx,
				"ssh", "-p", fmt.Sprintf("%v", config.SSHPort), fmt.Sprintf("%s@%s", config.User, host),
				"sudo", "systemctl", "disable", "--now", fmt.Sprintf("tikv-%v.service", port))
			err := cmd.Run()
			ch <- err
		}(node.Host, node.Port)
	}

	for _, node := range config.Nodes {
		if err := <-ch; err != nil {
			log.Errorf("Fail to stop TiKV server on %s:%v: %v", node.Host, node.Port, err)
			return err
		}
	}

	return nil
}

func logHelper(output []byte, err error) {
	out := string(output)
	if err != nil {
		log.Warn("%s: %v", out, err)
	} else {
		log.Info(out)
	}
}

func (r *ClusterRescuer) RebuildPD(ctx context.Context) error {
	c := r.config

	log.Info("Rebuilding PD server")

	cmd := exec.CommandContext(ctx, "tiup", "cluster", "deploy", "-y", c.ClusterName, c.ClusterVersion, c.NewTopology.Path)
	output, err := cmd.CombinedOutput()
	logHelper(output, err)

	cmd = exec.CommandContext(ctx, "tiup", "cluster", "start", "-y", c.ClusterName)
	output, err = cmd.CombinedOutput()
	logHelper(output, err)

	// PDRecover
	pdServer := c.NewTopology.PDServers[0]
	cmd = exec.CommandContext(ctx, c.PDRecoverPath,
		"-endpoints", fmt.Sprintf("http://%s:%v", pdServer.Host, pdServer.ClientPort),
		"-cluster-id", c.RecoverInfoFile.ClusterID, "-alloc-id", fmt.Sprintf("%v", c.RecoverInfoFile.AllocID))
	output, err = cmd.CombinedOutput()
	logHelper(output, err)

	if err != nil {
		log.Error("Fail to rebuild PD server")
		return err
	}

	cmd = exec.CommandContext(ctx, "tiup", "cluster", "restart", "-y", c.ClusterName)
	output, err = cmd.CombinedOutput()
	logHelper(output, err)

	if err != nil {
		log.Error("Fail to rebuild PD server")
		return err
	}

	client := resty.New()
	for {
		log.Info("Waiting PD server online")
		resp, err := client.R().SetContext(ctx).Get(fmt.Sprintf("http://%s:%v/pd/api/v1/config/replicate", pdServer.Host, pdServer.ClientPort))
		if err == nil && resp.StatusCode() == http.StatusOK {
			break
		}
		time.Sleep(time.Second * 1)
	}

	return nil
}

func (r *ClusterRescuer) Finish(ctx context.Context) error {
	c := r.config
	log.Info("Joining the TiKV servers")
	cmd := exec.CommandContext(ctx, "tiup", "cluster", "scale-out", "-y", c.ClusterName, c.JoinTopology)
	err := cmd.Run()
	return err
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

	err = r.RebuildPD(ctx)
	if err != nil {
		return err
	}

	return r.Finish(ctx)
}
