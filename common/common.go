package common

import (
	"os/exec"

	log "github.com/sirupsen/logrus"
)

func IsLabelsMatch(labels map[string]string, match map[string]string) bool {
	for k, v := range labels {
		if get, ok := match[k]; !ok || get != v {
			return false
		}
	}
	return true
}

func Run(cmd *exec.Cmd) (string, error) {
	output, err := cmd.CombinedOutput()
	out := string(output)

	if err != nil {
		log.Warnf("%s: %v", out, err)
	} else {
		log.Info(out)
	}

	return out, err
}
