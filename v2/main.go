package main

import (
	"github.com/iosmanthus/learner-recover/cmd"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		PadLevelText:  true,
		FullTimestamp: true,
	})
}

func main() {
	cmd.Execute()
}
