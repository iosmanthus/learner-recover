package cmd

import (
	"context"
	"github.com/iosmanthus/learner-recover/components/rpo"

	"github.com/gofrs/flock"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	rpoConfig string
	rpoCmd    = &cobra.Command{
		Use:   "rpo",
		Short: "Launch RPO computation",
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := rpo.NewConfig(rpoConfig)
			if err != nil {
				return err
			}

			lock := c.HistoryPath + ".lock"
			fileLock := flock.New(lock)
			log.Warnf("Acquiring lock for %s", lock)
			fileLock.Lock()
			defer fileLock.Unlock()
			gen := rpo.NewGenerator(c)
			return gen.Gen(context.Background())
		},
	}
)

func init() {
	rootCmd.AddCommand(rpoCmd)
	rpoCmd.Flags().StringVarP(&rpoConfig, "config", "c", "", "path of example file")
}
