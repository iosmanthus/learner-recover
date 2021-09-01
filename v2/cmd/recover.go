package cmd

import (
	"context"
	log "github.com/sirupsen/logrus"

	"github.com/iosmanthus/learner-recover/components/recover"

	"github.com/spf13/cobra"
)

var (
	recoverConfig string
	recoverCmd    = &cobra.Command{
		Use:   "recover",
		Short: "Recover TiKV cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			config, err := recover.NewConfig(recoverConfig)
			if err != nil {
				return err
			}
			rescuer := recover.NewClusterRescuer(config)
			err = rescuer.Execute(context.Background())
			if err != nil {
				log.Error(err)
			}
			return err
		},
	}
)

func init() {
	rootCmd.AddCommand(recoverCmd)
	recoverCmd.Flags().StringVarP(&recoverConfig, "config", "c", "", "path of example file")
}
