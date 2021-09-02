package cmd

import (
	"context"

	"github.com/iosmanthus/learner-recover/components/recover"
	log "github.com/sirupsen/logrus"
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
				return err
			}

			log.Info("Success!")
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(recoverCmd)
	recoverCmd.Flags().StringVarP(&recoverConfig, "config", "c", "", "path of example file")
}
