package cmd

import (
	"context"
	"github.com/iosmanthus/learner-recover/components/fetcher"
	"github.com/spf13/cobra"
)

var (
	fetchConfig string
	fetchCmd    = &cobra.Command{
		Use:   "fetch",
		Short: "Collect recover info",
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := fetcher.NewConfig(fetchConfig)
			if err != nil {
				return err
			}
			updater, err := fetcher.NewRecoverInfoUpdater(c)
			if err != nil {
				return err
			}

			if err = updater.Init(); err != nil {
				return err
			}

			ctx := context.Background()
			if err = updater.Update(ctx); err != nil {
				return err
			}
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(fetchCmd)
	fetchCmd.Flags().StringVarP(&fetchConfig, "example", "c", "", "path of example file")
}
