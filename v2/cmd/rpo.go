package cmd

import (
	"github.com/spf13/cobra"
)

var (
	rpoConfig string
	rpoCmd    = &cobra.Command{
		Use:   "rpo",
		Short: "Launch RPO computation",
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}
)

func init() {
	rootCmd.AddCommand(rpoCmd)
	rpoCmd.Flags().StringVarP(&rpoConfig, "config", "c", "", "path of example file")
}
