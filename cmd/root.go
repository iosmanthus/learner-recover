package cmd

import (
	"errors"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "learner-recover",
	Short: "learner-recover is a tool to recover TiKV cluster from learner stores",
	RunE: func(cmd *cobra.Command, args []string) error {
		return errors.New("missing subcommand")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
