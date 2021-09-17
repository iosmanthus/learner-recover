package cmd

import (
	"github.com/iosmanthus/learner-recover/version"
	"github.com/spf13/cobra"
)

var (
	versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Print the version number of learner-recover",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Printf("Version: %s\n", version.Version)
			cmd.Printf("GitCommit: %s\n", version.GitCommit)
		},
	}
)

func init() {
	rootCmd.AddCommand(versionCmd)
}
