package cmd

import (
	"dnsmag/internal"
	"fmt"

	"github.com/spf13/cobra"
)


func newVersionCmd() *cobra.Command {
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Show version",
		Long:  `Show software version.`,
		Run: func(cmd *cobra.Command, _ []string) {
			stdout := cmd.OutOrStdout()

			fmt.Fprintf(stdout, "dnsmag %s\n", internal.Version)
		},
	}

	return versionCmd
}

var versionCmd = newVersionCmd()

func init() {
	rootCmd.AddCommand(versionCmd)
}
