package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// version set at build time with -ldflags="-X dnsmag/app/cmd.Version=v0.0.1"
var Version = "undefined"

func newVersionCmd() *cobra.Command {
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Show version",
		Long:  `Show software version.`,
		Run: func(cmd *cobra.Command, _ []string) {
			stdout := cmd.OutOrStdout()

			fmt.Fprintf(stdout, "dnsmag %s\n", Version)
		},
	}

	return versionCmd
}

var versionCmd = newVersionCmd()

func init() {
	rootCmd.AddCommand(versionCmd)
}
