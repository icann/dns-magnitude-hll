package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// version set at build time with -ldflags="-X cmd.Version=v0.0.1"
var Version = "undefined"

func newVersionCmd() *cobra.Command {
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Show version",
		Long:  `SHow software version.`,
		RunE: func(cmd *cobra.Command, _args []string) error {
			stdout := cmd.OutOrStdout()

			fmt.Fprintf(stdout, "dnsmag %s\n", Version)

			return nil
		},
	}

	return versionCmd
}

var versionCmd = newVersionCmd()

func init() {
	rootCmd.AddCommand(versionCmd)
}
