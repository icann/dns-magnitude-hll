// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
	"dnsmag/internal"
	"fmt"

	"github.com/spf13/cobra"
)

func newViewCmd() *cobra.Command {
	viewCmd := &cobra.Command{
		Use:   "view <input-file>",
		Short: "View and display statistics from a DNSMAG file",
		Long:  `View domain statistics from a previously saved DNSMAG file and display them.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			stderr := cmd.ErrOrStderr()

			inputFile := args[0]

			var (
				verbose bool
				json    bool
				top     int
			)

			parseFlags(cmd, map[string]any{
				"verbose": &verbose,
				"json":    &json,
				"top":     &top,
			})

			if verbose && json {
				return fmt.Errorf("--verbose and --json are mutually exclusive")
			}

			seq := internal.NewDatasetSequence(top, nil)

			if err := loadDatasets(cmd, seq, []string{inputFile}, verbose); err != nil {
				cmd.SilenceUsage = true
				return err
			}

			// Format and print the domain statistics
			if json {
				stdout := cmd.OutOrStdout()
				if err := internal.OutputDatasetStatsJSON(stdout, seq.Result); err != nil {
					cmd.SilenceUsage = true
					return err
				}
			} else {
				if err := internal.OutputDatasetStats(stderr, seq.Result, verbose); err != nil {
					cmd.SilenceUsage = true
					return err
				}
			}

			return nil
		},
	}

	viewCmd.Flags().BoolP("verbose", "v", false, "Verbose output")
	viewCmd.Flags().BoolP("json", "j", false, "JSON output")
	viewCmd.Flags().IntP("top", "n", internal.DefaultDomainCount, "Number of top domains to display")

	return viewCmd
}

var viewCmd = newViewCmd()

func init() {
	rootCmd.AddCommand(viewCmd)
}
