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
			stdout := cmd.OutOrStdout()
			stderr := cmd.ErrOrStderr()

			inputFile := args[0]

			var (
				verbose bool
				top     int
			)

			parseFlags(cmd, map[string]any{
				"verbose": &verbose,
				"top":     &top,
			})

			// Load the CBOR file containing domain statistics (files with .dnsmag extension)
			stats, err := internal.LoadDNSMagFile(inputFile)
			if err != nil {
				fmt.Fprintf(stderr, "Failed to load DNSMAG: %v\n", err)
				cmd.SilenceUsage = true
				return fmt.Errorf("failed to load DNSMAG file %s: %w", inputFile, err)
			}

			// Truncate the stats to the top N domains
			stats.Truncate(top)

			// Format and print the domain statistics
			if err := internal.OutputDatasetStats(stdout, stats, verbose); err != nil {
				fmt.Fprintf(stderr, "%v\n", err)
				cmd.SilenceUsage = true
				return fmt.Errorf("failed to output dataset stats: %w", err)
			}

			return nil
		},
	}

	viewCmd.Flags().BoolP("verbose", "v", false, "Verbose output")
	viewCmd.Flags().IntP("top", "n", internal.DefaultDomainCount, "Number of top domains to display")

	return viewCmd
}

var viewCmd = newViewCmd()

func init() {
	rootCmd.AddCommand(viewCmd)
}
