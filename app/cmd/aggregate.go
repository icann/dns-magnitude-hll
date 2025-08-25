// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
	"dnsmag/internal"
	"fmt"

	"github.com/spf13/cobra"
)

func newAggregateCmd() *cobra.Command {
	aggregateCmd := &cobra.Command{
		Use:   "aggregate <dnsmag-file1> <dnsmag-file2> [dnsmag-file3...]",
		Short: "Aggregate multiple DNSMAG files into combined statistics",
		Long:  `Aggregate domain statistics from multiple DNSMAG files into a single combined dataset.`,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("requires at least 1 argument")
			}
			if len(args) == 1 && args[0] != "-" {
				return fmt.Errorf("requires at least 2 files, or use '-' to read from stdin")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			stderr := cmd.ErrOrStderr()
			stdout := cmd.OutOrStdout()

			timing := internal.NewTimingStats()

			var (
				top     int
				verbose bool
				quiet   bool
				output  string
			)

			parseFlags(cmd, map[string]any{
				"top":     &top,
				"verbose": &verbose,
				"quiet":   &quiet,
				"output":  &output,
			})

			// Quiet and verbose flags are mutually exclusive
			if quiet && verbose {
				cmd.SilenceUsage = true
				return fmt.Errorf("conflicting flags: cannot use both --quiet and --verbose")
			}

			seq := internal.NewDatasetSequence(top, nil)

			// Load all provided DNSMAG files
			err := loadDatasets(cmd, seq, args, verbose)
			if err != nil {
				cmd.SilenceUsage = true
				return err
			}

			if seq.Count > 0 && verbose {
				fmt.Fprintln(stderr)
			}

			// Save the aggregated dataset to output file if specified
			if output != "" {
				outFilename, err := internal.WriteDNSMagFile(seq.Result, output, stdout)
				if err != nil {
					cmd.SilenceUsage = true
					return fmt.Errorf("failed to write aggregated dataset to %s: %w", output, err)
				}
				if verbose {
					fmt.Fprintf(stderr, "Aggregated dataset saved to %s\n", outFilename)
				}
			}

			// Print statistics
			if !quiet {
				if seq.Count == 0 {
					fmt.Fprintf(stderr, "Statistics for %s:\n", args[0])
				} else {
					fmt.Fprintf(stderr, "Aggregated statistics for %d datasets:\n", seq.Count)
				}
				fmt.Fprintln(stderr)
			}

			// Finish timing and print statistics
			timing.Finish()

			if !quiet {
				// Format and print the aggregated domain statistics
				if err := internal.OutputDatasetStats(stderr, seq.Result, verbose); err != nil {
					cmd.SilenceUsage = true
					return fmt.Errorf("failed to output dataset stats: %w", err)
				}

				fmt.Fprintln(stderr)

				if err := internal.OutputTimingStats(stderr, timing); err != nil {
					cmd.SilenceUsage = true
					return fmt.Errorf("failed to format timing statistics: %w", err)
				}
			}

			return nil
		},
	}

	aggregateCmd.Flags().StringP("output", "o", "", "Output file to save the aggregated dataset (optional)")
	aggregateCmd.Flags().IntP("top", "n", internal.DefaultDomainCount, "Minimum number of domains required in each dataset")
	aggregateCmd.Flags().BoolP("verbose", "v", false, "Verbose output")
	aggregateCmd.Flags().BoolP("quiet", "q", false, "Quiet mode")

	return aggregateCmd
}

var aggregateCmd = newAggregateCmd()

func init() {
	rootCmd.AddCommand(aggregateCmd)
}
