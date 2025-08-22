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
			stdin := cmd.InOrStdin()
			stdout := cmd.OutOrStdout()
			stderr := cmd.ErrOrStderr()

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
			err := loadDatasets(seq, args, stdin, stdout, stderr, verbose)
			if err != nil {
				fmt.Fprintf(stderr, "Failed to aggregate datasets: %v\n", err)
				cmd.SilenceUsage = true
				return fmt.Errorf("failed to aggregate datasets: %w", err)
			}

			if seq.Count > 0 && verbose {
				fmt.Fprintln(stdout)
			}

			// Save the aggregated dataset to output file if specified
			if output != "" {
				outFilename, err := internal.WriteDNSMagFile(seq.Result, output)
				if err != nil {
					fmt.Fprintf(stderr, "Failed to write aggregated dataset to %s: %v\n", output, err)
					cmd.SilenceUsage = true
					return fmt.Errorf("failed to write aggregated dataset to %s: %w", output, err)
				}
				if verbose {
					fmt.Fprintf(stdout, "Aggregated dataset saved to %s\n", outFilename)
				}
			}

			// Print statistics
			if !quiet {
				if seq.Count == 0 {
					fmt.Fprintf(stdout, "Statistics for %s:\n", args[0])
				} else {
					fmt.Fprintf(stdout, "Aggregated statistics for %d datasets:\n", seq.Count)
				}
				fmt.Fprintln(stdout)
			}

			// Finish timing and print statistics
			timing.Finish()

			if !quiet {
				// Format and print the aggregated domain statistics
					fmt.Fprintf(stderr, "%v\n", err)
				if err := internal.OutputDatasetStats(stdout, seq.Result, verbose); err != nil {
					cmd.SilenceUsage = true
					return fmt.Errorf("failed to output dataset stats: %w", err)
				}

				fmt.Fprintln(stdout)

				if err := internal.OutputTimingStats(stdout, timing); err != nil {
					fmt.Fprintf(stderr, "Failed to format timing statistics: %v\n", err)
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
