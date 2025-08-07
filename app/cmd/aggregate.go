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
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			stdout := cmd.OutOrStdout()
			stderr := cmd.ErrOrStderr()

			timing := internal.NewTimingStats()

			var datasets []internal.MagnitudeDataset

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
				fmt.Fprintln(stderr, "Can't be both --quiet and --verbose at the same time")
				cmd.SilenceUsage = true
				return fmt.Errorf("conflicting flags: cannot use both --quiet and --verbose")
			}

			// Load all provided CBOR files
			for _, filename := range args {
				if verbose {
					fmt.Fprintf(stdout, "Loading dataset from %s\n", filename)
				}
				stats, err := internal.LoadDNSMagFile(filename)
				if err != nil {
					fmt.Fprintf(stderr, "Failed to load DNSMAG file %s: %v\n", filename, err)
					cmd.SilenceUsage = true
					return fmt.Errorf("failed to load DNSMAG file %s: %w", filename, err)
				}
				datasets = append(datasets, stats)
			}

			if len(datasets) > 0 && verbose {
				fmt.Fprintln(stdout)
			}

			// Aggregate the datasets
			aggregated, err := internal.AggregateDatasets(datasets)
			if err != nil {
				fmt.Fprintf(stderr, "Failed to aggregate datasets: %v\n", err)
				cmd.SilenceUsage = true
				return fmt.Errorf("failed to aggregate datasets: %w", err)
			}

			// Truncate the stats to the top N domains
			aggregated.Truncate(top)

			// Save the aggregated dataset to output file if specified
			if output != "" {
				outFilename, err := internal.WriteDNSMagFile(aggregated, output)
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
				if len(args) == 1 {
					fmt.Fprintf(stdout, "Statistics for %s:\n", args[0])
				} else {
					fmt.Fprintf(stdout, "Aggregated statistics for %d files:\n", len(args))
				}
				fmt.Fprintln(stdout)
			}

			// Finish timing and print statistics
			timing.Finish()

			if !quiet {
				// Format and print the aggregated domain statistics
				if err := internal.OutputDatasetStats(stdout, aggregated, verbose); err != nil {
					fmt.Fprintf(stderr, "%v\n", err)
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
