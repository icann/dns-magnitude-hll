// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
	"dnsmag/internal"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var aggregateCmd = &cobra.Command{
	Use:   "aggregate <dnsmag-file1> <dnsmag-file2> [dnsmag-file3...]",
	Short: "Aggregate multiple DNSMAG files into combined statistics",
	Long:  `Aggregate domain statistics from multiple DNSMAG files into a single combined dataset.`,
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
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
			fmt.Fprintln(os.Stderr, "Can't be both --quiet and --verbose at the same time")
			os.Exit(1)
		}

		// Load all provided CBOR files
		for _, filename := range args {
			if verbose {
				fmt.Printf("Loading dataset from %s\n", filename)
			}
			stats, err := internal.LoadDNSMagFile(filename)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to load DNSMAG file %s: %v\n", filename, err)
				os.Exit(1)
			}
			datasets = append(datasets, stats)
		}

		if len(datasets) > 0 && verbose {
			fmt.Println()
		}

		// Aggregate the datasets
		aggregated, err := internal.AggregateDatasets(datasets)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to aggregate datasets: %v\n", err)
			os.Exit(1)
		}

		// Truncate the stats to the top N domains
		aggregated.Truncate(top)

		// Save the aggregated dataset to output file if specified
		if output != "" {
			outFilename, err := internal.WriteDNSMagFile(aggregated, output)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write aggregated dataset to %s: %v\n", output, err)
				os.Exit(1)
			}
			if verbose {
				fmt.Printf("Aggregated dataset saved to %s\n", outFilename)
			}
		}

		// Print statistics
		if !quiet {
			if len(args) == 1 {
				fmt.Printf("Statistics for %s:\n", args[0])
			} else {
				fmt.Printf("Aggregated statistics for %d files:\n", len(args))
			}
			fmt.Println()
		}

		// Finish timing and print statistics
		timing.Finish()

		if !quiet {
			// Format and print the aggregated domain statistics
			if err := internal.OutputDatasetStats(os.Stdout, aggregated, verbose); err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				os.Exit(1)
			}

			fmt.Println()

			if err := internal.OutputTimingStats(os.Stdout, timing); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to format timing statistics: %v\n", err)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(aggregateCmd)
	aggregateCmd.Flags().StringP("output", "o", "", "Output file to save the aggregated dataset (optional)")
	aggregateCmd.Flags().IntP("top", "n", internal.DefaultDomainCount, "Minimum number of domains required in each dataset")
	aggregateCmd.Flags().BoolP("verbose", "v", false, "Verbose output")
	aggregateCmd.Flags().BoolP("quiet", "q", false, "Quiet mode")
}
