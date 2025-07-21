// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
	"bytes"
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
		var datasets []internal.MagnitudeDataset

		// Load all provided CBOR files
		for _, filename := range args {
			stats, err := internal.LoadDNSMagFile(filename)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to load DNSMAG file %s: %v\n", filename, err)
				os.Exit(1)
			}
			datasets = append(datasets, stats)
			fmt.Printf("Loaded dataset from %s\n", filename)
		}

		top, err := cmd.Flags().GetInt("top")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse 'top' flag: %v\n", err)
			os.Exit(1)
		}

		// Aggregate the datasets
		aggregated, err := internal.AggregateDatasets(datasets)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to aggregate datasets: %v\n", err)
			os.Exit(1)
		}

		// Truncate the stats to the top N domains
		err = aggregated.Truncate(top)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}

		// Format and print the aggregated domain statistics
		var buf bytes.Buffer
		err = internal.FormatDomainStats(&buf, aggregated, 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to format domain statistics: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(buf.String())

		// Save the aggregated dataset to output file if specified
		output, err := cmd.Flags().GetString("output")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse 'output' flag: %v\n", err)
			os.Exit(1)
		}
		if output != "" {
			outFilename, err := internal.WriteDNSMagFile(aggregated, output)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write aggregated dataset to %s: %v\n", output, err)
				os.Exit(1)
			}
			fmt.Printf("Aggregated dataset saved to %s\n", outFilename)
		}
	},
}

func init() {
	rootCmd.AddCommand(aggregateCmd)
	aggregateCmd.Flags().StringP("output", "o", "", "Output file to save the aggregated dataset (optional)")
	aggregateCmd.Flags().IntP("top", "n", internal.DefaultDomainCount, "Minimum number of domains required in each dataset")
}
