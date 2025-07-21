// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
	"bytes"
	"dnsmag/internal"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var collectCmd = &cobra.Command{
	Use:   "collect <input-file> [input-file2] [input-file3...]",
	Short: "Parse PCAP files and generate domain statistics",
	Long: `Parse one or more PCAP files containing DNS traffic and generate domain statistics.
Save them to a DNSMAG file (CBOR format).`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		top, err := cmd.Flags().GetInt("top")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get top flag: %v\n", err)
			os.Exit(1)
		}
		output, err := cmd.Flags().GetString("output")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get output flag: %v\n", err)
			os.Exit(1)
		}

		// Collect all datasets from input files
		var datasets []internal.MagnitudeDataset
		var totalElapsed time.Duration

		// Process each input file
		for _, inputFile := range args {
			stats, elapsed, err := internal.LoadPcap(inputFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to load PCAP file %s: %v\n", inputFile, err)
				os.Exit(1)
			}
			datasets = append(datasets, stats)
			totalElapsed += elapsed
		}

		// Aggregate all datasets into one
		var res internal.MagnitudeDataset

		if len(datasets) == 1 {
			res = datasets[0]
		} else {
			res, err = internal.AggregateDatasets(datasets)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to aggregate datasets: %v\n", err)
				os.Exit(1)
			}
		}

		// Truncate the aggregated stats to the top N domains
		err = res.Truncate(top)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to truncate results: %v\n", err)
			os.Exit(1)
		}

		// Format and print the domain statistics
		var buf bytes.Buffer
		err = internal.FormatDomainStats(&buf, res, totalElapsed)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to format domain statistics: %v\n", err)
			os.Exit(1)
		}
		if len(args) == 1 {
			fmt.Printf("Statistics for %s:\n", args[0])
		} else {
			fmt.Printf("Aggregated statistics for %d files:\n", len(args))
		}
		fmt.Println(buf.String())
		fmt.Println("---")

		// Write stats to DNSMAG file only if output is specified
		if output != "" {
			_, err := internal.WriteDNSMagFile(res, output)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write DNSMAG to %s: %v\n", output, err)
			} else {
				fmt.Printf("Saved aggregated statistics to %s\n", output)
			}
		}
		// When no output file is specified, only show stats on stdout
	},
}

func init() {
	rootCmd.AddCommand(collectCmd)
	collectCmd.Flags().IntP("top", "n", internal.DefaultDomainCount, "Number of domains to collect")
	collectCmd.Flags().StringP("output", "o", "", "Output file to save the aggregated dataset (optional, only shows stats on stdout if not specified)")
}
