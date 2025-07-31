// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
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
		timing := internal.NewTimingStats()

		var (
			topCount int
			output   string
			filetype string
			dateStr  string
			verbose  bool
			quiet    bool
			chunk    int
		)

		parseFlags(cmd, map[string]any{
			"top":      &topCount,
			"output":   &output,
			"filetype": &filetype,
			"date":     &dateStr,
			"verbose":  &verbose,
			"quiet":    &quiet,
			"chunk":    &chunk,
		})

		// Validate filetype
		if filetype != "pcap" && filetype != "csv" {
			fmt.Fprintf(os.Stderr, "Invalid filetype '%s', must be 'pcap' or 'csv'\n", filetype)
			os.Exit(1)
		}

		// Parse date if provided
		var date *time.Time
		if dateStr != "" {
			parsedDate, err := time.Parse(time.DateOnly, dateStr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid date format '%s', expected YYYY-MM-DD: %v\n", dateStr, err)
				os.Exit(1)
			}
			date = &parsedDate
		}

		// Quiet and verbose flags are mutually exclusive
		if quiet && verbose {
			fmt.Fprintln(os.Stderr, "Can't be both --quiet and --verbose at the same time")
			os.Exit(1)
		}

		// Collect all datasets from input files
		collector := internal.NewCollector(topCount, chunk*1000*1000, verbose, date, timing)
		err := collector.ProcessFiles(args, filetype)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}

		if verbose {
			fmt.Println()
		}

		// Write stats to DNSMAG file only if output is specified
		// When no output file is specified, only show stats on stdout
		if output != "" {
			_, err := internal.WriteDNSMagFile(collector.Result, output)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write DNSMAG to %s: %v\n", output, err)
			} else {
				if !quiet {
					fmt.Printf("Saved aggregated statistics to %s\n\n", output)
				}
			}
		}

		timing.Finish()

		// Print statistics and timing
		if err := internal.OutputCollectorStats(os.Stdout, collector, quiet, verbose, args); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(collectCmd)
	collectCmd.Flags().IntP("top", "n", internal.DefaultDomainCount, "Number of domains to collect")
	collectCmd.Flags().StringP("output", "o", "", "Output file to save the aggregated dataset (optional, only shows stats on stdout if not specified)")
	collectCmd.Flags().String("filetype", "pcap", "Input file type: 'pcap' or 'csv'")
	collectCmd.Flags().String("date", "", "Date for CSV data in YYYY-MM-DD format (optional, defaults to data from input files or the current date)")
	collectCmd.Flags().BoolP("verbose", "v", false, "Verbose output")
	collectCmd.Flags().BoolP("quiet", "q", false, "Quiet mode")
	collectCmd.Flags().IntP("chunk", "c", internal.DefaultCollectDomainsChunk, "Number of queries to process in one go (in millions, 0 = unlimited)")
}
