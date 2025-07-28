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
			top      int
			output   string
			filetype string
			dateStr  string
			verbose  bool
			quiet    bool
		)

		parseFlags(cmd, map[string]interface{}{
			"top":      &top,
			"output":   &output,
			"filetype": &filetype,
			"date":     &dateStr,
			"verbose":  &verbose,
			"quiet":    &quiet,
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
		var datasets []internal.MagnitudeDataset

		timing.StartParsing()

		// Process each input file
		for _, inputFile := range args {
			var stats internal.MagnitudeDataset
			var err error

			if verbose {
				fmt.Printf("Loading %s file: %s\n", filetype, inputFile)
			}

			if filetype == "csv" {
				stats, err = internal.LoadCSVFile(inputFile, date, verbose)
			} else {
				stats, err = internal.LoadPcap(inputFile, date, verbose)
			}

			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to load %s file %s: %v\n", filetype, inputFile, err)
				os.Exit(1)
			}

			datasets = append(datasets, stats)
		}

		timing.StopParsing()

		if len(datasets) > 0 && verbose {
			fmt.Println()
		}

		// Aggregate all datasets into one
		var res internal.MagnitudeDataset
		var err error

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

		// Write stats to DNSMAG file only if output is specified
		// When no output file is specified, only show stats on stdout
		if output != "" {
			_, err := internal.WriteDNSMagFile(res, output)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write DNSMAG to %s: %v\n", output, err)
			} else {
				if !quiet {
					fmt.Printf("Saved aggregated statistics to %s\n", output)
				}
			}
		}

		// Print statistics
		if !quiet {
			if len(args) == 1 {
				fmt.Printf("Statistics for %s:\n", args[0])
			} else {
				fmt.Printf("Aggregated statistics for %d files:\n", len(args))
			}
		}
		if err := internal.OutputDomainStats(res, quiet, verbose); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		if !quiet {
			fmt.Println()
		}

		// Print timing statistics at the end
		timing.Finish()
		if err := internal.OutputTimingStats(timing, quiet); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to format timing statistics: %v\n", err)
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
}
