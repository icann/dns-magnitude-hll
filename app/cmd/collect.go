// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
	"dnsmag/internal"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

func newCollectCmd() *cobra.Command {
	collectCmd := &cobra.Command{
		Use:   "collect <input-file> [input-file2] [input-file3...]",
		Short: "Parse PCAP files and generate domain statistics",
		Long: `Parse one or more PCAP files containing DNS traffic and generate domain statistics.
Save them to a DNSMAG file (CBOR format).`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			stdout := cmd.OutOrStdout()
			stderr := cmd.ErrOrStderr()

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
				fmt.Fprintf(stderr, "Invalid filetype '%s', must be 'pcap' or 'csv'\n", filetype)
				cmd.SilenceUsage = true
				return fmt.Errorf("invalid filetype '%s', must be 'pcap' or 'csv'", filetype)
			}

			// Parse date if provided
			var date *time.Time
			if dateStr != "" {
				parsedDate, err := time.Parse(time.DateOnly, dateStr)
				if err != nil {
					fmt.Fprintf(stderr, "Invalid date format '%s', expected YYYY-MM-DD: %v\n", dateStr, err)
					cmd.SilenceUsage = true
					return fmt.Errorf("invalid date format '%s', expected YYYY-MM-DD: %w", dateStr, err)
				}
				date = &parsedDate
			}

			// Quiet and verbose flags are mutually exclusive
			if quiet && verbose {
				fmt.Fprintln(stderr, "Can't be both --quiet and --verbose at the same time")
				cmd.SilenceUsage = true
				return fmt.Errorf("conflicting flags: cannot use both --quiet and --verbose")
			}

			// Collect all datasets from input files
			var chunkSize uint
			if chunk < 0 {
				chunkSize = 0
			} else {
				chunkSize = uint(chunk) * 1000 * 1000
			}
			collector := internal.NewCollector(topCount, chunkSize, verbose, date, timing)
			err := collector.ProcessFiles(args, filetype)
			if err != nil {
				fmt.Fprintf(stderr, "%v\n", err)
				cmd.SilenceUsage = true
				return fmt.Errorf("failed to process files: %w", err)
			}

			if verbose {
				fmt.Fprintln(stdout)
			}

			// Write stats to DNSMAG file only if output is specified
			// When no output file is specified, only show stats on stdout
			if output != "" {
				_, err := internal.WriteDNSMagFile(collector.Result, output)
				if err != nil {
					fmt.Fprintf(stderr, "Failed to write DNSMAG to %s: %v\n", output, err)
					cmd.SilenceUsage = true
					return fmt.Errorf("failed to write DNSMAG to %s: %w", output, err)
				}
				if !quiet {
					fmt.Fprintf(stdout, "Saved aggregated statistics to %s\n\n", output)
				}
			}

			timing.Finish()

			if !quiet {
				// Print statistics and timing
				if err := internal.OutputCollectorStats(stdout, collector, verbose); err != nil {
					fmt.Fprintf(stderr, "%v\n", err)
					cmd.SilenceUsage = true
					return fmt.Errorf("failed to output collector stats: %w", err)
				}
			}

			return nil
		},
	}
	collectCmd.Flags().IntP("top", "n", internal.DefaultDomainCount, "Number of domains to collect")
	collectCmd.Flags().StringP("output", "o", "", "Output file to save the aggregated dataset (optional, only shows stats on stdout if not specified)")
	collectCmd.Flags().String("filetype", "pcap", "Input file type: 'pcap' or 'csv'")
	collectCmd.Flags().String("date", "", "Date for CSV data in YYYY-MM-DD format (optional, defaults to data from input files or the current date)")
	collectCmd.Flags().BoolP("verbose", "v", false, "Verbose output")
	collectCmd.Flags().BoolP("quiet", "q", false, "Quiet mode")
	collectCmd.Flags().IntP("chunk", "c", internal.DefaultCollectDomainsChunk, "Number of queries to process in one go (in millions, 0 = unlimited)")

	return collectCmd
}

var collectCmd = newCollectCmd()

func init() {
	rootCmd.AddCommand(collectCmd)
}
