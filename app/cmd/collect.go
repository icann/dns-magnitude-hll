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
			stdin := cmd.InOrStdin()
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
			if filetype != "pcap" && filetype != "csv" && filetype != "tsv" {
				cmd.SilenceUsage = true
				return fmt.Errorf("invalid filetype '%s', must be 'pcap', 'csv' or 'tsv'", filetype)
			}

			// Parse date if provided
			var date *time.Time
			if dateStr != "" {
				parsedDate, err := time.Parse(time.DateOnly, dateStr)
				if err != nil {
					cmd.SilenceUsage = true
					return fmt.Errorf("invalid date format '%s', expected YYYY-MM-DD: %w", dateStr, err)
				}
				date = &parsedDate
			}

			// Quiet and verbose flags are mutually exclusive
			if quiet && verbose {
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
			err := collector.ProcessFiles(args, filetype, stdin, stderr)
			if err != nil {
				cmd.SilenceUsage = true
				return fmt.Errorf("failed to process files: %w", err)
			}

			if verbose {
				fmt.Fprintln(stderr)
			}

			// Write stats to DNSMAG file only if output is specified
			// When no output file is specified, only show stats on stderr
			if output != "" {
				filename, err := internal.WriteDNSMagFile(collector.Result, output, stdout)
				if err != nil {
					cmd.SilenceUsage = true
					return fmt.Errorf("failed to write DNSMAG to %s: %w", filename, err)
				}
				if !quiet {
					fmt.Fprintf(stderr, "Saved aggregated statistics to %s\n\n", filename)
				}
			}

			timing.Finish()

			if !quiet {
				// Print statistics and timing
				if err := internal.OutputCollectorStats(stderr, collector, verbose); err != nil {
					cmd.SilenceUsage = true
					return fmt.Errorf("failed to output collector stats: %w", err)
				}
			}

			return nil
		},
	}
	collectCmd.Flags().IntP("top", "n", internal.DefaultDomainCount, "Number of domains to collect")
	collectCmd.Flags().StringP("output", "o", "", "Output file to save the aggregated dataset (optional, only shows stats on stderr if not specified)")
	collectCmd.Flags().String("filetype", "pcap", "Input file type: 'pcap', 'csv' or 'tsv'")
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
