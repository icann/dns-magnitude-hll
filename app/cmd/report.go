// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
	"dnsmag/internal"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func newReportCmd() *cobra.Command {
	reportCmd := &cobra.Command{
		Use:   "report <dnsmag-file>",
		Short: "Generate a JSON report from a DNSMAG file",
		Long:  `Generate a JSON report from a DNSMAG file according to the report schema.`,
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, _ []string) error {
			sourceType, err := cmd.Flags().GetString("source-type")
			if err != nil {
				return fmt.Errorf("failed to get source-type flag: %v", err)
			}
			if sourceType != "authoritative" && sourceType != "recursive" {
				return fmt.Errorf("invalid source-type '%s'. Must be 'authoritative' or 'recursive'", sourceType)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			stdout := cmd.OutOrStdout()
			stderr := cmd.ErrOrStderr()

			filename := args[0]

			var (
				source     string
				sourceType string
				output     string
				verbose    bool
			)

			parseFlags(cmd, map[string]any{
				"source":      &source,
				"source-type": &sourceType,
				"output":      &output,
				"verbose":     &verbose,
			})

			seq := internal.NewDatasetSequence(0, nil)

			if err := loadDatasets(cmd, seq, []string{filename}, verbose); err != nil {
				cmd.SilenceUsage = true
				return err
			}

			// Generate the report in a data structure conforming to the schema (report-schema.yaml)
			report := internal.GenerateReport(seq.Result, source, sourceType)

			jsonData, err := json.MarshalIndent(report, "", "  ")
			if err != nil {
				cmd.SilenceUsage = true
				return fmt.Errorf("failed to generate JSON report: %w", err)
			}

			// Write the report to the specified output file or stdout
			if output != "" && output != "-" {
				err = os.WriteFile(output, jsonData, 0o644) // #nosec G306
				if err != nil {
					cmd.SilenceUsage = true
					return fmt.Errorf("failed to write report to %s: %w", output, err)
				}
				if verbose {
					fmt.Fprintf(stderr, "Report written to %s\n", output)
				}
			} else {
				fmt.Fprintln(stdout, string(jsonData))
				if verbose {
					fmt.Fprintf(stderr, "Report written to STDOUT\n")
				}
			}

			return nil
		},
	}

	reportCmd.Flags().StringP("source", "s", "", "The name of the provider of the magnitude score (required)")
	reportCmd.Flags().String("source-type", "authoritative", "Source type of the magnitude score (authoritative or recursive)")
	reportCmd.Flags().StringP("output", "o", "", "Output file (optional, defaults to stdout)")
	reportCmd.Flags().BoolP("verbose", "v", false, "Verbose output")
	if err := reportCmd.MarkFlagRequired("source"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to mark 'source' flag as required: %v\n", err)
		os.Exit(1)
	}

	return reportCmd
}

var reportCmd = newReportCmd()

func init() {
	rootCmd.AddCommand(reportCmd)
}
