// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
	"dnsmag/internal"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var reportCmd = &cobra.Command{
	Use:   "report <dnsmag-file>",
	Short: "Generate a JSON report from a DNSMAG file",
	Long:  `Generate a JSON report from a DNSMAG file according to the report schema.`,
	Args:  cobra.ExactArgs(1),
	PreRunE: func(cmd *cobra.Command, args []string) error {
		sourceType, _ := cmd.Flags().GetString("source-type")
		if sourceType != "authoritative" && sourceType != "recursive" {
			return fmt.Errorf("invalid source-type '%s'. Must be 'authoritative' or 'recursive'", sourceType)
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		filename := args[0]

		// Load the DNSMAG file
		stats, err := internal.LoadDnsMagFile(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load DNSMAG file %s: %v\n", filename, err)
			os.Exit(1)
		}

		source, _ := cmd.Flags().GetString("source")
		sourceType, _ := cmd.Flags().GetString("source-type")
		output, _ := cmd.Flags().GetString("output")

		// Generate the report in a data structure confirming to the schema (report-schema.yaml)
		report := internal.GenerateReport(stats, source, sourceType)

		jsonData, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to generate JSON report: %v\n", err)
			os.Exit(1)
		}

		// Write the report to the specified output file or stdout
		if output != "" {
			err = os.WriteFile(output, jsonData, 0644)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to write report to %s: %v\n", output, err)
				os.Exit(1)
			}
			fmt.Printf("Report written to %s\n", output)
		} else {
			fmt.Println(string(jsonData))
		}
	},
}

func init() {
	rootCmd.AddCommand(reportCmd)
	reportCmd.Flags().StringP("source", "s", "", "The name of the provider of the magnitude score (required)")
	reportCmd.Flags().String("source-type", "authoritative", "Source type of the magnitude score (authoritative or recursive)")
	reportCmd.Flags().StringP("output", "o", "", "Output file (optional, defaults to stdout)")
	reportCmd.MarkFlagRequired("source")
}
