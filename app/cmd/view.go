// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
	"bytes"
	"dnsmag/internal"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func newViewCmd() *cobra.Command {
	viewCmd := &cobra.Command{
		Use:   "view <input-file>",
		Short: "View and display statistics from a DNSMAG file",
		Long:  `View domain statistics from a previously saved DNSMAG file and display them.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			stderr := cmd.ErrOrStderr()
			stdout := cmd.OutOrStdout()

			inputFile := args[0]

			var (
				verbose bool
				json    bool
				top     int
				output  string
			)

			parseFlags(cmd, map[string]any{
				"verbose": &verbose,
				"json":    &json,
				"top":     &top,
				"output":  &output,
			})

			if verbose && json {
				return fmt.Errorf("--verbose and --json are mutually exclusive")
			}

			cmd.SilenceUsage = true

			seq := internal.NewDatasetSequence(top, nil)

			if err := loadDatasets(cmd, seq, []string{inputFile}, verbose); err != nil {
				return err
			}

			// Format and print the domain statistics

			var buf bytes.Buffer
			if json {
				if err := internal.OutputDatasetStatsJSON(&buf, seq.Result); err != nil {
					return err
				}
			} else {
				if err := internal.OutputDatasetStats(&buf, seq.Result, verbose); err != nil {
					return err
				}
			}

			// Write buffer to stderr (default), stdout or file
			if output != "" && output != "-" {
				// Write to file
				// #nosec G306
				if err := os.WriteFile(output, buf.Bytes(), 0o644); err != nil {
					return fmt.Errorf("failed to write to %s: %w", output, err)
				}
			} else if output == "-" {
				// Write to stdout
				if _, err := stdout.Write(buf.Bytes()); err != nil {
					return fmt.Errorf("failed to write to stdout: %w", err)
				}
			} else {
				// Default: stderr
				if _, err := stderr.Write(buf.Bytes()); err != nil {
					return fmt.Errorf("failed to write to stderr: %w", err)
				}
			}

			return nil
		},
	}

	viewCmd.Flags().BoolP("verbose", "v", false, "Verbose output")
	viewCmd.Flags().BoolP("json", "j", false, "JSON output")
	viewCmd.Flags().IntP("top", "n", internal.DefaultDomainCount, "Number of top domains to display")
	viewCmd.Flags().StringP("output", "o", "", "Output file (optional, use '-' for stdout, defaults to stderr)")

	return viewCmd
}

var viewCmd = newViewCmd()

func init() {
	rootCmd.AddCommand(viewCmd)
}
