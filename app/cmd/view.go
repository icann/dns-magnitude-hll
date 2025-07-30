// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
	"dnsmag/internal"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var viewCmd = &cobra.Command{
	Use:   "view <input-file>",
	Short: "View and display statistics from a DNSMAG file",
	Long:  `View domain statistics from a previously saved DNSMAG file and display them.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		inputFile := args[0]

		var (
			verbose bool
			top     int
		)

		parseFlags(cmd, map[string]interface{}{
			"verbose": &verbose,
			"top":     &top,
		})

		// Load the CBOR file containing domain statistics (files with .dnsmag extension)
		stats, err := internal.LoadDNSMagFile(inputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load DNSMAG: %v\n", err)
			os.Exit(1)
		}

		// Truncate the stats to the top N domains
		stats.Truncate(top)

		// Format and print the domain statistics
		if err := internal.OutputDomainStats(stats, false, verbose); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(viewCmd)
	viewCmd.Flags().BoolP("verbose", "v", false, "Verbose output")
	viewCmd.Flags().IntP("top", "n", internal.DefaultDomainCount, "Number of top domains to display")
}
