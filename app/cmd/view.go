// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
	"bytes"
	"dnsmag/internal"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var viewCmd = &cobra.Command{
	Use:   "view <input-file>",
	Short: "View and display statistics from a DNSMAG file",
	Long: `View domain statistics from a previously saved DNSMAG file and display them.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		inputFile := args[0]

		internal.InitStats()

		// Load the CBOR file containing domain statistics (files with .dnsmag extension)
		stats, err := internal.LoadDnsMagFile(inputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load DNSMAG: %v\n", err)
			os.Exit(1)
		}

		// Format and print the domain statistics
		var buf bytes.Buffer
		internal.FormatDomainStats(&buf, stats, 0)
		fmt.Println(buf.String())
	},
}

func init() {
	rootCmd.AddCommand(viewCmd)
}
