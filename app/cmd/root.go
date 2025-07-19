// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "dnsmag",
	Short: "DNS Magnitude analyzer for processing DNS traffic statistics",
	Long: `DNS Magnitude is a tool for analyzing DNS traffic and computing domain statistics.
It can parse PCAP files, view/save DNSMAG data (CBOR), aggregate multiple datasets and make a JSON report.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func init() {
	// Commands will be added via their individual init() functions
}
