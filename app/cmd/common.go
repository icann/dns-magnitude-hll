// Author: Fredrik Thulin <fredrik@ispik.se>

package cmd

import (
	"dnsmag/internal"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// parseFlags parses command flags in a table-driven manner
func parseFlags(cmd *cobra.Command, flags map[string]any) {
	stderr := cmd.ErrOrStderr()

	for name, dest := range flags {
		var err error
		switch v := dest.(type) {
		case *int:
			*v, err = cmd.Flags().GetInt(name)
		case *bool:
			*v, err = cmd.Flags().GetBool(name)
		case *string:
			*v, err = cmd.Flags().GetString(name)
		default:
			fmt.Fprintf(stderr, "Unsupported flag type for %s\n", name)
			os.Exit(1)
		}
		if err != nil {
			fmt.Fprintf(stderr, "Failed to get %s flag: %v\n", name, err)
			os.Exit(1)
		}
	}
}

// loadDatasets loads DNSMAG datasets from CBOR sequences in files or if the filename "-" is used, from STDIN.
func loadDatasets(cmd *cobra.Command, seq *internal.DatasetSequence, args []string, verbose bool) error {
	stdin := cmd.InOrStdin()
	stderr := cmd.ErrOrStderr()

	for _, filename := range args {
		var err error
		if filename == "-" {
			if verbose {
				fmt.Fprintf(stderr, "Loading datasets from STDIN\n")
			}
			err = seq.LoadDNSMagSequenceFromReader(stdin, "<stdin#%d>")
			if err != nil {
				return fmt.Errorf("failed to load datasets from STDIN: %w", err)
			}
			continue
		}

		if verbose {
			fmt.Fprintf(stderr, "Loading datasets from %s\n", filename)
		}

		err = seq.LoadDNSMagFile(filename)
		if err != nil {
			return fmt.Errorf("failed to load DNSMAG file %s: %w", filename, err)
		}
	}
	return nil
}
