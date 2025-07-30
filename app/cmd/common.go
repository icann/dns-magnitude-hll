package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// parseFlags parses command flags in a table-driven manner
func parseFlags(cmd *cobra.Command, flags map[string]any) {
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
			fmt.Fprintf(os.Stderr, "Unsupported flag type for %s\n", name)
			os.Exit(1)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get %s flag: %v\n", name, err)
			os.Exit(1)
		}
	}
}
