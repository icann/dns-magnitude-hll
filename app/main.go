// Author: Fredrik Thulin <fredrik@ispik.se>

package main

import (
	"dnsmag/app/cmd"
	"dnsmag/internal"
	"fmt"
	"os"
)

func main() {
	// Initialize HLL statistics with error checking
	if err := internal.InitStats(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize statistics: %v\n", err)
		os.Exit(1)
	}

	cmd.Execute()
}
