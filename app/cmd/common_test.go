package cmd

import (
	"bytes"
	"dnsmag/internal"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

// executeCollectAndVerify is a helper function to execute a collect command and verify the query count
func executeCollectAndVerify(t *testing.T, args []string, expectedQueries int, description string) string {
	t.Helper()

	collectCmd := newCollectCmd()
	collectCmd.SetArgs(args)

	var collectBuf bytes.Buffer
	collectCmd.SetOut(&collectBuf)
	collectCmd.SetErr(&collectBuf)

	err := collectCmd.Execute()
	if err != nil {
		t.Fatalf("%s collect command failed: %v\nOutput: %s", description, err, collectBuf.String())
	}

	// Verify the expected query count
	output := collectBuf.String()
	queriesPattern := regexp.MustCompile(`Total queries\s+:\s+(\d+)`)
	matches := queriesPattern.FindStringSubmatch(output)
	if len(matches) < 2 {
		t.Errorf("Could not find total queries count in %s output: %s", description, output)
	} else if matches[1] != fmt.Sprintf("%d", expectedQueries) {
		t.Errorf("Expected %s to have %d queries, got %s", description, expectedQueries, matches[1])
	}
	t.Logf("%s collect output shows %s queries", description, matches[1])

	return collectBuf.String()
}

// loadDatasetFromCSV creates a dataset from CSV data string for testing
// Returns the Collector for access to verbose stats and error counts.
func loadDatasetFromCSV(csvData string, dateStr string, verbose bool) (*internal.Collector, error) {
	var date *time.Time
	if dateStr != "" {
		parsedDate, err := time.Parse(time.DateOnly, dateStr)
		if err != nil {
			return nil, err
		}
		date = &parsedDate
	}

	timing := internal.NewTimingStats()
	collector := internal.NewCollector(internal.DefaultDomainCount, 0, verbose, date, timing)
	reader := strings.NewReader(csvData)

	timing.StartParsing()

	err := internal.LoadCSVFromReader(reader, collector, "csv")
	if err != nil {
		return nil, err
	}

	err = collector.Finalise()
	if err != nil {
		return nil, err
	}

	timing.Finish()

	return collector, nil
}
