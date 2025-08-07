package cmd

import (
	"bytes"
	"dnsmag/internal"
	"fmt"
	"os"
	"regexp"
	"testing"
)

func init() {
	internal.InitStats()
}

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

func TestAggregateCmd_Integration(t *testing.T) {
	// Create temporary DNSMAG files
	tmpDnsmag1, err := os.CreateTemp("", "test1_*.dnsmag")
	if err != nil {
		t.Fatalf("Failed to create temp DNSMAG file 1: %v", err)
	}
	defer os.Remove(tmpDnsmag1.Name())
	tmpDnsmag1.Close()

	tmpDnsmag2, err := os.CreateTemp("", "test2_*.dnsmag")
	if err != nil {
		t.Fatalf("Failed to create temp DNSMAG file 2: %v", err)
	}
	defer os.Remove(tmpDnsmag2.Name())
	tmpDnsmag2.Close()

	// Execute collect commands and verify query counts
	executeCollectAndVerify(t, []string{
		"../../testdata/test1.pcap.gz",
		"--output", tmpDnsmag1.Name(),
	}, 100, "PCAP")

	executeCollectAndVerify(t, []string{
		"../../testdata/test2.csv.gz",
		"--filetype", "csv",
		"--date", "2000-01-01",
		"--output", tmpDnsmag2.Name(),
	}, 200, "CSV")

	// Aggregate command: combine the two DNSMAG files
	aggregateCmd := newAggregateCmd()
	aggregateCmd.SetArgs([]string{
		tmpDnsmag1.Name(),
		tmpDnsmag2.Name(),
	})

	var aggregateBuf bytes.Buffer
	aggregateCmd.SetOut(&aggregateBuf)
	aggregateCmd.SetErr(&aggregateBuf)

	err = aggregateCmd.Execute()
	if err != nil {
		t.Fatalf("Aggregate command failed: %v\nOutput: %s", err, aggregateBuf.String())
	}

	output := aggregateBuf.String()

	// Expected output patterns
	expectedPatterns := []*regexp.Regexp{
		regexp.MustCompile(`Aggregated statistics for 2 files:`),
		regexp.MustCompile(`Dataset statistics`),
		regexp.MustCompile(`Date\s+:\s+2000-01-01`),
		regexp.MustCompile(`Total queries\s+:\s+300`),
		regexp.MustCompile(`Total domains\s+:\s+7`),
		regexp.MustCompile(`Total unique source IPs\s+:\s+81 \(estimated\)`),
		regexp.MustCompile(`Timing statistics`),
		regexp.MustCompile(`Total execution time`),
	}

	for _, pattern := range expectedPatterns {
		if !pattern.MatchString(output) {
			t.Errorf("Expected pattern %q not found in output:\n%s", pattern.String(), output)
		}
	}

	t.Logf("Aggregated command output:\n%s", output)
}
