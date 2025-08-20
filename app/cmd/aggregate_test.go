package cmd

import (
	"bytes"
	"dnsmag/internal"
	"os"
	"regexp"
	"testing"
)

func init() {
	internal.InitStats()
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
		regexp.MustCompile(`Total unique source IPs\s+:\s+92 \(estimated\)`),
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
