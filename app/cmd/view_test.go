package cmd

import (
	"bytes"
	"dnsmag/internal"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"testing"
)

func TestViewCmd_Integration(t *testing.T) {
	// Create temporary DNSMAG file
	tmpDnsmag, err := os.CreateTemp("", "test_view_*.dnsmag")
	if err != nil {
		t.Fatalf("Failed to create temp DNSMAG file: %v", err)
	}
	defer os.Remove(tmpDnsmag.Name())
	tmpDnsmag.Close()

	// Collect data into temporary DNSMAG file
	executeCollectAndVerify(t, []string{
		"../../testdata/test1.pcap.gz",
		"--output", tmpDnsmag.Name(),
	}, 100, "PCAP")

	// View the data (verbose output)
	viewCmd := newViewCmd()
	viewCmd.SetArgs([]string{
		tmpDnsmag.Name(),
		"--verbose",
	})

	var viewBuf bytes.Buffer
	viewCmd.SetOut(&viewBuf)
	viewCmd.SetErr(&viewBuf)

	err = viewCmd.Execute()
	if err != nil {
		t.Fatalf("View command failed: %v\nOutput: %s", err, viewBuf.String())
	}

	output := viewBuf.String()

	expectedPatterns := []*regexp.Regexp{
		regexp.MustCompile(`Dataset statistics`),
		regexp.MustCompile(`Date\s+:\s+2000-01-01`),
		regexp.MustCompile(`Total queries\s+:\s+100`),
		regexp.MustCompile(`Total domains\s+:\s+4`),
		regexp.MustCompile(`Total unique source IPs\s+:\s+70 \(estimated\)`),
		regexp.MustCompile(`Domain counts:`),
		regexp.MustCompile(`arpa.*magnitude: 6.037, queries 16, clients 13 \(estimated\), hll size 32`),
		regexp.MustCompile(`com.*magnitude: 6.037, queries 17, clients 13 \(estimated\), hll size 32`),
		regexp.MustCompile(`net.*magnitude: 6.374, queries 20, clients 15 \(estimated\), hll size 37`),
		regexp.MustCompile(`org.*magnitude: 7.380, queries 24, clients 23 \(estimated\), hll size 56`),
	}

	for _, pattern := range expectedPatterns {
		if !pattern.MatchString(output) {
			t.Errorf("Expected pattern %q not found in output:\n%s", pattern.String(), output)
		}
	}

	t.Logf("View command output:\n%s", output)
}

func TestViewCmd_JSON(t *testing.T) {
	// Create temporary DNSMAG file
	tmpDnsmag, err := os.CreateTemp("", "test_view_json_*.dnsmag")
	if err != nil {
		t.Fatalf("Failed to create temp DNSMAG file: %v", err)
	}
	defer os.Remove(tmpDnsmag.Name())
	tmpDnsmag.Close()

	// Collect data into temporary DNSMAG file
	executeCollectAndVerify(t, []string{
		"../../testdata/test1.pcap.gz",
		"--output", tmpDnsmag.Name(),
	}, 100, "PCAP")

	// View the data with JSON output
	viewCmd := newViewCmd()
	viewCmd.SetArgs([]string{
		tmpDnsmag.Name(),
		"--json",
	})

	var viewBuf bytes.Buffer
	viewCmd.SetOut(&viewBuf)
	viewCmd.SetErr(&viewBuf)

	err = viewCmd.Execute()
	if err != nil {
		t.Fatalf("View command with --json failed: %v\nOutput: %s", err, viewBuf.String())
	}

	output := viewBuf.String()

	// Parse JSON output
	var stats internal.DatasetStatsJSON
	if err := json.Unmarshal(viewBuf.Bytes(), &stats); err != nil {
		t.Fatalf("Failed to parse JSON output: %v\nOutput: %s", err, output)
	}

	// Verify ID is non-empty before overwriting
	if stats.DatasetStatistics.ID == "" {
		t.Error("Expected non-empty ID")
	}

	// Overwrite random ID field for comparison
	stats.DatasetStatistics.ID = ""

	expected := internal.DatasetStatsJSON{
		DatasetStatistics: internal.DatasetStats{
			ID:                 "",
			Generator:          fmt.Sprintf("dnsmag %s", internal.Version),
			Date:               "2000-01-01",
			TotalUniqueClients: 70,
			TotalQueryVolume:   100,
			TotalDomainCount:   4,
		},
	}

	if stats != expected {
		t.Errorf("JSON output mismatch.\nGot:      %+v\nExpected: %+v", stats, expected)
	}

	t.Logf("View --json output:\n%s", output)
}

func TestViewCmd_NonExistentFile(t *testing.T) {
	viewCmd := newViewCmd()
	viewCmd.SetArgs([]string{"non-existent.dnsmag"})

	var viewBuf bytes.Buffer
	viewCmd.SetOut(&viewBuf)
	viewCmd.SetErr(&viewBuf)

	err := viewCmd.Execute()
	if err == nil {
		t.Error("Expected error for non-existent file, got none")
		return
	}

	// Verify error message
	if !regexp.MustCompile(`failed to load DNSMAG file`).MatchString(err.Error()) {
		t.Errorf("Expected error about loading DNSMAG file, got: %v", err)
	}
}
