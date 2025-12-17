package cmd

import (
	"bytes"
	"dnsmag/internal"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
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

func TestViewCmd_OutputDestination(t *testing.T) {
	// Create temporary DNSMAG file
	tmpDnsmag, err := os.CreateTemp("", "test_view_output_*.dnsmag")
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

	tests := []struct {
		name         string
		outputFlag   string
		jsonFlag     bool
		expectIn     string // "stdout", "stderr", or "file"
		searchString string
	}{
		{
			name:         "text output to stderr (default)",
			outputFlag:   "",
			jsonFlag:     false,
			expectIn:     "stderr",
			searchString: "Dataset statistics",
		},
		{
			name:         "text output to file",
			outputFlag:   "output.txt",
			jsonFlag:     false,
			expectIn:     "file",
			searchString: "Dataset statistics",
		},
		{
			name:         "text output to stdout",
			outputFlag:   "-",
			jsonFlag:     false,
			expectIn:     "stdout",
			searchString: "Dataset statistics",
		},
		{
			name:         "JSON output to stderr (default)",
			outputFlag:   "",
			jsonFlag:     true,
			expectIn:     "stderr",
			searchString: "datasetStatistics",
		},
		{
			name:         "JSON output to file",
			outputFlag:   "output.json",
			jsonFlag:     true,
			expectIn:     "file",
			searchString: "datasetStatistics",
		},
		{
			name:         "JSON output to stdout",
			outputFlag:   "-",
			jsonFlag:     true,
			expectIn:     "stdout",
			searchString: "datasetStatistics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viewCmd := newViewCmd()
			args := []string{tmpDnsmag.Name()}
			if tt.jsonFlag {
				args = append(args, "--json")
			}

			var outputPath string
			if tt.outputFlag != "" {
				if tt.expectIn == "file" {
					tmpFile, err := os.CreateTemp("", tt.outputFlag)
					if err != nil {
						t.Fatalf("Failed to create temp file: %v", err)
					}
					outputPath = tmpFile.Name()
					tmpFile.Close()
					defer os.Remove(outputPath)
					args = append(args, "--output", outputPath)
				} else {
					args = append(args, "--output", tt.outputFlag)
				}
			}
			viewCmd.SetArgs(args)

			var stdout, stderr bytes.Buffer
			viewCmd.SetOut(&stdout)
			viewCmd.SetErr(&stderr)

			err := viewCmd.Execute()
			if err != nil {
				t.Fatalf("View command failed: %v\nStdout: %s\nStderr: %s", err, stdout.String(), stderr.String())
			}

			// Check output appears in expected location
			switch tt.expectIn {
			case "stdout":
				if !strings.Contains(stdout.String(), tt.searchString) {
					t.Errorf("Expected %q in stdout", tt.searchString)
				}
			case "stderr":
				if !strings.Contains(stderr.String(), tt.searchString) {
					t.Errorf("Expected %q in stderr", tt.searchString)
				}
			case "file":
				fileContent, err := os.ReadFile(outputPath)
				if err != nil {
					t.Fatalf("Failed to read output file: %v", err)
				}
				if !strings.Contains(string(fileContent), tt.searchString) {
					t.Errorf("Expected %q in file", tt.searchString)
				}
			}
		})
	}
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
