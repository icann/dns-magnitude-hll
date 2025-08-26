package cmd

import (
	"bytes"
	"dnsmag/internal"
	"io"
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
		regexp.MustCompile(`Aggregated statistics for 2 datasets:`),
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

func TestAggregateCmd_StdinDatasets(t *testing.T) {
	tests := []struct {
		name        string
		setupReader func(fullData []byte) io.Reader
		description string
	}{
		{
			name: "normal stdin read",
			setupReader: func(fullData []byte) io.Reader {
				return bytes.NewBuffer(fullData)
			},
			description: "multiple datasets from stdin",
		},
		{
			name: "partial stdin read",
			setupReader: func(fullData []byte) io.Reader {
				return &slowReader{
					data:      fullData,
					chunkSize: 10, // Very small chunks to simulate a slow writer feeding us datasets
					pos:       0,
				}
			},
			description: "partial read simulation with slow writer",
		},
		{
			name: "1.5 datasets partial stdin read",
			setupReader: func(fullData []byte) io.Reader {
				return &slowReader{
					data:      fullData,
					chunkSize: 200, // The two datasets are about 159 bytes each
					pos:       0,
				}
			},
			description: "partial read simulation with slow writer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			csvDataSets := []string{
				`192.168.1.1,example.com,25
192.168.2.1,example.com,25`,
				`10.0.1.1,example.org,15
10.0.2.2,example.org,15`,
			}

			// Encode datasets to CBOR sequence
			var testStdin []byte

			for i, csvData := range csvDataSets {
				collector, err := loadDatasetFromCSV(csvData, "2025-08-21", false)
				if err != nil {
					t.Fatalf("loadDatasetFromCSV failed for dataset %d: %v", i+1, err)
				}

				datasetBytes, err := internal.MarshalDatasetToCBOR(collector.Result)
				if err != nil {
					t.Fatalf("Failed to marshal dataset %d: %v", i+1, err)
				}
				testStdin = append(testStdin, datasetBytes...)
			}

			// Setup reader behaving differently based on test case
			reader := tt.setupReader(testStdin)

			// Create aggregate command with stdin input
			aggregateCmd := newAggregateCmd()
			aggregateCmd.SetArgs([]string{"-"})
			aggregateCmd.SetIn(reader)

			var aggregateBuf bytes.Buffer
			aggregateCmd.SetOut(&aggregateBuf)
			aggregateCmd.SetErr(&aggregateBuf)

			err := aggregateCmd.Execute()
			if err != nil {
				t.Fatalf("Aggregate command failed: %v\nOutput: %s", err, aggregateBuf.String())
			}

			output := aggregateBuf.String()

			// Verify aggregated output contains expected patterns
			expectedPatterns := []*regexp.Regexp{
				regexp.MustCompile(`Aggregated statistics for 2 datasets:`),
				regexp.MustCompile(`Dataset statistics`),
				regexp.MustCompile(`Total queries\s+:\s+80`), // 50 + 30
				regexp.MustCompile(`Total domains\s+:\s+2`),  // com + org
			}

			for _, pattern := range expectedPatterns {
				if !pattern.MatchString(output) {
					t.Errorf("Expected pattern %q not found in output:\n%s", pattern.String(), output)
				}
			}

			// Additional verification for slow reader
			if slowReader, ok := reader.(*slowReader); ok {
				if slowReader.pos != len(testStdin) {
					t.Errorf("Expected slow reader to read all %d bytes, but only read %d", len(testStdin), slowReader.pos)
				}
			}

			t.Logf("Stdin test %s command output:\n%s", tt.description, output)
		})
	}
}

// slowReader simulates a slow reader that only returns small chunks at a time
type slowReader struct {
	data      []byte
	chunkSize int
	pos       int
}

func (sr *slowReader) Read(out []byte) (n int, err error) {
	if sr.pos >= len(sr.data) {
		return 0, io.EOF
	}

	// Calculate how much to read (minimum of chunk size, buffer size, and remaining data)
	remaining := len(sr.data) - sr.pos
	numBytes := min(sr.chunkSize, len(out), remaining)

	copy(out, sr.data[sr.pos:sr.pos+numBytes])
	sr.pos += numBytes

	return numBytes, nil
}
