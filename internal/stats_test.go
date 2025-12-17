package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func init() {
	InitStats()
}

func TestCountAsString(t *testing.T) {
	tests := []struct {
		name      string
		actual    uint
		estimated uint
		expected  string
	}{
		{
			name:      "exact match",
			actual:    100,
			estimated: 100,
			expected:  "100 (estimated: 100, diff: +0.00%)",
		},
		{
			name:      "estimated higher",
			actual:    100,
			estimated: 105,
			expected:  "100 (estimated: 105, diff: +5.00%)",
		},
		{
			name:      "estimated lower",
			actual:    100,
			estimated: 95,
			expected:  "100 (estimated: 95, diff: −5.00%)",
		},
		{
			name:      "only estimated",
			actual:    0,
			estimated: 50,
			expected:  "50 (estimated)",
		},
		{
			name:      "numbers larger than maxInt",
			actual:    18446744073709551615,
			estimated: 18446744073709551610,
			expected:  "18446744073709551615 (estimated: 18446744073709551610)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := countAsString(tt.actual, tt.estimated)
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestOutputDatasetStats(t *testing.T) {
	// Initialize test dataset using CSV data
	csvData := `# Test CSV data
192.168.1.10,example.com,5
192.168.1.20,example.org,3
10.0.0.5,example.com,2
2001:db8::1,example.net,1`

	collector, err := loadDatasetFromCSV(csvData, "2009-12-21", false)
	if err != nil {
		t.Fatalf("loadDatasetFromCSV failed: %v", err)
	}
	dataset := collector.Result

	// Validate the dataset before testing output
	validateDataset(t, dataset, DatasetExpected{
		queriesCount:    11,
		domainCount:     3,
		expectedDomains: []string{"com", "org", "net"},
		invalidDomains:  0,
		invalidRecords:  0,
	}, collector)

	validateDatasetDomains(t, dataset, DatasetDomainsExpected{
		expectedDomains: map[DomainName]uint64{
			"com": 7,
			"org": 3,
			"net": 1,
		},
	})

	tests := []struct {
		name     string
		verbose  bool
		contains []string
	}{
		{
			name:    "basic output",
			verbose: false,
			contains: []string{
				"Dataset statistics",
				"Date",
				"Total queries",
				"Total domains",
				"Total unique source IPs",
				"All clients HLL storage size",
			},
		},
		{
			name:    "verbose output",
			verbose: true,
			contains: []string{
				"Dataset statistics",
				"Domain counts:",
				"com",
				"org",
				"net",
				"magnitude:",
				"queries",
				"clients",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := OutputDatasetStats(&buf, dataset, tt.verbose)
			if err != nil {
				t.Fatalf("OutputDatasetStats failed: %v", err)
			}

			output := buf.String()
			for _, expected := range tt.contains {
				if !strings.Contains(output, expected) {
					t.Errorf("Expected output to contain '%s', but it didn't.\nOutput:\n%s", expected, output)
				}
			}
		})
	}
}

func TestOutputDatasetStatsJSON(t *testing.T) {
	// Initialize test dataset using CSV data
	csvData := `# Test CSV data
192.168.1.10,example.com,5
192.168.1.20,example.org,3
10.0.0.5,example.com,2
2001:db8::1,example.net,1`

	collector, err := loadDatasetFromCSV(csvData, "2009-12-21", false)
	if err != nil {
		t.Fatalf("loadDatasetFromCSV failed: %v", err)
	}
	dataset := collector.Result

	// Validate the dataset before testing output
	validateDataset(t, dataset, DatasetExpected{
		queriesCount:    11,
		domainCount:     3,
		expectedDomains: []string{"com", "org", "net"},
		invalidDomains:  0,
		invalidRecords:  0,
	}, collector)

	var buf bytes.Buffer
	err = OutputDatasetStatsJSON(&buf, dataset)
	if err != nil {
		t.Fatalf("OutputDatasetStatsJSON failed: %v", err)
	}

	// Parse JSON output
	var stats DatasetStatsJSON
	if err := json.Unmarshal(buf.Bytes(), &stats); err != nil {
		t.Fatalf("Failed to unmarshal JSON output: %v", err)
	}

	// Verify random fields are non-empty before overwriting
	if stats.DatasetStatistics.ID == "" {
		t.Error("Expected non-empty ID")
	}

	// Overwrite random ID field for comparison
	stats.DatasetStatistics.ID = ""

	expected := DatasetStatsJSON{
		DatasetStatistics: DatasetStats{
			ID:                 "",
			Generator:          fmt.Sprintf("dnsmag %s", Version),
			Date:               "2009-12-21",
			TotalUniqueClients: 4,
			TotalQueryVolume:   11,
			TotalDomainCount:   3,
		},
	}

	if stats != expected {
		t.Errorf("JSON output mismatch.\nGot:      %+v\nExpected: %+v", stats, expected)
	}
}

func TestOutputCollectorStats(t *testing.T) {
	// Initialize test dataset using CSV data
	csvData := `# Test CSV data
192.168.1.10,example.com,5
192.168.1.20,example.org,3
10.0.0.5,example.com,2`

	collector, err := loadDatasetFromCSV(csvData, "2009-12-21", true)
	if err != nil {
		t.Fatalf("loadDatasetFromCSV failed: %v", err)
	}
	collector.filesLoaded = []string{"test1.csv", "test2.csv"}

	validateDataset(t, collector.Result, DatasetExpected{
		queriesCount:    10,
		domainCount:     2,
		expectedDomains: []string{"com", "org"},
		invalidDomains:  0,
		invalidRecords:  0,
	}, collector)

	validateDatasetDomains(t, collector.Result, DatasetDomainsExpected{
		expectedDomains: map[DomainName]uint64{
			"com": 7,
			"org": 3,
		},
	})

	tests := []struct {
		name     string
		verbose  bool
		contains []string
	}{
		{
			name:    "basic collector stats",
			verbose: false,
			contains: []string{
				"Aggregated statistics for 2 files",
				"Collection statistics",
				"Files loaded",
				"Records processed",
				"Invalid records",
				"Invalid domains",
				"Memory allocated",
				"Timing statistics",
				"Total execution time",
				"2 (estimated: 3, diff: +50.00%)",
			},
		},
		{
			name:    "verbose collector stats",
			verbose: true,
			contains: []string{
				"Domain counts:",
				"magnitude:",
				"Files loaded",
				"Records processed",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := OutputCollectorStats(&buf, collector, tt.verbose)
			if err != nil {
				t.Fatalf("OutputCollectorStats failed: %v", err)
			}

			output := buf.String()
			for _, expected := range tt.contains {
				if !strings.Contains(output, expected) {
					t.Errorf("Expected output to contain '%s', but it didn't.\nOutput:\n%s", expected, output)
				}
			}
		})
	}
}

func TestOutputCollectorStats_WriteErrors(t *testing.T) {
	// Initialize test dataset using CSV data
	csvData := `# Test CSV data
192.168.1.10,example.com,5
192.168.1.20,example.org,3
10.0.0.5,example.com,2`

	collector, err := loadDatasetFromCSV(csvData, "2009-12-21", true)
	if err != nil {
		t.Fatalf("loadDatasetFromCSV failed: %v", err)
	}
	collector.filesLoaded = []string{"test.csv"}

	// First, determine the full output length with a normal buffer
	var fullBuf bytes.Buffer
	err = OutputCollectorStats(&fullBuf, collector, false)
	if err != nil {
		t.Fatalf("OutputCollectorStats failed: %v", err)
	}

	fullLength := fullBuf.Len()
	t.Logf("Full output length: %d bytes", fullLength)

	// Test with limited buffers of various sizes
	// All sizes except fullLength should fail
	testSizes := []int{
		0,               // Immediate failure
		1,               // Very small
		fullLength / 4,  // Quarter length
		fullLength / 2,  // Half length
		fullLength - 10, // Near full length
		fullLength - 1,  // One byte short
		fullLength,      // Exact length (should succeed)
	}

	for _, size := range testSizes {
		t.Run(fmt.Sprintf("limit_%d", size), func(t *testing.T) {
			limitedBuf := &limitedBuffer{
				data:  make([]byte, 0),
				limit: size,
			}

			err := OutputCollectorStats(limitedBuf, collector, false)

			if size < fullLength {
				// Should fail for sizes smaller than full length
				if err == nil {
					t.Errorf("Expected error for buffer limit %d (full length: %d), but got none", size, fullLength)
				}
			} else {
				// Should succeed for sizes >= full length
				if err != nil {
					t.Errorf("Expected no error for buffer limit %d (full length: %d), but got: %v", size, fullLength, err)
				}
			}
		})
	}
}

func TestOutputTimingStats(t *testing.T) {
	tests := []struct {
		name     string
		timing   *TimingStats
		contains []string
		isEmpty  bool
	}{
		{
			name:    "nil timing stats",
			timing:  nil,
			isEmpty: true,
		},
		{
			name: "valid timing stats",
			timing: func() *TimingStats {
				ts := NewTimingStats()
				ts.StartParsing()
				time.Sleep(1 * time.Millisecond)
				ts.StopParsing()
				ts.Finish()
				return ts
			}(),
			contains: []string{
				"Timing statistics",
				"Total execution time",
				"File parsing time",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := OutputTimingStats(&buf, tt.timing)
			if err != nil {
				t.Fatalf("OutputTimingStats failed: %v", err)
			}

			output := buf.String()

			if tt.isEmpty {
				if output != "" {
					t.Errorf("Expected empty output for nil timing, got: %s", output)
				}
				return
			}

			for _, expected := range tt.contains {
				if !strings.Contains(output, expected) {
					t.Errorf("Expected output to contain '%s', but it didn't.\nOutput:\n%s", expected, output)
				}
			}
		})
	}
}

func TestOutputTimingStats_WriteErrors(t *testing.T) {
	// Create timing stats with some data
	timing := NewTimingStats()
	timing.StartParsing()
	time.Sleep(1 * time.Millisecond) // Small delay for timing
	timing.StopParsing()
	timing.Finish()

	// First, determine the full output length with a normal buffer
	var fullBuf bytes.Buffer
	err := OutputTimingStats(&fullBuf, timing)
	if err != nil {
		t.Fatalf("OutputTimingStats failed: %v", err)
	}

	fullLength := fullBuf.Len()
	t.Logf("Full timing output length: %d bytes", fullLength)

	testSizes := []int{
		fullLength - 1, // One byte short
		fullLength,     // Exact length (should succeed)
	}

	for _, size := range testSizes {
		t.Run(fmt.Sprintf("limit_%d", size), func(t *testing.T) {
			limitedBuf := &limitedBuffer{
				data:  make([]byte, 0),
				limit: size,
			}

			err := OutputTimingStats(limitedBuf, timing)

			if size < fullLength {
				// Should fail for sizes smaller than full length
				if err == nil {
					t.Errorf("Expected error for buffer limit %d (full length: %d), but got none", size, fullLength)
				}
			} else {
				// Should succeed for sizes >= full length
				if err != nil {
					t.Errorf("Expected no error for buffer limit %d (full length: %d), but got: %v", size, fullLength, err)
				}
			}
		})
	}
}

func TestFormatDatasetStats(t *testing.T) {
	// Initialize test dataset using CSV data
	csvData := `192.168.1.10,example.com,5
192.168.1.20,example.org,3`

	collector, err := loadDatasetFromCSV(csvData, "2009-12-21", false)
	if err != nil {
		t.Fatalf("loadDatasetFromCSV failed: %v", err)
	}
	dataset := collector.Result

	// Validate the dataset before testing formatting
	validateDataset(t, dataset, DatasetExpected{
		queriesCount:    8,
		domainCount:     2,
		expectedDomains: []string{"com", "org"},
		invalidDomains:  0,
		invalidRecords:  0,
	}, collector)

	validateDatasetDomains(t, dataset, DatasetDomainsExpected{
		expectedDomains: map[DomainName]uint64{
			"com": 5,
			"org": 3,
		},
	})

	table, domains := formatDatasetStats(dataset)

	// Verify table contains expected rows
	expectedRowTypes := []string{
		"Dataset statistics",
		"Date",
		"Total queries",
		"Total domains",
		"Total unique source IPs",
	}

	foundRows := make(map[string]bool)
	for _, row := range table {
		for _, expected := range expectedRowTypes {
			if strings.Contains(row.lhs, expected) {
				foundRows[expected] = true
			}
		}
	}

	for _, expected := range expectedRowTypes {
		if !foundRows[expected] {
			t.Errorf("Expected to find row containing '%s' in table", expected)
		}
	}

	// Verify domains list
	if len(domains) != 2 {
		t.Errorf("Expected 2 domains in list, got %d", len(domains))
	}

	for _, domain := range domains {
		if !strings.Contains(domain, "magnitude:") || !strings.Contains(domain, "queries") {
			t.Errorf("Expected domain entry to contain magnitude and queries info, got: %s", domain)
		}
	}
}

func TestCollectorAggregation_OutputVerification(t *testing.T) {
	// Create two separate datasets to aggregate
	csvData1 := `192.168.1.1,example.com,5
192.168.1.2,example.org,3`

	csvData2 := `10.0.0.1,example.com,2
10.0.0.2,example.net,1`

	collector1, err := loadDatasetFromCSV(csvData1, "2007-09-09", false)
	if err != nil {
		t.Fatalf("loadDatasetFromCSV failed for dataset 1: %v", err)
	}

	collector2, err := loadDatasetFromCSV(csvData2, "2007-09-09", false)
	if err != nil {
		t.Fatalf("loadDatasetFromCSV failed for dataset 2: %v", err)
	}

	// Aggregate the datasets
	aggregated, err := AggregateDatasets([]MagnitudeDataset{collector1.Result, collector2.Result})
	if err != nil {
		t.Fatalf("AggregateDatasets failed: %v", err)
	}

	// clear extraAllClients to avoid verbose mode output
	aggregated.extraAllClients = nil
	aggregated.extraAllDomains = nil

	// Generate output from the aggregated dataset using OutputDatasetStats
	var buf bytes.Buffer
	err = OutputDatasetStats(&buf, aggregated, false)
	if err != nil {
		t.Fatalf("OutputDatasetStats failed: %v", err)
	}

	output := buf.String()

	t.Logf("Aggregated output:\n%s", output)

	// Verify aggregated output contains expected strings
	expectedStrings := []string{
		"Dataset statistics",
		": 3",
		"Total unique source IPs",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected aggregated output to contain '%s', but it didn't", expected)
		}
	}

	// Verify that pre-aggregation specific strings are NOT present
	// In aggregated datasets, there should be no verbose mode client tracking,
	// so no "estimated:" or "diff:" strings should appear
	unexpectedStrings := []string{
		"estimated:",
		"diff:",
		"+50.00%",
		"−25.00%",
	}

	for _, unexpected := range unexpectedStrings {
		if strings.Contains(output, unexpected) {
			t.Errorf("Aggregated output should not contain pre-aggregation string '%s', but it did.\nOutput:\n%s", unexpected, output)
		}
	}
}

func TestPrintTable(t *testing.T) {
	tests := []struct {
		name     string
		rows     []TableRow
		expected string
	}{
		{
			name: "basic table",
			rows: []TableRow{
				{"Column 1", "Value 1"},
				{"Column 2", "Value 2"},
			},
			expected: "Column 1 : Value 1\nColumn 2 : Value 2\n",
		},
		{
			name: "table with separator",
			rows: []TableRow{
				{"Header", "Info"},
				{"", ""}, // separator
				{"Data 1", "Content 1"},
				{"Data 2", "Content 2"},
			},
			expected: "Header : Info\n\nData 1 : Content 1\nData 2 : Content 2\n",
		},
		{
			name: "table with different column widths",
			rows: []TableRow{
				{"Short", "Value"},
				{"Longer", "Another Value"},
			},
			expected: "Short  : Value\nLonger : Another Value\n",
		},
		{
			name: "multiple separators",
			rows: []TableRow{
				{"Section A", "Data A"},
				{"", ""}, // separator
				{"Section B", "Data B"},
				{"", ""}, // separator
				{"Section C", "Data C"},
			},
			expected: "Section A : Data A\n\nSection B : Data B\n\nSection C : Data C\n",
		},
		{
			name:     "empty table",
			rows:     []TableRow{},
			expected: "",
		},
		{
			name: "only separators",
			rows: []TableRow{
				{"", ""},
				{"", ""},
			},
			expected: "\n\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := printTable(&buf, tt.rows)
			if err != nil {
				t.Fatalf("printTable failed: %v", err)
			}

			result := buf.String()
			if result != tt.expected {
				t.Errorf("Expected:\n%q\nGot:\n%q", tt.expected, result)
			}
		})
	}
}

func TestPrintTable_WriteError(t *testing.T) {
	tests := []struct {
		name        string
		rows        []TableRow
		limit       int
		expectError bool
	}{
		{
			name: "separator write error",
			rows: []TableRow{
				{"", ""}, // separator that should fail
			},
			limit:       0, // Immediate failure
			expectError: true,
		},
		{
			name: "normal row write error",
			rows: []TableRow{
				{"Column", "Value"},
			},
			limit:       5, // Allow some characters but not the full row
			expectError: true,
		},
		{
			name: "successful output",
			rows: []TableRow{
				{"", ""}, // separator
				{"Test", "Data"},
			},
			limit:       100, // Enough space for all output
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limitedBuf := &limitedBuffer{
				data:  make([]byte, 0),
				limit: tt.limit,
			}

			err := printTable(limitedBuf, tt.rows)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for limited buffer with limit %d, but got none", tt.limit)
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error for sufficient buffer limit %d, but got: %v", tt.limit, err)
				}
			}
		})
	}
}

// limitedBuffer is a buffer that returns an error after writing a specific number of bytes
type limitedBuffer struct {
	data    []byte
	limit   int
	written int
}

func (lb *limitedBuffer) Write(p []byte) (n int, err error) {
	if lb.written >= lb.limit {
		return 0, fmt.Errorf("write limit exceeded")
	}

	available := lb.limit - lb.written
	if len(p) > available {
		// Write only what we can, then return error
		lb.data = append(lb.data, p[:available]...)
		lb.written += available
		return available, fmt.Errorf("write limit exceeded")
	}

	lb.data = append(lb.data, p...)
	lb.written += len(p)
	return len(p), nil
}
