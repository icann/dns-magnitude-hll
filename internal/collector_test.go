package internal

import (
	"compress/gzip"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

func init() {
	InitStats()
}

func TestCollectorChunking(t *testing.T) {
	// Table-driven test cases
	tests := []struct {
		name           string
		numIPs         uint64
		expectedChunks uint // Expected number of chunks processed
	}{
		{
			name:           "90 IPs chunked",
			numIPs:         90,
			expectedChunks: 9, // 90 IPs with chunk size 10 results in 9 chunks
		},
		{
			name:           "99 IPs chunked",
			numIPs:         99,
			expectedChunks: 10, // 99 IPs with chunk size 10 results in 10 chunks (9 full + 1 partial)
		},
		{
			name:           "100 IPs chunked",
			numIPs:         100,
			expectedChunks: 10, // 100 IPs with chunk size 10 results in 10 chunks
		},
		{
			name:           "101 IPs chunked",
			numIPs:         101,
			expectedChunks: 11, // 101 IPs with chunk size 10 results in 11 chunks (10 full + 1 partial)
		},
	}

	chunkSize := uint(10) // Set a default chunk size for all tests

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary CSV file
			tmpFile, err := os.CreateTemp("", "test_*.csv")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tmpFile.Name())
			defer tmpFile.Close()

			// Generate CSV data with sequential IPs. All queries are for .net for these tests.
			var i uint64
			for i = 1; i <= tt.numIPs; i++ {
				line := fmt.Sprintf("192.168.%d.1,net,1\n", i)
				if _, err := tmpFile.WriteString(line); err != nil {
					t.Fatalf("Failed to write to temp file: %v", err)
				}
			}
			tmpFile.Close()

			// Load temp file using a Collector
			testDate := time.Date(2009, 12, 21, 0, 0, 0, 0, time.UTC)
			timing := NewTimingStats()
			collector := NewCollector(DefaultDomainCount, chunkSize, true, &testDate, timing)

			err = collector.ProcessFiles([]string{tmpFile.Name()}, "csv")
			if err != nil {
				t.Fatalf("ProcessFiles failed: %v", err)
			}

			dataset := collector.Result

			validateDataset(t, dataset, DatasetExpected{
				queriesCount:    tt.numIPs,
				domainCount:     1,
				expectedDomains: []string{"net"},
				invalidDomains:  0,
				invalidRecords:  0,
			}, collector)

			validateDatasetDomains(t, dataset, DatasetDomainsExpected{
				expectedDomains: map[DomainName]uint64{
					"net": tt.numIPs,
				},
			})

			// validate expected number of chunks processed
			if collector.chunkCount != tt.expectedChunks {
				t.Errorf("Expected %d chunks processed, got %d", tt.expectedChunks, collector.chunkCount)
			}

			// Check client counts in verbose mode
			numClients := uint64(len(dataset.extraAllClients))
			if numClients != tt.numIPs {
				t.Errorf("Expected %d unique IPs, got %d", tt.numIPs, numClients)
			}

			// Verify HLL estimate is reasonable (within expected error range)
			hllEstimate := makeInt(dataset.AllClientsCount)
			errorRate := float64(abs(hllEstimate-makeInt(tt.numIPs))) / float64(tt.numIPs)
			if errorRate > 0.05 { // Allow 5% error for HLL
				t.Errorf("HLL estimate %d too far from actual %d (error rate: %.2f%%)",
					hllEstimate, tt.numIPs, errorRate*100)
			}
		})
	}
}

func TestCollectorPcapLoading(t *testing.T) {
	timing := NewTimingStats()
	collector := NewCollector(DefaultDomainCount, 0, true, nil, timing)

	err := collector.ProcessFiles([]string{"../testdata/test1.pcap.gz"}, "pcap")
	if err != nil {
		t.Fatalf("ProcessFiles failed for PCAP: %v", err)
	}

	dataset := collector.Result

	validateDataset(t, dataset, DatasetExpected{
		queriesCount:    100,
		domainCount:     4,
		expectedDomains: []string{"com", "net", "org", "arpa"},
		invalidDomains:  0,
		invalidRecords:  0,
	}, collector)

	// Validate cardinality (estimated unique clients)
	if dataset.AllClientsCount != 70 {
		t.Errorf("Expected 70 clients from PCAP file, got %d", dataset.AllClientsCount)
	}

	// Validate exact number of clients
	if len(dataset.extraAllClients) != 69 {
		t.Errorf("Expected 57 unique clients, got %d", len(dataset.extraAllClients))
	}

	if len(dataset.extraV6Clients) != 1 {
		t.Errorf("Expected 1 unique v6 client, got %d", len(dataset.extraV6Clients))
	}

	// Verify date was set from PCAP packet timestamps
	expectedDate := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	if dataset.Date.Time != expectedDate {
		t.Errorf("Expected date %v, got %v", expectedDate, dataset.Date.Time)
	}
}

func TestCollectorNonExistentFiles(t *testing.T) {
	tests := []struct {
		name     string
		files    []string
		filetype string
		errMsg   string
	}{
		{
			name:     "non-existent CSV file",
			files:    []string{"non-existent.csv"},
			filetype: "csv",
			errMsg:   "failed to load csv file non-existent.csv",
		},
		{
			name:     "non-existent PCAP file",
			files:    []string{"non-existent.pcap"},
			filetype: "pcap",
			errMsg:   "failed to load pcap file non-existent.pcap",
		},
		{
			name:     "multiple non-existent CSV files",
			files:    []string{"missing1.csv", "missing2.csv"},
			filetype: "csv",
			errMsg:   "failed to load csv file missing1.csv",
		},
		{
			name:     "mixed existing and non-existent files",
			files:    []string{"../testdata/test1.pcap.gz", "missing.pcap"},
			filetype: "pcap",
			errMsg:   "failed to load pcap file missing.pcap",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timing := NewTimingStats()
			collector := NewCollector(DefaultDomainCount, 0, false, nil, timing)

			err := collector.ProcessFiles(tt.files, tt.filetype)

			if err == nil {
				t.Error("Expected error for non-existent file, got nil")
				return
			}

			if !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("Expected error message to contain '%s', got: %v", tt.errMsg, err)
			}
		})
	}
}

func TestCollectorFileLoadingError(t *testing.T) {
	tests := []struct {
		name      string
		filetype  string
		chunkSize uint
		setup     func() (string, func())
		errMsg    string
	}{
		{
			name:      "CSV aggregation error",
			filetype:  "csv",
			chunkSize: 10,
			setup: func() (string, func()) {
				tmpFile, err := os.CreateTemp("", "test_*.csv")
				if err != nil {
					t.Fatalf("Failed to create temp file: %v", err)
				}
				tmpFile.WriteString("192.168.1.1,example.com,1\n")
				tmpFile.Close()
				return tmpFile.Name(), func() { os.Remove(tmpFile.Name()) }
			},
			errMsg: "failed to finalise collection: failed to migrate current dataset: failed to aggregate datasets: version mismatch:",
		},
		{
			name:      "CSV aggregation error with chunking",
			filetype:  "csv",
			chunkSize: 2,
			setup: func() (string, func()) {
				tmpFile, err := os.CreateTemp("", "test_*.csv")
				if err != nil {
					t.Fatalf("Failed to create temp file: %v", err)
				}
				csvData := `192.168.1.1,example.com,1
192.168.1.2,example.org,1
192.168.1.3,example.net,1
`

				tmpFile.WriteString(csvData)
				tmpFile.Close()
				return tmpFile.Name(), func() { os.Remove(tmpFile.Name()) }
			},
			errMsg: "failed to parse CSV: failed to process CSV record at line 2: failed to process record: failed to migrate current dataset: failed to aggregate datasets: version mismatch: dataset",
		},
		{
			name:      "PCAP aggregation error",
			filetype:  "pcap",
			chunkSize: 10,
			setup: func() (string, func()) {
				return "../testdata/test1.pcap.gz", func() {}
			},
			errMsg: "version mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename, cleanup := tt.setup()
			defer cleanup()

			timing := NewTimingStats()
			collector := NewCollector(DefaultDomainCount, tt.chunkSize, false, nil, timing)

			// Modify the Result dataset version to make file loading fail
			collector.Result.Version++

			err := collector.ProcessFiles([]string{filename}, tt.filetype)

			if err == nil {
				t.Error("Expected version mismatch error, got nil")
				return
			}

			if !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("Expected error message to contain '%s', got: %v", tt.errMsg, err)
			}
		})
	}
}

func TestCollectorGzippedCSV(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_*.csv.gz")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	gzipWriter := gzip.NewWriter(tmpFile)
	defer gzipWriter.Close()

	csvData := `# Test gzipped CSV data
192.168.1.1,example.com,5
192.168.1.2,example.org,3
10.0.0.1,example.net,2
192.168.1.3,invalid-domain.example.123,1`

	if _, err := gzipWriter.Write([]byte(csvData)); err != nil {
		t.Fatalf("Failed to write to gzip writer: %v", err)
	}
	gzipWriter.Close()
	tmpFile.Close()

	testDate := time.Date(2009, 12, 21, 0, 0, 0, 0, time.UTC)
	timing := NewTimingStats()
	collector := NewCollector(DefaultDomainCount, 0, true, &testDate, timing)

	err = collector.ProcessFiles([]string{tmpFile.Name()}, "csv")
	if err != nil {
		t.Fatalf("ProcessFiles failed for gzipped CSV: %v", err)
	}

	dataset := collector.Result

	validateDataset(t, dataset, DatasetExpected{
		queriesCount:    11, // 5 + 3 + 2 + 1, count all queries - even invalid ones
		domainCount:     3,
		expectedDomains: []string{"com", "org", "net"},
		invalidDomains:  1,
		invalidRecords:  0,
	}, collector)
}

// Helper function to safely convert uint64 to int without overflow
func makeInt(u uint64) int {
	if u > uint64(math.MaxInt) {
		return math.MaxInt
	}
	return int(u)
}

// Helper function for absolute value
func abs(x int) int {
	return max(x, -x)
}
