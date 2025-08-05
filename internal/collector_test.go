package internal

import (
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

			// Verify basic properties
			if dataset.AllQueriesCount != tt.numIPs {
				t.Errorf("Expected %d queries, got %d", tt.numIPs, dataset.AllQueriesCount)
			}

			numClients := uint64(len(dataset.extraAllClients))
			if numClients != tt.numIPs {
				t.Errorf("Expected %d unique IPs, got %d", tt.numIPs, numClients)
			}

			numDomains := len(dataset.extraAllDomains)
			if numDomains != 1 {
				t.Errorf("Expected %d unique domains, got %d", 1, numDomains)
			}

			// validate expected number of chunks processed
			if collector.chunkCount != tt.expectedChunks {
				t.Errorf("Expected %d chunks processed, got %d", tt.expectedChunks, collector.chunkCount)
			}

			// Check that "net" domain exists
			netDomain, exists := dataset.Domains[DomainName("net")]
			if !exists {
				t.Fatal("Expected domain not found")
			}

			if netDomain.QueriesCount != tt.numIPs {
				t.Errorf("Expected %d queries for .net domain, got %d", tt.numIPs, netDomain.QueriesCount)
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
