package internal

import (
	"testing"
	"time"
)

func init() {
	InitStats()
}

func TestLoadPcap_TestData(t *testing.T) {
	tests := []struct {
		name                      string
		pcapFile                  string
		expectedQueryCount        uint64
		expectedClientCount       uint64
		expectedExtraClientsLen   int
		expectedExtraV6ClientsLen int
		expectedDomains           map[DomainName]uint64 // domain -> query count
		expectError               bool
	}{
		{
			name:                      "test1.pcap validation",
			pcapFile:                  "../testdata/test1.pcap.gz",
			expectedQueryCount:        100,
			expectedClientCount:       70, // the HLL cardinality
			expectedExtraClientsLen:   69, // the real number of unique clients
			expectedExtraV6ClientsLen: 1,
			expectedDomains: map[DomainName]uint64{
				"com":  17,
				"net":  20,
				"org":  24,
				"arpa": 16,
				// ".":    23,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create collector for processing
			testDate := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
			timing := NewTimingStats()
			collector := NewCollector(DefaultDomainCount, 0, true, &testDate, timing)

			// Load and process the PCAP file
			err := LoadPcap(tt.pcapFile, collector)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("LoadPcap failed: %v", err)
			}

			// Finalize the collector to get final results
			collector.finalise()
			dataset := collector.Result

			validateDataset(t, dataset, DatasetExpected{
				queriesCount:    tt.expectedQueryCount,
				domainCount:     len(tt.expectedDomains), // The root domain "." is not counted
				expectedDomains: []string{"com", "net", "org", "arpa"},
				invalidDomains:  0,
				invalidRecords:  0,
			}, collector)

			// Validate client count (require exact match for test data)
			if dataset.AllClientsCount != tt.expectedClientCount {
				t.Errorf("Expected client count %d, got %d", tt.expectedClientCount, dataset.AllClientsCount)
			}

			if len(dataset.extraAllClients) != tt.expectedExtraClientsLen {
				t.Errorf("Expected %d extra clients, got %d", tt.expectedExtraClientsLen, len(dataset.extraAllClients))
			}

			if len(dataset.extraV6Clients) != tt.expectedExtraV6ClientsLen {
				t.Errorf("Expected %d extra v6 clients, got %d", tt.expectedExtraV6ClientsLen, len(dataset.extraV6Clients))
			}

			validateDatasetDomains(t, dataset, DatasetDomainsExpected{
				expectedDomains: tt.expectedDomains,
			})
		})
	}
}
