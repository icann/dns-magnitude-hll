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
		name                string
		pcapFile            string
		expectedQueryCount  uint64
		expectedClientCount uint64
		expectedDomains     map[DomainName]uint64 // domain -> query count
		expectError         bool
	}{
		{
			name:                "test1.pcap validation",
			pcapFile:            "../testdata/test1.pcap.gz",
			expectedQueryCount:  100,
			expectedClientCount: 58,
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

			// Validate total query count
			if dataset.AllQueriesCount != tt.expectedQueryCount {
				t.Errorf("Expected total queries %d, got %d", tt.expectedQueryCount, dataset.AllQueriesCount)
			}

			// Validate client count (require exact match for test data)
			if dataset.AllClientsCount != tt.expectedClientCount {
				t.Errorf("Expected client count %d, got %d", tt.expectedClientCount, dataset.AllClientsCount)
			}

			// Validate domain counts
			if len(dataset.Domains) != len(tt.expectedDomains) {
				t.Errorf("Expected %d domains, got %d", len(tt.expectedDomains), len(dataset.Domains))
			}

			for expectedDomain, expectedQueries := range tt.expectedDomains {
				domain, exists := dataset.Domains[expectedDomain]
				if !exists {
					t.Errorf("Expected domain %s not found in results", expectedDomain)
					continue
				}

				if domain.QueriesCount != expectedQueries {
					t.Errorf("Domain %s: expected %d queries, got %d",
						expectedDomain, expectedQueries, domain.QueriesCount)
				}
			}

			// Verify no unexpected domains
			for actualDomain := range dataset.Domains {
				if _, expected := tt.expectedDomains[actualDomain]; !expected {
					t.Errorf("Unexpected domain found: %s with %d queries",
						actualDomain, dataset.Domains[actualDomain].QueriesCount)
				}
			}
		})
	}
}
