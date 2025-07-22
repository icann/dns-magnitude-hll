package internal

import (
	"strings"
	"testing"
	"time"
)

func init() {
	InitStats()
}

func TestLoadCSVFromReader(t *testing.T) {
	csvData := `# client,domain,queries_count
192.168.1.1,example.com
192.168.1.1,example.org,12,
2001:db8::1,iana.org,1
192.0.2.12,test.se,0`

	reader := strings.NewReader(csvData)

	testDate := time.Date(2009, 12, 31, 0, 0, 0, 0, time.UTC)

	dataset, err := LoadCSVFromReader(reader, &testDate)
	if err != nil {
		t.Fatalf("LoadCSVFromReader failed: %v", err)
	}

	if dataset.Date.Time != testDate {
		t.Errorf("Expected date %v, got %v", testDate, dataset.Date.Time)
	}

	// check the TLDs are counted
	expectedDomains := []string{"com", "org"}
	for _, domain := range expectedDomains {
		if _, exists := dataset.Domains[DomainName(domain)]; !exists {
			t.Errorf("Expected domain %s not found in dataset", domain)
		}
	}

	// check total queries count
	if dataset.AllQueriesCount != 14 {
		t.Errorf("Expected total queries count 13, got %d", dataset.AllQueriesCount)
	}
}

func TestLoadCSVFromReader_InvalidRecord(t *testing.T) {
	csvData := `invalid`

	reader := strings.NewReader(csvData)

	_, err := LoadCSVFromReader(reader, nil)
	if err == nil {
		t.Error("Expected error for invalid CSV record, got nil")
	}
}

func TestProcessCSVRecord_ErrorCases(t *testing.T) {
	tests := []struct {
		name   string
		record []string
		errMsg string
	}{
		{
			name:   "negative queries_count",
			record: []string{"192.168.1.1", "example.com", "-5"},
			errMsg: "queries_count must be non-negative",
		},
		{
			name:   "invalid queries_count",
			record: []string{"192.168.1.1", "example.com", "invalid"},
			errMsg: "invalid queries_count",
		},
		{
			name:   "too few fields",
			record: []string{"192.168.1.1"},
			errMsg: "CSV record must have at least two fields",
		},
		{
			name:   "zero queries_count",
			record: []string{"192.168.1.1", "example.com", "0"},
			errMsg: "", // Should not error, just skip
		},
		{
			name:   "invalid domain name",
			record: []string{"192.168.1.1", "123"},
			errMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataset := newDataset()
			err := processCSVRecord(&dataset, tt.record)

			if dataset.AllQueriesCount != 0 {
				t.Errorf("Expected no counted queries for error case, got %d", dataset.AllQueriesCount)
			}

			if tt.errMsg != "" && err == nil {
				t.Errorf("expected error but got none")
			}
			if tt.errMsg == "" && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if tt.errMsg != "" && err != nil && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("expected error message to contain '%s', got '%s'", tt.errMsg, err.Error())
			}
		})
	}
}
