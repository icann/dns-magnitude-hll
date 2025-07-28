package internal

import (
	"reflect"
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

	testDate := time.Date(2009, 12, 21, 0, 0, 0, 0, time.UTC)

	verbose := false
	dataset, err := LoadCSVFromReader(reader, &testDate, verbose)
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
		t.Errorf("Expected total queries count 14, got %d", dataset.AllQueriesCount)
	}

	// check that unique clients are not counted when verbose is false
	c := len(dataset.extraAllClients)
	if c != 0 {
		t.Errorf("Expected unique clients count 0, got %d", c)
	}
}

func TestLoadCSVFromReader_VerboseMode(t *testing.T) {
	csvData := `
192.168.1.1,com,8
192.168.1.2,org
2001:db8::1,org,1
192.0.2.12,net,0`

	reader := strings.NewReader(csvData)

	testDate := time.Date(2007, 9, 9, 0, 0, 0, 0, time.UTC)

	verbose := true
	dataset, err := LoadCSVFromReader(reader, &testDate, verbose)
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
	if dataset.AllQueriesCount != 10 {
		t.Errorf("Expected total queries count 10, got %d", dataset.AllQueriesCount)
	}

	// extract unique IPs from dataset to a slice for easier verification
	var uniqueIPs []string
	for ip := range dataset.extraAllClients {
		uniqueIPs = append(uniqueIPs, ip.String())
	}

	expectedIPs := []string{"192.168.1.0", "2001:db8::"}

	if !reflect.DeepEqual(uniqueIPs, expectedIPs) {
		t.Errorf("Expected unique IPs %v, got %v", expectedIPs, uniqueIPs)
	}

	// Verify "net" domain is not counted
	if _, exists := dataset.Domains[DomainName("net")]; exists {
		t.Error("Expected 'net' domain not to be counted, with query count 0")
	}
}

func TestLoadCSVFromReader_InvalidRecord(t *testing.T) {
	csvData := `invalid`

	reader := strings.NewReader(csvData)

	_, err := LoadCSVFromReader(reader, nil, false)
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
			errMsg: "", // Should not error, just skip
		},
		{
			name:   "invalid domain name (unbalanced quotes)",
			record: []string{"192.168.1.1", "un\"balanced"},
			errMsg: "", // Should not error, just skip
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataset := newDataset()
			err := processCSVRecord(&dataset, tt.record, false)

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
