package internal

import (
	"net/netip"
	"reflect"
	"slices"
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
	timing := NewTimingStats()
	collector := NewCollector(DefaultDomainCount, 100000, verbose, &testDate, timing)
	err := LoadCSVFromReader(reader, collector)
	if err != nil {
		t.Fatalf("LoadCSVFromReader failed: %v", err)
	}

	collector.finalise()
	timing.Finish() // for coverage

	dataset := collector.Result

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
	timing := NewTimingStats()
	collector := NewCollector(DefaultDomainCount, 100000, verbose, &testDate, timing)
	err := LoadCSVFromReader(reader, collector)
	if err != nil {
		t.Fatalf("LoadCSVFromReader failed: %v", err)
	}

	collector.finalise()
	dataset := collector.Result

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
	slices.Sort(uniqueIPs)

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

	timing := NewTimingStats()
	collector := NewCollector(DefaultDomainCount, 100000, false, nil, timing)
	err := LoadCSVFromReader(reader, collector)
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
			timing := NewTimingStats()
			collector := NewCollector(DefaultDomainCount, 100000, false, nil, timing)
			err := processCSVRecord(collector, tt.record)

			dataset := collector.Result
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

// buildExpectedDataset creates a test dataset with the given configuration
func buildExpectedDataset(date time.Time, totalQueries uint64, domains []struct {
	domain     DomainName
	queryCount uint64
	clientIPs  []string
}, allClientIPs []string, v6ClientIPs []string,
) MagnitudeDataset {
	expected := newDataset()
	expected.Date = &TimeWrapper{Time: date}
	expected.AllQueriesCount = totalQueries

	// Initialize domains
	for _, ed := range domains {
		domain := newDomain(ed.domain)
		domain.QueriesCount = ed.queryCount
		domain.extraAllClients = make(map[netip.Addr]struct{})
		for _, ip := range ed.clientIPs {
			domain.extraAllClients[newIPAddressFromString(ip).truncatedIP] = struct{}{}
		}
		expected.Domains[ed.domain] = domain
		expected.extraAllDomains[ed.domain] = struct{}{}
	}

	// Initialize global client IPs
	expected.extraAllClients = make(map[netip.Addr]struct{})
	for _, ip := range allClientIPs {
		expected.extraAllClients[newIPAddressFromString(ip).truncatedIP] = struct{}{}
	}

	expected.extraV6Clients = make(map[netip.Addr]struct{})
	for _, ip := range v6ClientIPs {
		expected.extraV6Clients[newIPAddressFromString(ip).truncatedIP] = struct{}{}
	}

	return expected
}

func TestLoadCSVFromReader_CompleteDatasetVerification(t *testing.T) {
	csvData := `# Test CSV data
192.168.1.10,example.com,5
192.168.1.20,example.org,3
10.0.0.5,example.com,2
2001:db8::1,example.net,1`

	reader := strings.NewReader(csvData)
	testDate := time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)

	timing := NewTimingStats()
	collector := NewCollector(DefaultDomainCount, 100000, true, &testDate, timing)
	err := LoadCSVFromReader(reader, collector)
	if err != nil {
		t.Fatalf("LoadCSVFromReader failed: %v", err)
	}

	collector.finalise()
	dataset := collector.Result

	// Build expected dataset using helper function
	expectedDomains := []struct {
		domain     DomainName
		queryCount uint64
		clientIPs  []string
	}{
		{
			domain:     DomainName("com"),
			queryCount: 7,
			clientIPs:  []string{"192.168.1.10", "10.0.0.5"},
		},
		{
			domain:     DomainName("org"),
			queryCount: 3,
			clientIPs:  []string{"192.168.1.20"},
		},
		{
			domain:     DomainName("net"),
			queryCount: 1,
			clientIPs:  []string{"2001:db8::1"},
		},
	}

	expected := buildExpectedDataset(
		testDate,
		11,
		expectedDomains,
		[]string{"192.168.1.10", "10.0.0.5", "2001:db8::1"},
		[]string{"2001:db8::1"},
	)

	// Compare key fields (skip HLL data and computed values)
	if dataset.Date.Time != expected.Date.Time {
		t.Errorf("Date mismatch: expected %v, got %v", expected.Date.Time, dataset.Date.Time)
	}

	if dataset.AllQueriesCount != expected.AllQueriesCount {
		t.Errorf("AllQueriesCount mismatch: expected %d, got %d", expected.AllQueriesCount, dataset.AllQueriesCount)
	}

	if len(dataset.Domains) != len(expected.Domains) {
		t.Errorf("Domain count mismatch: expected %d, got %d", len(expected.Domains), len(dataset.Domains))
	}

	// Compare domain queries counts
	for domain, expectedDomain := range expected.Domains {
		actualDomain, exists := dataset.Domains[domain]
		if !exists {
			t.Errorf("Expected domain %s not found", domain)
			continue
		}
		if actualDomain.QueriesCount != expectedDomain.QueriesCount {
			t.Errorf("Domain %s queries count mismatch: expected %d, got %d",
				domain, expectedDomain.QueriesCount, actualDomain.QueriesCount)
		}
	}

	// Compare client IP sets
	if !reflect.DeepEqual(dataset.extraAllClients, expected.extraAllClients) {
		t.Errorf("extraAllClients mismatch: expected %v, got %v",
			expected.extraAllClients, dataset.extraAllClients)
	}

	if !reflect.DeepEqual(dataset.extraV6Clients, expected.extraV6Clients) {
		t.Errorf("extraV6Clients mismatch: expected %v, got %v",
			expected.extraV6Clients, dataset.extraV6Clients)
	}
}
