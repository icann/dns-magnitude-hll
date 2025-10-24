package internal

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

type DatasetExpected struct {
	queriesCount    uint64
	domainCount     int
	expectedDomains []string
	invalidDomains  uint
	invalidRecords  uint
}

type DatasetExtrasExpected struct {
	expectedAllClients []string
	expectedV6Clients  []string
}

type DatasetDomainsExpected struct {
	expectedDomains map[DomainName]uint64
}

// validateDataset is a helper function to validate dataset properties
func validateDataset(t *testing.T, dataset MagnitudeDataset, expected DatasetExpected, collector *Collector,
) {
	t.Helper()

	if dataset.AllQueriesCount != expected.queriesCount {
		t.Errorf("Expected %d queries, got %d", expected.queriesCount, dataset.AllQueriesCount)
	}

	if len(dataset.Domains) != expected.domainCount {
		t.Errorf("Expected %d domains, got %d", expected.domainCount, len(dataset.Domains))
	}

	for _, domain := range expected.expectedDomains {
		if _, exists := dataset.Domains[DomainName(domain)]; !exists {
			t.Errorf("Expected domain %s not found in dataset", domain)
		}
	}

	if collector != nil {
		if collector.invalidDomainCount != expected.invalidDomains {
			t.Errorf("Expected %d invalid domains, got %d", expected.invalidDomains, collector.invalidDomainCount)
		}

		if collector.invalidRecordCount != expected.invalidRecords {
			t.Errorf("Expected %d invalid records, got %d", expected.invalidRecords, collector.invalidRecordCount)
		}
	}
}

// validateDatasetExtras is a helper function to validate extra dataset properties in verbose mode
func validateDatasetExtras(t *testing.T, dataset MagnitudeDataset, expected DatasetExtrasExpected) {
	t.Helper()

	var actualAllClients []string
	for ip := range dataset.extraAllClients {
		actualAllClients = append(actualAllClients, ip.String())
	}
	slices.Sort(actualAllClients)

	if !reflect.DeepEqual(actualAllClients, expected.expectedAllClients) {
		t.Errorf("Expected all clients %v, got %v", expected.expectedAllClients, actualAllClients)
	}

	var actualV6Clients []string
	for ip := range dataset.extraV6Clients {
		actualV6Clients = append(actualV6Clients, ip.String())
	}
	slices.Sort(actualV6Clients)

	if !reflect.DeepEqual(actualV6Clients, expected.expectedV6Clients) {
		t.Errorf("Expected IPv6 clients %v, got %v", expected.expectedV6Clients, actualV6Clients)
	}
}

// validateDatasetDomains is a helper function to validate specific domain counts in datasets
func validateDatasetDomains(t *testing.T, dataset MagnitudeDataset, expected DatasetDomainsExpected) {
	t.Helper()

	if len(dataset.Domains) != len(expected.expectedDomains) {
		t.Errorf("Expected %d domains, got %d", len(expected.expectedDomains), len(dataset.Domains))
	}

	for expectedDomain, expectedQueries := range expected.expectedDomains {
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
		if _, expected := expected.expectedDomains[actualDomain]; !expected {
			t.Errorf("Unexpected domain found: %s with %d queries",
				actualDomain, dataset.Domains[actualDomain].QueriesCount)
		}
	}
}

// loadDatasetFromCSV creates a dataset from CSV data string for testing
// Returns the Collector for access to verbose stats and error counts.
func loadDatasetFromCSV(csvData string, dateStr string, verbose bool) (*Collector, error) {
	var date *time.Time
	if dateStr != "" {
		parsedDate, err := time.Parse(time.DateOnly, dateStr)
		if err != nil {
			return nil, err
		}
		date = &parsedDate
	}

	timing := NewTimingStats()
	collector := NewCollector(DefaultDomainCount, 0, verbose, date, timing)
	reader := strings.NewReader(csvData)

	timing.StartParsing()

	err := LoadCSVFromReader(reader, collector, "csv")
	if err != nil {
		return nil, err
	}

	err = collector.Finalise()
	if err != nil {
		return nil, err
	}

	timing.Finish()

	return collector, nil
}
