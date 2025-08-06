package internal

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/go-hll"
)

func init() {
	InitStats()
}

func TestMagnitudeDataset_SortedByMagnitude(t *testing.T) {
	tests := []struct {
		name            string
		allClientsCount uint64
		domains         []struct {
			name         DomainName
			clientsCount uint64
		}
		expected []DomainMagnitude
	}{
		{
			name:            "basic magnitude calculation",
			allClientsCount: 1000,
			domains: []struct {
				name         DomainName
				clientsCount uint64
			}{
				{"a.example.org", 100},
				{"b.example.org", 10},
				{"c.example.org", 1},
			},
			expected: []DomainMagnitude{
				{Domain: "c.example.org", Magnitude: 0.0},                                   // log(1)/log(1000) * 10 = 0
				{Domain: "b.example.org", Magnitude: (math.Log(10) / math.Log(1000)) * 10},  // ≈ 3.33
				{Domain: "a.example.org", Magnitude: (math.Log(100) / math.Log(1000)) * 10}, // ≈ 6.67
			},
		},
		{
			name:            "equal client counts",
			allClientsCount: 100,
			domains: []struct {
				name         DomainName
				clientsCount uint64
			}{
				{"a.example.org", 50},
				{"b.example.org", 50},
				{"c.example.org", 25},
			},
			expected: []DomainMagnitude{
				{Domain: "c.example.org", Magnitude: (math.Log(25) / math.Log(100)) * 10}, // ≈ 7.19
				{Domain: "a.example.org", Magnitude: (math.Log(50) / math.Log(100)) * 10}, // ≈ 8.50
				{Domain: "b.example.org", Magnitude: (math.Log(50) / math.Log(100)) * 10}, // ≈ 8.50
			},
		},
		{
			name:            "single domain",
			allClientsCount: 42,
			domains: []struct {
				name         DomainName
				clientsCount uint64
			}{
				{"only.example.org", 21},
			},
			expected: []DomainMagnitude{
				{Domain: "only.example.org", Magnitude: (math.Log(21) / math.Log(42)) * 10}, // ≈ 8.16
			},
		},
		{
			name:            "maximum magnitude case",
			allClientsCount: 1000,
			domains: []struct {
				name         DomainName
				clientsCount uint64
			}{
				{"all.example.org", 1000},
				{"half.example.org", 500},
			},
			expected: []DomainMagnitude{
				{Domain: "half.example.org", Magnitude: (math.Log(500) / math.Log(1000)) * 10}, // ≈ 9.03
				{Domain: "all.example.org", Magnitude: 10.0},                                   // log(1000)/log(1000) * 10 = 10
			},
		},
		{
			name:            "two domains with same magnitude",
			allClientsCount: 100,
			domains: []struct {
				name         DomainName
				clientsCount uint64
			}{
				{"z.example.org", 25},
				{"a.example.org", 25},
			},
			expected: []DomainMagnitude{
				{Domain: "a.example.org", Magnitude: (math.Log(25) / math.Log(100)) * 10}, // ≈ 7.19
				{Domain: "z.example.org", Magnitude: (math.Log(25) / math.Log(100)) * 10}, // ≈ 7.19
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock dataset
			dataset := newDataset(nil)
			dataset.AllClientsCount = tt.allClientsCount

			// Create mock domains with specified client counts
			for _, domain := range tt.domains {
				dh := newDomain(domain.name)
				dh.ClientsCount = domain.clientsCount
				dataset.Domains[domain.name] = dh
			}

			// Sort domains by magnitude
			result := dataset.SortedByMagnitude()

			// Verify we have the expected number of results
			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d domains, got %d", len(tt.expected), len(result))
				return
			}

			// Verify the actual order and magnitudes match expected
			for i, expected := range tt.expected {
				this := result[i]

				tolerance := 0.0

				// Check domain name
				if this.Domain != expected.Domain ||
					math.Abs(this.Magnitude-expected.Magnitude) > tolerance {
					t.Errorf("Result position %d, got %s(%.2f), expected %s(%.2f)",
						i, this.Domain, this.Magnitude, expected.Domain, expected.Magnitude)
					continue
				}
			}
		})
	}
}

func TestMagnitudeDataset_SortedByMagnitude_EmptyDataset(t *testing.T) {
	dataset := newDataset(nil)
	dataset.AllClientsCount = 100

	sorted := dataset.SortedByMagnitude()

	if len(sorted) != 0 {
		t.Errorf("Expected empty slice for dataset with no domains, got %d items", len(sorted))
	}
}

func TestMagnitudeDataset_Truncate(t *testing.T) {
	tests := []struct {
		name          string
		totalDomains  uint
		truncateLimit int
		expectedCount uint
	}{
		{
			name:          "truncate 10 domains to 5",
			totalDomains:  10,
			truncateLimit: 5,
			expectedCount: 5,
		},
		{
			name:          "truncate 5 domains to 10 (no change)",
			totalDomains:  5,
			truncateLimit: 10,
			expectedCount: 5,
		},
		{
			name:          "truncate 8 domains to 3",
			totalDomains:  8,
			truncateLimit: 3,
			expectedCount: 3,
		},
		{
			name:          "truncate 1 domain to 0 (no change)",
			totalDomains:  1,
			truncateLimit: 0,
			expectedCount: 1,
		},
		{
			name:          "truncate 0 domains to 5 (no change)",
			totalDomains:  0,
			truncateLimit: 5,
			expectedCount: 0,
		},
		{
			name:          "truncate with negative limit (no change)",
			totalDomains:  3,
			truncateLimit: -1,
			expectedCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create dataset with specified number of domains
			dataset := newDataset(nil)
			dataset.AllClientsCount = 1000

			var expectedRemaining []DomainName

			// Create domains with decreasing client counts for predictable magnitudes
			// Domain 0 gets highest client count (totalDomains), domain 1 gets (totalDomains-1), etc.
			// This way, lower numbered domains have higher magnitude and will be kept after truncation
			for i := uint(0); i < tt.totalDomains; i++ {
				domainName := DomainName(fmt.Sprintf("%d.example.org", i))
				dh := newDomain(domainName)
				// Give each domain a decreasing client count (n, n-1, n-2, ..., 1)
				// This creates predictable magnitudes where lower numbers = higher magnitude
				dh.ClientsCount = uint64(tt.totalDomains - i)
				dataset.Domains[domainName] = dh

				// add to expected remaining domains
				if i < tt.expectedCount {
					expectedRemaining = append(expectedRemaining, domainName)
				}
			}

			// Truncate the dataset
			dataset.Truncate(tt.truncateLimit)

			// Verify the number of domains after truncation
			if uint(len(dataset.Domains)) != tt.expectedCount {
				t.Errorf("Expected %d domains after truncation, got %d", tt.expectedCount, len(dataset.Domains))
				return
			}

			// Verify that the expected domains remain (those with highest magnitude)
			for _, expectedDomain := range expectedRemaining {
				if _, exists := dataset.Domains[expectedDomain]; !exists {
					t.Errorf("Expected domain %s to remain after truncation, but it was removed", expectedDomain)
				}
			}
		})
	}
}

func TestMagnitudeDataset_UpdateStats_ZeroQueryCount(t *testing.T) {
	dataset := newDataset(nil)
	testIP, err := NewIPAddressFromString("192.168.1.1")
	if err != nil {
		t.Fatalf("newIPAddressFromString failed: %v", err)
	}
	testDomain := DomainName("org")

	// Call updateStats with zero query count - should result in no change
	dataset.updateStats(testDomain, testIP, 0, true)

	// Verify no change
	if dataset.AllQueriesCount != 0 {
		t.Errorf("Expected AllQueriesCount to remain 0, got %d", dataset.AllQueriesCount)
	}

	if len(dataset.Domains) != 0 {
		t.Errorf("Expected domain count to remain 0, got %d", len(dataset.Domains))
	}

	// Now test with non-zero query count to ensure updateStats works normally
	dataset.updateStats(testDomain, testIP, 1999, true)

	if dataset.AllQueriesCount != 1999 {
		t.Errorf("Expected AllQueriesCount to be 1999, got %d", dataset.AllQueriesCount)
	}

	if _, exists := dataset.Domains[testDomain]; !exists {
		t.Errorf("Expected domain %s to be added with non-zero query count", testDomain)
	}
}

func TestAggregateDatasets_ValidationErrors(t *testing.T) {
	// Create base datasets for testing
	date1 := time.Date(2007, 9, 9, 0, 0, 0, 0, time.UTC)
	date2 := time.Date(2009, 12, 21, 0, 0, 0, 0, time.UTC)

	createDataset := func(version uint16, date time.Time, filename string) MagnitudeDataset {
		dataset := newDataset(&date)
		dataset.Version = version
		dataset.extraSourceFilename = filename

		// Add a simple domain for testing
		domain := newDomain("test.example.org")
		domain.ClientsCount = 10
		domain.QueriesCount = 100
		dataset.Domains["test.example.org"] = domain
		dataset.AllQueriesCount = 100
		dataset.AllClientsCount = 10

		return dataset
	}

	tests := []struct {
		name        string
		datasets    []MagnitudeDataset
		expectError bool
		errorMsg    string
	}{
		{
			name: "matching versions and dates - should succeed",
			datasets: []MagnitudeDataset{
				createDataset(1, date1, "file1.dnsmag"),
				createDataset(1, date1, "file2.dnsmag"),
			},
			expectError: false,
		},
		{
			name: "version mismatch - should fail",
			datasets: []MagnitudeDataset{
				createDataset(1, date1, "file1.dnsmag"),
				createDataset(2, date1, "file2.dnsmag"),
			},
			expectError: true,
			errorMsg:    "version mismatch: dataset file2.dnsmag has version 2, expected 1",
		},
		{
			name: "date mismatch - should fail",
			datasets: []MagnitudeDataset{
				createDataset(1, date1, "file1.dnsmag"),
				createDataset(1, date2, "file2.dnsmag"),
			},
			expectError: true,
			errorMsg:    "date mismatch: dataset file2.dnsmag has date 2009-12-21, expected 2007-09-09",
		},
		{
			name: "multiple datasets with version mismatch - should fail on first mismatch",
			datasets: []MagnitudeDataset{
				createDataset(1, date1, "file1.dnsmag"),
				createDataset(1, date1, "file2.dnsmag"),
				createDataset(2, date1, "file3.dnsmag"),
			},
			expectError: true,
			errorMsg:    "version mismatch: dataset file3.dnsmag has version 2, expected 1",
		},
		{
			name: "multiple datasets with date mismatch - should fail on first mismatch",
			datasets: []MagnitudeDataset{
				createDataset(1, date1, "file1.dnsmag"),
				createDataset(1, date1, "file2.dnsmag"),
				createDataset(1, date2, "file3.dnsmag"),
			},
			expectError: true,
			errorMsg:    "date mismatch: dataset file3.dnsmag has date 2009-12-21, expected 2007-09-09",
		},
		{
			name: "single dataset - should fail",
			datasets: []MagnitudeDataset{
				createDataset(1, date1, "file1.dnsmag"),
			},
			expectError: true,
			errorMsg:    "no datasets to aggregate",
		},
		{
			name:        "empty datasets - should fail",
			datasets:    []MagnitudeDataset{},
			expectError: true,
			errorMsg:    "no datasets to aggregate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AggregateDatasets(tt.datasets)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("Expected error message '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %v", err)
					return
				}

				// Verify successful aggregation
				if len(result.Domains) != 1 {
					t.Errorf("Expected aggregated result to have 1 domain, got %d", len(result.Domains))
				}

				expectedCount := uint64(100) * uint64(len(tt.datasets))
				if result.AllQueriesCount != expectedCount {
					t.Errorf("Expected aggregated result to have query count %d, got %d", expectedCount, result.AllQueriesCount)
				}
			}
		})
	}
}

func TestAggregateDatasets_Success(t *testing.T) {
	date := time.Date(1999, 8, 21, 0, 0, 0, 0, time.UTC)

	// Create first dataset with domains A, B, C
	dataset1 := newDataset(&date)
	dataset1.Version = 1
	dataset1.extraSourceFilename = "file1.dnsmag"
	dataset1.AllQueriesCount = 300
	dataset1.AllClientsCount = 30

	domainA := newDomain("a.example.org")
	domainA.ClientsCount = 10
	domainA.QueriesCount = 100
	dataset1.Domains["a.example.org"] = domainA

	domainB := newDomain("b.example.org")
	domainB.ClientsCount = 15
	domainB.QueriesCount = 150
	dataset1.Domains["b.example.org"] = domainB

	domainC := newDomain("c.example.org")
	domainC.ClientsCount = 5
	domainC.QueriesCount = 50
	dataset1.Domains["c.example.org"] = domainC

	// Create second dataset with domains B, C, D (overlapping with first)
	dataset2 := newDataset(&date)
	dataset2.Version = 1
	dataset2.extraSourceFilename = "file2.dnsmag"
	dataset2.AllQueriesCount = 250
	dataset2.AllClientsCount = 25

	domainB2 := newDomain("b.example.org")
	domainB2.ClientsCount = 8
	domainB2.QueriesCount = 80
	dataset2.Domains["b.example.org"] = domainB2

	domainC2 := newDomain("c.example.org")
	domainC2.ClientsCount = 12
	domainC2.QueriesCount = 120
	dataset2.Domains["c.example.org"] = domainC2

	domainD := newDomain("d.example.org")
	domainD.ClientsCount = 5
	domainD.QueriesCount = 50
	dataset2.Domains["d.example.org"] = domainD

	expectedDomainAQueries := domainA.QueriesCount
	expectedDomainBQueries := domainB.QueriesCount + domainB2.QueriesCount
	expectedDomainCQueries := domainC.QueriesCount + domainC2.QueriesCount
	expectedDomainDQueries := domainD.QueriesCount

	// Aggregate the datasets
	result, err := AggregateDatasets([]MagnitudeDataset{dataset1, dataset2})
	if err != nil {
		t.Fatalf("AggregateDatasets failed: %v", err)
	}

	// Verify aggregated totals
	expectedTotalQueries := dataset1.AllQueriesCount + dataset2.AllQueriesCount
	if result.AllQueriesCount != expectedTotalQueries {
		t.Errorf("Expected total queries %d, got %d", expectedTotalQueries, result.AllQueriesCount)
	}

	// Verify we have all unique domains (A, B, C, D)
	expectedDomains := []DomainName{"a.example.org", "b.example.org", "c.example.org", "d.example.org"}
	if len(result.Domains) != len(expectedDomains) {
		t.Errorf("Expected %d domains, got %d", len(expectedDomains), len(result.Domains))
	}

	// Verify each domain exists and has correct aggregated values
	for _, domainName := range expectedDomains {
		domain, exists := result.Domains[domainName]
		if !exists {
			t.Errorf("Expected domain %s to exist in aggregated result", domainName)
			continue
		}

		// Check aggregated query counts
		switch domainName {
		case "a.example.org":
			if domain.QueriesCount != expectedDomainAQueries {
				t.Errorf("Domain %s: expected queries %d, got %d", domainName, expectedDomainAQueries, domain.QueriesCount)
			}
		case "b.example.org":
			if domain.QueriesCount != expectedDomainBQueries {
				t.Errorf("Domain %s: expected queries %d, got %d", domainName, expectedDomainBQueries, domain.QueriesCount)
			}
		case "c.example.org":
			if domain.QueriesCount != expectedDomainCQueries {
				t.Errorf("Domain %s: expected queries %d, got %d", domainName, expectedDomainCQueries, domain.QueriesCount)
			}
		case "d.example.org":
			if domain.QueriesCount != expectedDomainDQueries {
				t.Errorf("Domain %s: expected queries %d, got %d", domainName, expectedDomainDQueries, domain.QueriesCount)
			}
		}
	}

	// Verify date and version are preserved
	if result.Date.Time != date {
		t.Errorf("Expected date %v, got %v", date, result.Date.Time)
	}

	if result.Version != 1 {
		t.Errorf("Expected version 1, got %d", result.Version)
	}
}

func TestAggregateDatasets_HLLUnionErrors(t *testing.T) {
	// Initialize HLL with default settings
	err := InitStats()
	if err != nil {
		t.Fatalf("Failed to initialize stats: %v", err)
	}

	tests := []struct {
		name             string
		allClientsHll    bool
		expectedErrorMsg string
	}{
		{
			name:             "all clients HLL union error",
			allClientsHll:    true,
			expectedErrorMsg: "failed to union all clients HLL",
		},
		{
			name:             "domain HLL union error",
			allClientsHll:    false,
			expectedErrorMsg: "failed to union HLL for domain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataset1 := newDataset(nil)
			domain := DomainName("example.com")
			dataset1.Domains[domain] = newDomain(domain)

			dataset2 := newDataset(nil)
			domainData := newDomain(domain)
			dataset2.Domains[domain] = domainData

			settings := dataset1.AllClientsHll.Settings()
			settings.Regwidth = settings.Regwidth + 1

			incompatibleHLL, err := hll.NewHll(settings)
			if err != nil {
				t.Fatalf("Failed to create incompatible HLL: %v", err)
			}

			if tt.allClientsHll {
				dataset2.AllClientsHll = &HLLWrapper{Hll: &incompatibleHLL}
			} else {
				domainData.Hll = &HLLWrapper{Hll: &incompatibleHLL}
				dataset2.Domains[domain] = domainData
			}

			datasets := []MagnitudeDataset{dataset1, dataset2}

			// Attempt to aggregate - should fail due to incompatible HLL settings
			_, err = AggregateDatasets(datasets)
			if err == nil {
				t.Error("Expected error when aggregating datasets with incompatible HLLs, but got nil")
			}
			if err != nil && !strings.Contains(err.Error(), tt.expectedErrorMsg) {
				t.Errorf("Expected error message to contain '%s', got: %v", tt.expectedErrorMsg, err)
			}
		})
	}
}
