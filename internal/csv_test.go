package internal

import (
	"os"
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
	err := LoadCSVFromReader(reader, collector, "csv")
	if err != nil {
		t.Fatalf("LoadCSVFromReader failed: %v", err)
	}

	collector.Finalise()
	timing.Finish() // for coverage

	dataset := collector.Result

	if dataset.Date.Time != testDate {
		t.Errorf("Expected date %v, got %v", testDate, dataset.Date.Time)
	}

	validateDataset(t, dataset, DatasetExpected{
		queriesCount:    14,
		domainCount:     2,
		expectedDomains: []string{"com", "org"},
		invalidDomains:  0,
		invalidRecords:  0,
	}, collector)

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
	err := LoadCSVFromReader(reader, collector, "csv")
	if err != nil {
		t.Fatalf("LoadCSVFromReader failed: %v", err)
	}

	collector.Finalise()
	dataset := collector.Result

	if dataset.Date.Time != testDate {
		t.Errorf("Expected date %v, got %v", testDate, dataset.Date.Time)
	}

	validateDataset(t, dataset, DatasetExpected{
		queriesCount:    10,
		domainCount:     2,
		expectedDomains: []string{"com", "org"},
		invalidDomains:  0,
		invalidRecords:  0,
	}, collector)

	validateDatasetExtras(t, dataset, DatasetExtrasExpected{
		expectedAllClients: []string{"192.168.1.0", "2001:db8::"},
		expectedV6Clients:  []string{"2001:db8::"},
	})

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
	err := LoadCSVFromReader(reader, collector, "csv")
	if err == nil {
		t.Error("Expected error for invalid CSV record, got nil")
	}
}

func TestLoadCSVFile_InvalidFile(t *testing.T) {
	tests := []struct {
		name        string
		content     []byte
		errorPrefix string
	}{
		{
			name:        "single byte file",
			content:     []byte("x"),
			errorPrefix: "failed to read CSV: ",
		},
		{
			name:        "three byte file",
			content:     []byte("xyz"),
			errorPrefix: "failed to parse CSV: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp("", "invalid_*.csv")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tmpFile.Name())
			defer tmpFile.Close()

			if _, err := tmpFile.Write(tt.content); err != nil {
				t.Fatalf("Failed to write to temp file: %v", err)
			}
			tmpFile.Close()

			timing := NewTimingStats()
			collector := NewCollector(DefaultDomainCount, 100000, false, nil, timing)

			err = LoadCSVFile(tmpFile.Name(), collector, "csv")
			if err == nil {
				t.Error("Expected error for invalid CSV file, got nil")
				return
			}

			if !strings.HasPrefix(err.Error(), tt.errorPrefix) {
				t.Errorf("Expected error to start with '%s', got: %v", tt.errorPrefix, err)
			}
		})
	}
}

func TestProcessCSVRecord_ErrorCases(t *testing.T) {
	tests := []struct {
		name                   string
		record                 []string
		errMsg                 string
		expectedInvalidRecords uint
	}{
		{
			name:                   "negative queries_count",
			record:                 []string{"192.168.1.1", "example.com", "-5"},
			errMsg:                 "queries_count must be non-negative",
			expectedInvalidRecords: 0,
		},
		{
			name:                   "invalid queries_count",
			record:                 []string{"192.168.1.1", "example.com", "invalid"},
			errMsg:                 "invalid queries_count",
			expectedInvalidRecords: 0,
		},
		{
			name:                   "too few fields",
			record:                 []string{"192.168.1.1"},
			errMsg:                 "CSV record must have at least two fields",
			expectedInvalidRecords: 0,
		},
		{
			name:                   "zero queries_count",
			record:                 []string{"192.168.1.1", "example.com", "0"},
			errMsg:                 "", // Should not error, just skip
			expectedInvalidRecords: 0,
		},
		{
			name:                   "invalid domain name",
			record:                 []string{"192.168.1.1", "123"},
			errMsg:                 "", // Should not error, just skip
			expectedInvalidRecords: 1,
		},
		{
			name:                   "invalid domain name (unbalanced quotes)",
			record:                 []string{"192.168.1.1", "un\"balanced"},
			errMsg:                 "", // Should not error, just skip
			expectedInvalidRecords: 1,
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
			if tt.errMsg != "" && err != nil && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("expected error message to contain '%s', got '%s'", tt.errMsg, err.Error())
			}

			if collector.invalidDomainCount != tt.expectedInvalidRecords {
				t.Errorf("Expected %d invalid domain records, got %d", tt.expectedInvalidRecords, collector.invalidDomainCount)
			}
		})
	}
}

func TestLoadCSVFromReader_CompleteDatasetVerification(t *testing.T) {
	csvData := `# Test CSV data
192.168.1.10,example.com,5
192.168.1.20,example.org,3
10.0.0.5,example.com,2
2001:db8::1,example.net,1`

	reader := strings.NewReader(csvData)
	testDate := time.Date(2007, 9, 9, 0, 0, 0, 0, time.UTC)

	timing := NewTimingStats()
	collector := NewCollector(DefaultDomainCount, 100000, true, &testDate, timing)
	err := LoadCSVFromReader(reader, collector, "csv")
	if err != nil {
		t.Fatalf("LoadCSVFromReader failed: %v", err)
	}

	collector.Finalise()
	dataset := collector.Result

	// Verify date was set correctly
	if dataset.Date.Time != testDate {
		t.Errorf("Date mismatch: expected %v, got %v", testDate, dataset.Date.Time)
	}

	validateDataset(t, dataset, DatasetExpected{
		queriesCount:    11,
		domainCount:     3,
		expectedDomains: []string{"com", "org", "net"},
		invalidDomains:  0,
		invalidRecords:  0,
	}, collector)

	validateDatasetExtras(t, dataset, DatasetExtrasExpected{
		expectedAllClients: []string{"10.0.0.0", "192.168.1.0", "2001:db8::"},
		expectedV6Clients:  []string{"2001:db8::"},
	})

	validateDatasetDomains(t, dataset, DatasetDomainsExpected{
		expectedDomains: map[DomainName]uint64{
			"com": 7, // 5 + 2
			"org": 3,
			"net": 1,
		},
	})
}

func TestLoadCSVFromReader_MixedValidInvalidRecords(t *testing.T) {
	csvData := `192.168.1.1,com,5
192.168.1.2,invalid"record,3
10.0.0.1,invalid-domain.example.123,2`

	reader := strings.NewReader(csvData)

	timing := NewTimingStats()
	collector := NewCollector(DefaultDomainCount, 100000, false, nil, timing)
	err := LoadCSVFromReader(reader, collector, "csv")
	if err != nil {
		t.Fatalf("LoadCSVFromReader failed: %v", err)
	}

	collector.Finalise()

	validateDataset(t, collector.Result, DatasetExpected{
		queriesCount:    10,
		domainCount:     1,
		expectedDomains: []string{"com"},
		invalidDomains:  2,
		invalidRecords:  0,
	}, collector)
}

func TestLoadCSVFromReader_TrailingDot(t *testing.T) {
	csvData := `192.168.1.1,com.,5`

	reader := strings.NewReader(csvData)

	timing := NewTimingStats()
	collector := NewCollector(DefaultDomainCount, 100000, false, nil, timing)
	err := LoadCSVFromReader(reader, collector, "csv")
	if err != nil {
		t.Fatalf("LoadCSVFromReader failed: %v", err)
	}

	collector.Finalise()

	validateDataset(t, collector.Result, DatasetExpected{
		queriesCount:    5,
		domainCount:     1,
		expectedDomains: []string{"com"},
		invalidDomains:  0,
		invalidRecords:  0,
	}, collector)
}

func TestLoadCSVFromReader_TestTabSeparated(t *testing.T) {
	csvData := "192.168.1.1\tcom\t5\n"

	reader := strings.NewReader(csvData)

	timing := NewTimingStats()
	collector := NewCollector(DefaultDomainCount, 100000, false, nil, timing)
	err := LoadCSVFromReader(reader, collector, "tsv")
	if err != nil {
		t.Fatalf("LoadCSVFromReader failed: %v", err)
	}

	collector.Finalise()

	validateDataset(t, collector.Result, DatasetExpected{
		queriesCount:    5,
		domainCount:     1,
		expectedDomains: []string{"com"},
		invalidDomains:  0,
		invalidRecords:  0,
	}, collector)
}

func TestUnescapeDomain(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "plain text",
			input:    "example",
			expected: "example",
		},
		{
			name:     "octal escapes",
			input:    "\\163\\145", // \163\145 -> "se"
			expected: "se",
		},
		{
			name:     "hex escapes (lowercase x)",
			input:    "\\x73\\x65", // \x73\x65 -> "se"
			expected: "se",
		},
		{
			name:     "mixed with octal producing space",
			input:    "hello\\040world", // \040 -> space
			expected: "hello world",
		},
		{
			name:     "hex followed by literal",
			input:    "\\x41B", // \x41 -> 'A' then 'B'
			expected: "AB",
		},
		{
			name:     "trailing backslash",
			input:    "\\",
			expected: "\\",
		},
		{
			name:     "bare \\x with no hex digits",
			input:    "\\x",
			expected: "x",
		},
		{
			name:     "invalid octal digit falls back to literal",
			input:    "\\8",
			expected: "8",
		},
		{
			name:     "invalid hexdigit falls back to literal",
			input:    "\\xg",
			expected: "xg",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := unescapeDomain(tc.input)
			if got != tc.expected {
				t.Errorf("unescapeDomain(%q) = %q; want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestLoadCSVFromReader_TestTabSeparatedStrangeDomain(t *testing.T) {
	csvData := "192.0.2.1\t\\042#$%'\\(\\)*+,-<>[]_~\t4\n" + "192.168.1.1\tcom.\t5\n"

	reader := strings.NewReader(csvData)

	timing := NewTimingStats()
	collector := NewCollector(DefaultDomainCount, 100000, false, nil, timing)
	err := LoadCSVFromReader(reader, collector, "tsv")
	if err != nil {
		t.Fatalf("LoadCSVFromReader failed: %v", err)
	}

	collector.Finalise()
	dataset := collector.Result

	validateDataset(t, dataset, DatasetExpected{
		queriesCount:    9,
		domainCount:     1,
		expectedDomains: []string{"com"},
		invalidDomains:  1,
		invalidRecords:  0,
	}, collector)

	validateDatasetDomains(t, dataset, DatasetDomainsExpected{
		expectedDomains: map[DomainName]uint64{
			"com": 5,
		},
	})
}
