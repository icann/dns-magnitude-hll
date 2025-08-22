package internal

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
)

func init() {
	InitStats()
}

func TestWriteAndLoadDNSMagFile_WriteLoadCycle(t *testing.T) {
	// Create test dataset using CSV data
	csvData := `192.168.1.10,example.com,5
192.168.1.20,example.org,3
10.0.0.5,com.,2`

	collector, err := loadDatasetFromCSV(csvData, "1999-08-21", false)
	if err != nil {
		t.Fatalf("loadDatasetFromCSV failed: %v", err)
	}
	originalDataset := collector.Result

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "test_*.dnsmag")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Test writing
	filename, err := WriteDNSMagFile(originalDataset, tmpFile.Name())
	if err != nil {
		t.Fatalf("WriteDNSMagFile failed: %v", err)
	}

	if filename != tmpFile.Name() {
		t.Errorf("Expected filename %s, got %s", tmpFile.Name(), filename)
	}

	// Test loading
	seq := NewDatasetSequence(0, nil)
	err = seq.LoadDNSMagFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("LoadDNSMagFile failed: %v", err)
	}

	// Validate loaded dataset
	validateDataset(t, seq.Result, DatasetExpected{
		queriesCount:    10,
		domainCount:     2,
		expectedDomains: []string{"com", "org"},
		invalidDomains:  0,
		invalidRecords:  0,
	}, nil)

	validateDatasetDomains(t, seq.Result, DatasetDomainsExpected{
		expectedDomains: map[DomainName]uint64{
			"com": 7,
			"org": 3,
		},
	})

	// Verify date was preserved
	if !seq.Result.Date.Equal(originalDataset.Date.Time) {
		t.Errorf("Date mismatch: expected %v, got %v", originalDataset.Date.Time, seq.Result.Date.Time)
	}

	// Verify source filename was set (and a dataset sequence number was added)
	expectedFilename := fmt.Sprintf("%s#1", tmpFile.Name())
	if seq.Result.extraSourceFilename != expectedFilename {
		t.Errorf("Expected source filename %s, got %s", expectedFilename, seq.Result.extraSourceFilename)
	}
}

func TestWriteDNSMagFile_CreateError(t *testing.T) {
	// Try to write to invalid path
	dataset := newDataset(nil)
	_, err := WriteDNSMagFile(dataset, "/invalid/path/file.dnsmag")
	if err == nil {
		t.Error("Expected error when writing to invalid path, got nil")
	}
}

func TestLoadDNSMagFile_FileNotFound(t *testing.T) {
	seq := NewDatasetSequence(0, nil)
	err := seq.LoadDNSMagFile("non-existent.dnsmag")
	if err == nil {
		t.Error("Expected error when loading non-existent file, got nil")
	}
}

func TestLoadDNSMagFile_InvalidFormat(t *testing.T) {
	// Create file with invalid content
	tmpFile, err := os.CreateTemp("", "invalid_*.dnsmag")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write invalid data
	if _, err := tmpFile.WriteString("invalid cbor data"); err != nil {
		t.Fatalf("Failed to write invalid data: %v", err)
	}
	tmpFile.Close()

	seq := NewDatasetSequence(0, nil)
	err = seq.LoadDNSMagFile(tmpFile.Name())
	if err == nil {
		t.Error("Expected error when loading invalid CBOR file, got nil")
	}
}

func TestTimeWrapper_TimeWrapper_MarshalUnmarshal(t *testing.T) {
	// Make sure we use a date-only CBOR encoding, discarding the time component
	date := time.Date(2007, 9, 9, 1, 2, 3, 4, time.UTC)
	wrapper := TimeWrapper{Time: date}

	data, err := wrapper.MarshalCBOR()
	if err != nil {
		t.Fatalf("Marshal TimeWrapper failed: %v", err)
	}

	var unmarshaled TimeWrapper
	err = unmarshaled.UnmarshalCBOR(data)
	if err != nil {
		t.Fatalf("Unmarshal TimeWrapper failed: %v", err)
	}

	// Verify time was preserved (only date part)
	expectedDate := time.Date(2007, 9, 9, 0, 0, 0, 0, time.UTC)
	if !unmarshaled.Equal(expectedDate) {
		t.Errorf("Time mismatch: expected %v, got %v", expectedDate, unmarshaled.Time)
	}
}

func TestTimeWrapper_TimeWrapper_UnmarshalCBOR_InvalidDate(t *testing.T) {
	var wrapper TimeWrapper

	// Test with invalid CBOR data
	err := wrapper.UnmarshalCBOR([]byte("invalid"))
	if err == nil {
		t.Error("Expected error when unmarshaling invalid CBOR data, got nil")
	}
}

func TestTimeWrapper_TimeWrapper_UnmarshalCBOR_DirectTimeEncoding(t *testing.T) {
	// Create a time.Time and encode it directly with CBOR
	testTime := time.Date(2008, 8, 21, 0, 0, 0, 0, time.UTC)

	// This would be how time.Time is normally encoded in CBOR
	data, err := cbor.Marshal(testTime)
	if err != nil {
		t.Fatalf("Failed to marshal time.Time: %v", err)
	}

	// Try to unmarshal it with TimeWrapper - should fail because TimeWrapper expects tag 1004
	var wrapper TimeWrapper
	err = wrapper.UnmarshalCBOR(data)
	if err == nil {
		t.Error("Expected error when unmarshaling direct time.Time encoding with TimeWrapper, got nil")
	}
}

func TestTimeWrapper_TimeWrapper_UnmarshalCBOR_WrongTag(t *testing.T) {
	// Create a CBOR tag with wrong tag number but correct content format
	wrongTag := cbor.Tag{Number: 999, Content: "2009-12-21"}

	data, err := cbor.Marshal(wrongTag)
	if err != nil {
		t.Fatalf("Failed to marshal CBOR tag: %v", err)
	}

	// Try to unmarshal it with TimeWrapper - should fail because TimeWrapper expects tag 1004
	var wrapper TimeWrapper
	err = wrapper.UnmarshalCBOR(data)
	if err == nil {
		t.Error("Expected error when unmarshaling CBOR with wrong tag number, got nil")
	}
}

func TestTimeWrapper_HLLWrapper_UnmarshalCBOR_InvalidHll(t *testing.T) {
	var wrapper HLLWrapper

	// Test with invalid CBOR data
	err := wrapper.UnmarshalCBOR([]byte("invalid"))
	if err == nil {
		t.Error("Expected error when unmarshaling invalid CBOR data, got nil")
	}
}

func TestHLLWrapper_HLLWrapper_UnmarshalCBOR_InvalidHLLBytes(t *testing.T) {
	// Create CBOR with invalid HLL byte data
	invalidHLLBytes := []byte("test")
	data, err := cbor.Marshal(invalidHLLBytes)
	if err != nil {
		t.Fatalf("Failed to marshal invalid HLL bytes: %v", err)
	}

	var wrapper HLLWrapper
	err = wrapper.UnmarshalCBOR(data)
	if err == nil {
		t.Error("Expected error when unmarshaling invalid HLL bytes, got nil")
	}
}

func TestHLLWrapper_HLLWrapper_UnmarshalCBOR_WrongDataType(t *testing.T) {
	// Create CBOR with wrong data type (string instead of bytes)
	data, err := cbor.Marshal("test")
	if err != nil {
		t.Fatalf("Failed to marshal string: %v", err)
	}

	var wrapper HLLWrapper
	err = wrapper.UnmarshalCBOR(data)
	if err == nil {
		t.Error("Expected error when unmarshaling string as HLL, got nil")
	}
}
