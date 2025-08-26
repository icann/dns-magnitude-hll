package cmd

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"testing"
)

// validateReportJSON is a helper function to validate the expected JSON structure
func validateReportJSON(t *testing.T, jsonData []byte, expectedSource, expectedSourceType string) {
	t.Helper()

	var reportData map[string]any
	err := json.Unmarshal(jsonData, &reportData)
	if err != nil {
		t.Fatalf("Report output is not valid JSON: %v\nOutput: %s", err, string(jsonData))
	}

	expectedFields := []struct {
		field    string
		expected any
	}{
		{"source", expectedSource},
		{"sourceType", expectedSourceType},
		{"date", "2000-01-01"},
		{"totalUniqueClients", float64(70)},
		{"totalQueryVolume", float64(100)},
	}

	for _, tf := range expectedFields {
		actual, ok := reportData[tf.field]
		if !ok {
			t.Errorf("Field %s not found in report", tf.field)
			continue
		}
		if !reflect.DeepEqual(actual, tf.expected) {
			t.Errorf("Field %s: expected %v (%T), got %v (%T)", tf.field, tf.expected, tf.expected, actual, actual)
		}
	}

	magnitudeData, ok := reportData["magnitudeData"].([]any)
	if !ok {
		t.Fatalf("Expected magnitudeData to be an array, got %T", reportData["magnitudeData"])
	}

	expectedMagnitudeData := []any{
		map[string]any{
			"domain":        "arpa",
			"magnitude":     6.03731253380026,
			"uniqueClients": float64(13),
			"queryVolume":   float64(16),
		},
		map[string]any{
			"domain":        "com",
			"magnitude":     6.03731253380026,
			"uniqueClients": float64(13),
			"queryVolume":   float64(17),
		},
		map[string]any{
			"domain":        "net",
			"magnitude":     6.374139658435677,
			"uniqueClients": float64(15),
			"queryVolume":   float64(20),
		},
		map[string]any{
			"domain":        "org",
			"magnitude":     7.380246504446294,
			"uniqueClients": float64(23),
			"queryVolume":   float64(24),
		},
	}

	// Use DeepEqual to validate the complete magnitudeData structure
	if !reflect.DeepEqual(magnitudeData, expectedMagnitudeData) {
		t.Errorf("magnitudeData does not match expected structure")
		t.Logf("Expected: %+v", expectedMagnitudeData)
		t.Logf("Actual: %+v", magnitudeData)
	}
}

func TestReportCmd_OutputToStdout(t *testing.T) {
	// Create temporary DNSMAG file
	tmpDnsmag, err := os.CreateTemp("", "test_report_*.dnsmag")
	if err != nil {
		t.Fatalf("Failed to create temp DNSMAG file: %v", err)
	}
	defer os.Remove(tmpDnsmag.Name())
	tmpDnsmag.Close()

	// First, collect data from test1.pcap.gz into the temporary DNSMAG file
	executeCollectAndVerify(t, []string{
		"../../testdata/test1.pcap.gz",
		"--output", tmpDnsmag.Name(),
	}, 100, "PCAP")

	// Now generate a report from the DNSMAG file
	reportCmd := newReportCmd()
	reportCmd.SetArgs([]string{
		tmpDnsmag.Name(),
		"--source", "test-source",
		"--source-type", "authoritative",
	})

	var reportStdout bytes.Buffer
	var reportStderr bytes.Buffer
	reportCmd.SetOut(&reportStdout)
	reportCmd.SetErr(&reportStderr)

	err = reportCmd.Execute()
	if err != nil {
		t.Fatalf("Report command failed: %v\nOutput: %s", err, reportStdout.String())
	}

	output := reportStdout.String()

	// Validate the JSON output using shared helper
	validateReportJSON(t, []byte(output), "test-source", "authoritative")

	// Validate that stderr is empty
	if reportStderr.Len() != 0 {
		t.Errorf("Expected stderr to be empty, got: %s", reportStderr.String())
	}

	t.Logf("Complete report structure validation passed")
	t.Logf("Report command output:\n%s", output)
}

func TestReportCmd_OutputToStdout_FromStdin(t *testing.T) {
	// Create temporary DNSMAG file
	tmpDnsmag, err := os.CreateTemp("", "test_report_pipe_*.dnsmag")
	if err != nil {
		t.Fatalf("Failed to create temp DNSMAG file: %v", err)
	}
	defer os.Remove(tmpDnsmag.Name())
	tmpDnsmag.Close()

	// First, collect data from test1.pcap.gz into the temporary DNSMAG file
	executeCollectAndVerify(t, []string{
		"../../testdata/test1.pcap.gz",
		"--output", tmpDnsmag.Name(),
	}, 100, "PCAP")

	// Open the DNSMAG file for reading (simulate piping to stdin)
	file, err := os.Open(tmpDnsmag.Name())
	if err != nil {
		t.Fatalf("Failed to open DNSMAG file for reading: %v", err)
	}
	defer file.Close()

	reportCmd := newReportCmd()
	reportCmd.SetArgs([]string{
		"-",
		"--source", "test-source",
		"--source-type", "recursive",
		"--output", "-",
		"--verbose",
	})

	var reportBuf bytes.Buffer
	var reportErrBuf bytes.Buffer
	reportCmd.SetIn(file)
	reportCmd.SetOut(&reportBuf)
	reportCmd.SetErr(&reportErrBuf)

	err = reportCmd.Execute()
	if err != nil {
		t.Fatalf("Report command failed: %v\nOutput: %s", err, reportBuf.String())
	}

	output := reportBuf.String()

	// Validate the JSON output using shared helper
	validateReportJSON(t, []byte(output), "test-source", "recursive")

	// Check that the verbose message about printing the report appears on stderr
	expectedVerboseMsg := "Report written to STDOUT"
	if !regexp.MustCompile(regexp.QuoteMeta(expectedVerboseMsg)).MatchString(reportErrBuf.String()) {
		t.Errorf("Expected verbose message about printing report to STDOUT not found in stderr: %s", reportErrBuf.String())
	}

	t.Logf("Complete report structure validation passed (piped to stdin, output to stdout)")
	t.Logf("Report command output:\n%s", output)
}

func TestReportCmd_WithOutputFile(t *testing.T) {
	// Create temporary DNSMAG file
	tmpDnsmag, err := os.CreateTemp("", "test_report_output_*.dnsmag")
	if err != nil {
		t.Fatalf("Failed to create temp DNSMAG file: %v", err)
	}
	defer os.Remove(tmpDnsmag.Name())
	tmpDnsmag.Close()

	// Create temporary output file
	tmpOutput, err := os.CreateTemp("", "test_report_*.json")
	if err != nil {
		t.Fatalf("Failed to create temp output file: %v", err)
	}
	defer os.Remove(tmpOutput.Name())
	tmpOutput.Close()

	// Collect data from test1.pcap.gz
	executeCollectAndVerify(t, []string{
		"../../testdata/test1.pcap.gz",
		"--output", tmpDnsmag.Name(),
	}, 100, "PCAP")

	// Generate report with output file and verbose mode
	reportCmd := newReportCmd()
	reportCmd.SetArgs([]string{
		tmpDnsmag.Name(),
		"--source", "test-provider",
		"--source-type", "recursive",
		"--output", tmpOutput.Name(),
		"--verbose",
	})

	var reportBuf bytes.Buffer
	reportCmd.SetOut(&reportBuf)
	reportCmd.SetErr(&reportBuf)

	err = reportCmd.Execute()
	if err != nil {
		t.Fatalf("Report command failed: %v\nOutput: %s", err, reportBuf.String())
	}

	// Verify verbose output message
	output := reportBuf.String()
	expectedMessage := "Report written to " + tmpOutput.Name()
	if !regexp.MustCompile(regexp.QuoteMeta(expectedMessage)).MatchString(output) {
		t.Errorf("Expected verbose message about output file not found in: %s", output)
	}

	// Verify the output file was created and contains valid JSON
	fileData, err := os.ReadFile(tmpOutput.Name())
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	// Validate the JSON output using shared helper
	validateReportJSON(t, fileData, "test-provider", "recursive")
}

func TestReportCmd_InvalidSourceType(t *testing.T) {
	// Create temporary DNSMAG file
	tmpDnsmag, err := os.CreateTemp("", "test_report_invalid_*.dnsmag")
	if err != nil {
		t.Fatalf("Failed to create temp DNSMAG file: %v", err)
	}
	defer os.Remove(tmpDnsmag.Name())
	tmpDnsmag.Close()

	// Collect minimal data
	executeCollectAndVerify(t, []string{
		"../../testdata/test1.pcap.gz",
		"--output", tmpDnsmag.Name(),
	}, 100, "PCAP")

	// Try to generate report with invalid source-type
	reportCmd := newReportCmd()
	reportCmd.SetArgs([]string{
		tmpDnsmag.Name(),
		"--source", "test-source",
		"--source-type", "invalid",
	})

	var reportBuf bytes.Buffer
	reportCmd.SetOut(&reportBuf)
	reportCmd.SetErr(&reportBuf)

	err = reportCmd.Execute()
	if err == nil {
		t.Error("Expected error for invalid source-type, got none")
		return
	}

	// Verify error message
	if !regexp.MustCompile(`invalid source-type 'invalid'`).MatchString(err.Error()) {
		t.Errorf("Expected error about invalid source-type, got: %v", err)
	}
}

func TestReportCmd_NonExistentFile(t *testing.T) {
	reportCmd := newReportCmd()
	reportCmd.SetArgs([]string{
		"non-existent.dnsmag",
		"--source", "test-source",
	})

	var reportBuf bytes.Buffer
	reportCmd.SetOut(&reportBuf)
	reportCmd.SetErr(&reportBuf)

	err := reportCmd.Execute()
	if err == nil {
		t.Error("Expected error for non-existent file, got none")
		return
	}

	// Verify error message
	if !regexp.MustCompile(`failed to load DNSMAG file`).MatchString(err.Error()) {
		t.Errorf("Expected error about loading DNSMAG file, got: %v", err)
	}
}
