package cmd

import (
	"bytes"
	"dnsmag/internal"
	"os"
	"regexp"
	"testing"
)

func init() {
	internal.InitStats()
}

func TestCollect_JustCollect(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		expectedOutput []*regexp.Regexp
	}{
		{
			name: "basic collect",
			args: []string{"../../testdata/test1.pcap.gz"},
			expectedOutput: []*regexp.Regexp{
				regexp.MustCompile(`Statistics for .*test1.pcap.gz:`),
				regexp.MustCompile(`Dataset statistics`),
				regexp.MustCompile(`Date\s+:\s+2000-01-01`),
				regexp.MustCompile(`Total queries\s+:\s+100`),
				regexp.MustCompile(`Total domains\s+:\s+4`),
				regexp.MustCompile(`Collection statistics`),
				regexp.MustCompile(`Files loaded\s+:\s+1`),
				regexp.MustCompile(`Records processed\s+:\s+100`),
				regexp.MustCompile(`Timing statistics`),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdout := &bytes.Buffer{}
			stderr := &bytes.Buffer{}
			cmd := newCollectCmd()
			cmd.SetArgs(tt.args)
			cmd.SetOut(stdout)
			cmd.SetErr(stderr)
			cmd.Execute()

			t.Log(stderr.String())

			for _, this := range tt.expectedOutput {
				if !this.MatchString(stderr.String()) {
					t.Fatalf(
						"expected pattern %q not found in output:\n%s",
						this.String(), stdout.String())
				}
			}

			// No output to stdout is expected for this test
			if stdout.Len() > 0 {
				t.Fatalf("unexpected stdout output:\n%s", stdout.String())
			}
		})
	}
}

func TestCollect_JustCollect_Stdin(t *testing.T) {
	// Read the test pcap.gz into memory and provide it as stdin to the command.
	path := "../../testdata/test1.pcap.gz"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read test pcap file: %v", err)
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd := newCollectCmd()
	// Use "-" to indicate stdin as input, do not request output file so stats print to stderr.
	cmd.SetArgs([]string{"-"})
	cmd.SetIn(bytes.NewReader(data))
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("collect command failed when reading from stdin: %v\nstderr: %s", err, stderr.String())
	}

	// Verify stderr contains expected statistics
	expectedPatterns := []*regexp.Regexp{
		regexp.MustCompile(`Dataset statistics`),
		regexp.MustCompile(`Date\s*:\s*2000-01-01`),
		regexp.MustCompile(`Total queries\s*:\s*100`),
		regexp.MustCompile(`Total domains\s*:\s*4`),
		regexp.MustCompile(`Records processed\s*:\s*100`),
	}

	out := stderr.String()
	for _, re := range expectedPatterns {
		if !re.MatchString(out) {
			t.Fatalf("expected pattern %q not found in stderr:\n%s", re.String(), out)
		}
	}

	// For this invocation (no --output), stdout should be empty
	if stdout.Len() > 0 {
		t.Fatalf("unexpected stdout output when reading from stdin:\n%s", stdout.String())
	}
}

func TestCollect_WriteDNSMagFile_ReportCommandLine(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd := newCollectCmd()
	cmd.SetArgs([]string{"../../testdata/test1.pcap.gz", "--output", "-"})
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	err := cmd.Execute()
	if err != nil {
		t.Fatalf("collect command failed: %v\nstderr: %s", err, stderr.String())
	}

	// Now pass the CBOR output buffer to the report command as stdin
	reportStdout := &bytes.Buffer{}
	reportStderr := &bytes.Buffer{}
	reportCmd := newReportCmd()
	reportCmd.SetArgs([]string{"-", "--source", "test"})
	reportCmd.SetIn(stdout)
	reportCmd.SetOut(reportStdout)
	reportCmd.SetErr(reportStderr)
	err = reportCmd.Execute()
	if err != nil {
		t.Fatalf("report command failed: %v\nstderr: %s", err, reportStderr.String())
	}

	regexpChecks := []struct {
		name     string
		pattern  string
		expected bool
	}{
		{"date", `"date"\s*:\s*"2000-01-01"`, true},
		{"totalQueryVolume", `"totalQueryVolume"\s*:\s*100`, true},
		{"domain com", `"domain"\s*:\s*"com"`, true},
		{"domain net", `"domain"\s*:\s*"net"`, true},
		{"domain org", `"domain"\s*:\s*"org"`, true},
		{"domain arpa", `"domain"\s*:\s*"arpa"`, true},
	}

	jsonOutput := reportStdout.String()
	for _, check := range regexpChecks {
		re := regexp.MustCompile(check.pattern)
		found := re.MatchString(jsonOutput)
		if found != check.expected {
			t.Errorf("Regexp check '%s' failed: pattern %q, expected %v, got %v\nOutput:\n%s",
				check.name, check.pattern, check.expected, found, jsonOutput)
		}
	}
}

func TestCollect_NoDomainsCSV_Verbose(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	// create temp CSV file with only a comment to get past the GZIP detection
	tmp, err := os.CreateTemp("", "no_domains_*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString("# this is an empty CSV file\n"); err != nil {
		tmp.Close()
		t.Fatalf("failed to write temp file: %v", err)
	}
	tmp.Close()

	cmd := newCollectCmd()
	cmd.SetArgs([]string{tmp.Name(), "--filetype", "csv", "--verbose"})
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("collect command failed: %v\nstderr: %s", err, stderr.String())
	}

	t.Log(stderr.String())

	// No output to stdout is expected for this test
	if stdout.Len() > 0 {
		t.Fatalf("unexpected stdout output:\n%s", stdout.String())
	}

	tests := []struct {
		name           string
		args           []string
		expectedOutput []*regexp.Regexp
	}{
		{
			name: "basic collect",
			args: []string{"../../testdata/test1.pcap.gz"},
			expectedOutput: []*regexp.Regexp{
				regexp.MustCompile(`Statistics for .*no_domains.*:`),
				regexp.MustCompile(`Dataset statistics`),
				regexp.MustCompile(`Total queries\s+:\s+0`),
				regexp.MustCompile(`Total domains\s+:\s+0`),
				regexp.MustCompile(`Collection statistics`),
				regexp.MustCompile(`Files loaded\s+:\s+1`),
				regexp.MustCompile(`Records processed\s+:\s+0`),
				regexp.MustCompile(`Timing statistics`),
				regexp.MustCompile(`Memory allocated\s+:\s+\d+ MB`), // expect single digit MB, 0 or 1 usually
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, this := range tt.expectedOutput {
				if !this.MatchString(stderr.String()) {
					t.Fatalf(
						"expected pattern %q not found in output:\n%s",
						this.String(), stdout.String())
				}
			}
		})
	}
}
