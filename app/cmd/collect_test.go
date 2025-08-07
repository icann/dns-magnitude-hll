package cmd

import (
	"bytes"
	"dnsmag/internal"
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

			t.Log(stdout.String())

			for _, this := range tt.expectedOutput {
				if !this.MatchString(stdout.String()) {
					t.Fatalf(
						"expected pattern %q not found in output:\n%s",
						this.String(), stdout.String())
				}
			}

			if stderr.Len() > 0 {
				t.Fatalf("unexpected stderr output:\n%s", stderr.String())
			}
		})
	}
}
