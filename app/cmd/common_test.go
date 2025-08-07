package cmd

import (
	"bytes"
	"fmt"
	"regexp"
	"testing"
)

// executeCollectAndVerify is a helper function to execute a collect command and verify the query count
func executeCollectAndVerify(t *testing.T, args []string, expectedQueries int, description string) string {
	t.Helper()

	collectCmd := newCollectCmd()
	collectCmd.SetArgs(args)

	var collectBuf bytes.Buffer
	collectCmd.SetOut(&collectBuf)
	collectCmd.SetErr(&collectBuf)

	err := collectCmd.Execute()
	if err != nil {
		t.Fatalf("%s collect command failed: %v\nOutput: %s", description, err, collectBuf.String())
	}

	// Verify the expected query count
	output := collectBuf.String()
	queriesPattern := regexp.MustCompile(`Total queries\s+:\s+(\d+)`)
	matches := queriesPattern.FindStringSubmatch(output)
	if len(matches) < 2 {
		t.Errorf("Could not find total queries count in %s output: %s", description, output)
	} else if matches[1] != fmt.Sprintf("%d", expectedQueries) {
		t.Errorf("Expected %s to have %d queries, got %s", description, expectedQueries, matches[1])
	}
	t.Logf("%s collect output shows %s queries", description, matches[1])

	return collectBuf.String()
}
