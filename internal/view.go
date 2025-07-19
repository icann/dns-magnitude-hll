// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"io"
	"math"
	"time"
)

// countAsString returns a string with the length of extraAllClients, the clientCount, and the percent difference
// e.g. "3906 (estimated: 3923, diff: +0.44%)""
func countAsString(actual, estimated uint) string {
	if actual != 0 {
		diff := int(estimated) - int(actual)
		percentDiff := (math.Abs(float64(diff)) / float64(actual)) * 100
		sign := '+'
		if diff < 0 {
			sign = '−'
		}
		return fmt.Sprintf("%d (estimated: %d, diff: %c%.2f%%)", actual, estimated, sign, percentDiff)
	} else {
		return fmt.Sprintf("%d (estimated)", estimated)
	}
}

// FormatDomainStats prepares domain statistics for printing.
func FormatDomainStats(w io.Writer, stats MagnitudeDataset, elapsed time.Duration) {

	fmt.Fprintln(w, "Domain counts:")

	for _, dm := range stats.SortedByMagnitude() {
		fmt.Fprintf(w, "%-30s magnitude: %.3f, queries %d, clients %s, hll size %d\n",
			string(dm.Domain),
			dm.Magnitude,
			dm.DomainHll.QueriesCount,
			countAsString(uint(len(dm.DomainHll.extraAllClients)), uint(dm.DomainHll.ClientsCount)),
			len(dm.DomainHll.Hll.ToBytes()),
		)
	}

	fmt.Fprintln(w, "")
	fmt.Fprintf(w, "Global statistics         :\n")
	fmt.Fprintf(w, "Date                      : %s\n", stats.DateString())
	fmt.Fprintf(w, "Total queries             : %d\n", stats.AllQueriesCount)
	if stats.extraDomainsCount > 0 {
		// If stats.extraDomainsCount is set, it is the number of domains before truncation
		fmt.Fprintf(w, "Total domains             : %d (truncated: %d)\n", stats.extraDomainsCount, len(stats.Domains))
	} else {
		fmt.Fprintf(w, "Total domains             : %d\n", len(stats.Domains))
	}
	fmt.Fprintf(w, "Total unique source IPs   : %s\n", countAsString(uint(len(stats.extraAllClients)), uint(stats.AllClientsCount)))
	if len(stats.extraV6Clients) > 0 {
		// Information about IPv6 clients is only available in the "collect" command. It is not saved in the DNSMAG file.
		fmt.Fprintf(w, "Total unique v6 source IPs: %d\n", uint(len(stats.extraV6Clients)))
	}
	fmt.Fprintf(w, "Global HLL storage size   : %d bytes\n", len(stats.GlobalHll.ToBytes()))

	if elapsed != 0 {
		fmt.Fprintf(w, "\nExecution time: %s\n", elapsed)
	}
}
