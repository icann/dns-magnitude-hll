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

// TableRow represents a row in the output table with left and right columns
type TableRow struct {
	Lhs string
	Rhs string
}

// printTable prints a table with dynamic column widths
func printTable(w io.Writer, rows []TableRow) error {
	if len(rows) == 0 {
		return nil
	}

	maxLhsWidth := 0
	for _, row := range rows {
		if len(row.Lhs) > maxLhsWidth {
			maxLhsWidth = len(row.Lhs)
		}
	}

	for _, row := range rows {
		if _, err := fmt.Fprintf(w, "%-*s : %s\n", maxLhsWidth, row.Lhs, row.Rhs); err != nil {
			return err
		}
	}
	return nil
}

// FormatDomainStats prepares domain statistics for printing.
func FormatDomainStats(w io.Writer, stats MagnitudeDataset, elapsed time.Duration) error {

	if _, err := fmt.Fprintln(w, "Domain counts:"); err != nil {
		return err
	}

	for _, dm := range stats.SortedByMagnitude() {
		if _, err := fmt.Fprintf(w, "%-30s magnitude: %.3f, queries %d, clients %s, hll size %d\n",
			string(dm.Domain),
			dm.Magnitude,
			dm.DomainHll.QueriesCount,
			countAsString(uint(len(dm.DomainHll.extraAllClients)), uint(dm.DomainHll.ClientsCount)),
			len(dm.DomainHll.Hll.ToBytes()),
		); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(w, ""); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w, "Global statistics:"); err != nil {
		return err
	}

	// Build table rows for global statistics
	var table []TableRow

	table = append(table, TableRow{"Date", stats.DateString()})
	table = append(table, TableRow{"Total queries", fmt.Sprintf("%d", stats.AllQueriesCount)})

	if stats.extraDomainsCount > 0 {
		// If stats.extraDomainsCount is set, it is the number of domains before truncation
		table = append(table, TableRow{"Total domains", fmt.Sprintf("%d (truncated: %d)", stats.extraDomainsCount, len(stats.Domains))})
	} else {
		table = append(table, TableRow{"Total domains", fmt.Sprintf("%d", len(stats.Domains))})
	}

	table = append(table, TableRow{"Total unique source IPs", countAsString(uint(len(stats.extraAllClients)), uint(stats.AllClientsCount))})

	if len(stats.extraV6Clients) > 0 {
		// Information about IPv6 clients is only available in the "collect" command. It is not saved in the DNSMAG file.
		table = append(table, TableRow{"Total unique v6 source IPs", fmt.Sprintf("%d", uint(len(stats.extraV6Clients)))})
	}

	table = append(table, TableRow{"Global HLL storage size", fmt.Sprintf("%d bytes", len(stats.AllClientsHll.ToBytes()))})

	if err := printTable(w, table); err != nil {
		return err
	}

	if elapsed != 0 {
		if _, err := fmt.Fprintf(w, "\nExecution time: %s\n", elapsed); err != nil {
			return err
		}
	}

	return nil
}
