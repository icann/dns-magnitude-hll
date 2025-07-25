// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"io"
	"math"
	"runtime"
	"time"
)

// countAsString returns a string with the length of extraAllClients, the clientCount, and the percent difference
// e.g. "3906 (estimated: 3923, diff: +0.44%)""
func countAsString(actual, estimated uint) string {
	if actual > math.MaxInt || estimated > math.MaxInt {
		return fmt.Sprintf("%d (estimated: %d)", actual, estimated)
	}
	if actual != 0 {
		diff := int(estimated) - int(actual)
		percentDiff := (math.Abs(float64(diff)) / float64(actual)) * 100
		sign := '+'
		if diff < 0 {
			sign = '−'
		}
		return fmt.Sprintf("%d (estimated: %d, diff: %c%.2f%%)", actual, estimated, sign, percentDiff)
	}
	return fmt.Sprintf("%d (estimated)", estimated)
}

// tableRow represents a row in the output table with left and right columns
type tableRow struct {
	lhs string
	rhs string
}

// printTable prints a table with dynamic column widths
func printTable(w io.Writer, rows []tableRow) error {
	if len(rows) == 0 {
		return nil
	}

	maxLHSWidth := 0
	for _, row := range rows {
		if len(row.lhs) > maxLHSWidth {
			maxLHSWidth = len(row.lhs)
		}
	}

	for _, row := range rows {
		if _, err := fmt.Fprintf(w, "%-*s : %s\n", maxLHSWidth, row.lhs, row.rhs); err != nil {
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
	var table []tableRow

	table = append(table, tableRow{"Date", stats.DateString()})
	table = append(table, tableRow{"Total queries", fmt.Sprintf("%d", stats.AllQueriesCount)})

	if stats.extraDomainsCount > 0 {
		// If stats.extraDomainsCount is set, it is the number of domains before truncation
		table = append(table, tableRow{"Total domains", fmt.Sprintf("%d (truncated: %d)", stats.extraDomainsCount, len(stats.Domains))})
	} else {
		table = append(table, tableRow{"Total domains", fmt.Sprintf("%d", len(stats.Domains))})
	}

	table = append(table, tableRow{"Total unique source IPs", countAsString(uint(len(stats.extraAllClients)), uint(stats.AllClientsCount))})

	if len(stats.extraV6Clients) > 0 {
		// Information about IPv6 clients is only available in the "collect" command. It is not saved in the DNSMAG file.
		table = append(table, tableRow{"Total unique v6 source IPs", fmt.Sprintf("%d", uint(len(stats.extraV6Clients)))})
	}

	table = append(table, tableRow{"Global HLL storage size", fmt.Sprintf("%d bytes", len(stats.AllClientsHll.ToBytes()))})

	// Add memory usage statistics
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	table = append(table, tableRow{"Memory Alloc", fmt.Sprintf("%d MB", m.Alloc/1024/1024)})
	table = append(table, tableRow{"Memory TotalAlloc", fmt.Sprintf("%d MB", m.TotalAlloc/1024/1024)})
	table = append(table, tableRow{"Memory HeapSys", fmt.Sprintf("%d MB", m.HeapSys/1024/1024)})
	table = append(table, tableRow{"Memory Sys", fmt.Sprintf("%d MB", m.Sys/1024/1024)})
	table = append(table, tableRow{"Memory NumGC", fmt.Sprintf("%d", m.NumGC)})

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
