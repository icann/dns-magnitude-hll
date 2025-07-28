// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"time"
)

// countAsString returns a string with an estimated number, the actual number if known, and the percent difference
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

// TableRow represents a row in the output table with left and right columns
type TableRow struct {
	lhs string
	rhs string
}

// printTable prints a table with dynamic column widths
func printTable(w io.Writer, rows []TableRow) error {
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
func FormatDomainStats(stats MagnitudeDataset) ([]TableRow, []string, error) {
	// Build table rows for global statistics
	var table []TableRow
	var domains []string

	var domainHllSize uint

	for _, dm := range stats.SortedByMagnitude() {
		domainHllSize += uint(len(dm.DomainHll.Hll.ToBytes()))

		domainInfo := fmt.Sprintf("%-33s magnitude: %.3f, queries %d, clients %s, hll size %d",
			string(dm.Domain),
			dm.Magnitude,
			dm.DomainHll.QueriesCount,
			countAsString(uint(len(dm.DomainHll.extraAllClients)), uint(dm.DomainHll.ClientsCount)),
			len(dm.DomainHll.Hll.ToBytes()),
		)
		domains = append(domains, domainInfo)
	}

	table = append(table, TableRow{"Dataset statistics", ""})
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

	table = append(table, TableRow{"All clients HLL storage size", fmt.Sprintf("%d bytes", len(stats.AllClientsHll.ToBytes()))})
	table = append(table, TableRow{"Per domain total HLL storage size", fmt.Sprintf("%d bytes", domainHllSize)})

	// Add memory usage statistics
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapStr := fmt.Sprintf("%d MB", m.HeapAlloc/1024/1024)
	maxStr := fmt.Sprintf("%d MB", m.HeapSys/1024/1024)
	table = append(table, TableRow{"Memory allocated", fmt.Sprintf("%s (peak estimated: %s)", heapStr, maxStr)})

	return table, domains, nil
}

// FormatTimingStats formats timing statistics as table rows
func FormatTimingStats(timing *TimingStats) []TableRow {
	var table []TableRow

	table = append(table, TableRow{"Timing statistics", ""})
	table = append(table, TableRow{"Total execution time", timing.TotalElapsed.Truncate(time.Millisecond).String()})
	if timing.ParsingElapsed > 0 {
		overhead := timing.TotalElapsed - timing.ParsingElapsed
		table = append(table, TableRow{"File parsing time", timing.ParsingElapsed.Truncate(time.Millisecond).String()})
		table = append(table, TableRow{"Processing overhead", overhead.Truncate(time.Millisecond).String()})
	}

	return table
}

// OutputDomainStats formats and prints domain statistics based on flags
func OutputDomainStats(stats MagnitudeDataset, quiet, verbose bool) error {
	if quiet {
		return nil // Skip output in quiet mode
	}

	table, domains, err := FormatDomainStats(stats)
	if err != nil {
		return fmt.Errorf("failed to format domain statistics: %w", err)
	}

	if verbose && len(domains) > 0 {
		fmt.Println()
		fmt.Println("Domain counts:")
		for _, domain := range domains {
			fmt.Println(domain)
		}
		fmt.Println()
	}

	return printTable(os.Stdout, table)
}

// OutputTimingStats formats and prints timing statistics based on flags
func OutputTimingStats(timing *TimingStats, quiet bool) error {
	if quiet || timing == nil {
		return nil // Skip output in quiet mode or if no timing data
	}

	table := FormatTimingStats(timing)
	if err := printTable(os.Stdout, table); err != nil {
		return fmt.Errorf("failed to print timing statistics: %w", err)
	}
	return nil
}
