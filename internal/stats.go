// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"io"
	"math"
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
		if row.lhs == "" {
			// separator
			fmt.Fprintln(w)
			continue
		}
		if _, err := fmt.Fprintf(w, "%-*s : %s\n", maxLHSWidth, row.lhs, row.rhs); err != nil {
			return err
		}
	}
	return nil
}

// formatDomainRecords traverses domains and builds domain information records
func formatDomainRecords(stats MagnitudeDataset) ([]TableRow, []string) {
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
	table = append(table, TableRow{"Per domain total HLL storage size", fmt.Sprintf("%d bytes", domainHllSize)})

	return table, domains
}

// formatGeneralStats builds general dataset statistics table rows
func formatGeneralStats(stats MagnitudeDataset) []TableRow {
	var table []TableRow

	table = append(table, TableRow{"Dataset statistics", ""})
	table = append(table, TableRow{"Date", stats.DateString()})
	table = append(table, TableRow{"Total queries", fmt.Sprintf("%d", stats.AllQueriesCount)})

	numDomains := uint64(len(stats.Domains))
	if len(stats.extraAllDomains) > 0 {
		numDomains = uint64(len(stats.extraAllDomains))
		// If stats.extraAllDomains is set, it contains all domains before truncation
		table = append(table, TableRow{"Total domains", fmt.Sprintf("%d (truncated: %d)", numDomains, len(stats.Domains))})
	} else {
		table = append(table, TableRow{"Total domains", fmt.Sprintf("%d", numDomains)})
	}

	table = append(table, TableRow{"Total unique source IPs", countAsString(uint(len(stats.extraAllClients)), uint(stats.AllClientsCount))})

	if len(stats.extraV6Clients) > 0 {
		// Information about IPv6 clients is only available in the "collect" command. It is not saved in the DNSMAG file.
		table = append(table, TableRow{"Total unique v6 source IPs", fmt.Sprintf("%d", uint(len(stats.extraV6Clients)))})
	}

	// Add memory usage statistics
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	heapStr := fmt.Sprintf("%d MB", m.HeapAlloc/1024/1024)
	maxStr := fmt.Sprintf("%d MB", m.HeapSys/1024/1024)
	table = append(table, TableRow{"Memory allocated", fmt.Sprintf("%s (peak estimated: %s)", heapStr, maxStr)})
	table = append(table, TableRow{"Memory allocated per domain", fmt.Sprintf("%d B (peak)", m.HeapSys/numDomains)})

	table = append(table, TableRow{"All clients HLL storage size", fmt.Sprintf("%d bytes", len(stats.AllClientsHll.ToBytes()))})

	return table
}

// FormatDomainStats prepares domain statistics for printing.
func FormatDomainStats(stats MagnitudeDataset) ([]TableRow, []string, error) {
	domainTable, domains := formatDomainRecords(stats)
	generalTable := formatGeneralStats(stats)

	var table []TableRow

	// Concatenate tables
	table = append(table, generalTable...)
	table = append(table, domainTable...)

	return table, domains, nil
}

// FormatTimingStats formats timing statistics as table rows
func FormatTimingStats(timing *TimingStats, recordCount uint) []TableRow {
	var table []TableRow

	table = append(table, TableRow{"Timing statistics", ""})
	table = append(table, TableRow{"Total execution time", timing.TotalElapsed.Truncate(time.Millisecond).String()})
	if timing.ParsingElapsed > 0 {
		table = append(table, TableRow{"File parsing time", timing.ParsingElapsed.Truncate(time.Millisecond).String()})

		if recordCount > 0 {
			recordsPerSecond := float64(recordCount) / timing.TotalElapsed.Seconds()
			table = append(table, TableRow{"Records processed per second", fmt.Sprintf("%.0f", recordsPerSecond)})
		}
	}

	return table
}

// OutputDatasetStats formats and prints statistics from a MagnitudeDataset
func OutputDatasetStats(w io.Writer, stats MagnitudeDataset, quiet, verbose bool) error {
	if quiet {
		return nil // Skip output in quiet mode
	}

	table, domains, err := FormatDomainStats(stats)
	if err != nil {
		return fmt.Errorf("failed to format dataset statistics: %w", err)
	}

	if verbose && len(domains) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Domain counts:")
		for _, domain := range domains {
			fmt.Fprintln(w, domain)
		}
		fmt.Fprintln(w)
	}

	return printTable(w, table)
}

// OutputCollectorStats formats and prints both dataset and timing statistics for collection operations
func OutputCollectorStats(w io.Writer, collector *Collector, quiet, verbose bool, args []string) error {
	if quiet {
		return nil // Skip output in quiet mode
	}

	if len(args) == 1 {
		fmt.Fprintf(w, "Statistics for %s:\n", args[0])
	} else {
		fmt.Fprintf(w, "Aggregated statistics for %d files:\n", len(args))
	}
	fmt.Fprintln(w)

	if err := OutputDatasetStats(w, collector.Result, quiet, verbose); err != nil {
		return err
	}

	fmt.Fprintln(w)

	if quiet || collector.timing == nil {
		return nil // Skip timing output in quiet mode or if no timing data
	}

	table := FormatTimingStats(collector.timing, collector.recordCount)
	if err := printTable(w, table); err != nil {
		return fmt.Errorf("failed to print timing statistics: %w", err)
	}
	return nil
}

// OutputTimingStats formats and prints timing statistics based on flags
func OutputTimingStats(w io.Writer, timing *TimingStats, quiet bool) error {
	if quiet || timing == nil {
		return nil // Skip output in quiet mode or if no timing data
	}

	table := FormatTimingStats(timing, 0)
	if err := printTable(w, table); err != nil {
		return fmt.Errorf("failed to print timing statistics: %w", err)
	}
	return nil
}
