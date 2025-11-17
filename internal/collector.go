// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"time"
)

type Collector struct {
	topCount           int
	chunkSize          uint
	verbose            bool
	current            MagnitudeDataset
	Result             MagnitudeDataset // Resulting dataset after processing
	recordCount        uint             // Count of processed records
	chunkCount         uint             // Number of chunks processed
	timing             *TimingStats     // Timing statistics
	invalidDomainCount uint             // Count of invalid domains encountered
	invalidRecordCount uint             // Count of invalid records encountered
	filesLoaded        []string         // List of files that were successfully loaded
}

func NewCollector(topCount int, chunkSize uint, verbose bool, date *time.Time, timing *TimingStats) *Collector {
	c := &Collector{
		topCount:           topCount,
		chunkSize:          chunkSize,
		verbose:            verbose,
		current:            newDataset(date),
		Result:             newDataset(date),
		chunkCount:         0,
		timing:             timing,
		invalidDomainCount: 0,
		invalidRecordCount: 0,
		filesLoaded:        nil,
	}
	c.SetDate(date)
	return c
}

func (c *Collector) ProcessRecord(domainStr string, src IPAddress, queryCount uint64) error {
	err := c.current.updateStats(domainStr, src, queryCount, c.verbose)
	if err != nil {
		c.invalidDomainCount++
		return nil // Invalid domain is not a fatal error
	}

	c.recordCount++
	if c.chunkSize != 0 && c.recordCount%c.chunkSize == 0 {
		if err := c.migrateCurrent(); err != nil {
			return fmt.Errorf("failed to migrate current dataset: %w", err)
		}
	}
	return nil
}

func (c *Collector) migrateCurrent() error {
	if c.current.AllQueriesCount == 0 {
		return nil
	}
	c.Result.Date = c.current.Date

	// Aggregate current dataset into result
	res, err := AggregateDatasets([]MagnitudeDataset{c.Result, c.current})
	if err != nil {
		return fmt.Errorf("failed to aggregate datasets: %w", err)
	}
	res.Truncate(c.topCount)
	c.Result = res
	c.current = newDataset(&c.Result.Date.Time)

	c.chunkCount++

	// Run garbage collection to free memory
	runtime.GC()
	return nil
}

// Since "current" is not public, we need a public method to set the date
func (c *Collector) SetDate(date *time.Time) {
	c.current.SetDate(date)
}

func (c *Collector) Finalise() error {
	if err := c.migrateCurrent(); err != nil {
		return fmt.Errorf("failed to migrate current dataset: %w", err)
	}

	// Truncate the aggregated stats to the top N domains
	c.Result.Truncate(c.topCount)
	c.Result.finaliseStats()
	return nil
}

// ProcessFiles processes multiple input files into collector.Result
func (c *Collector) ProcessFiles(files []string, filetype string, stdin io.Reader, stderr io.Writer) error {
	c.timing.StartParsing()

	// Process each input file
	for _, inputFile := range files {
		if c.verbose {
			fmt.Fprintf(stderr, "Loading %s file: %s\n", filetype, inputFile)
		}

		var err error
		var reader io.Reader
		if inputFile == "-" {
			reader = stdin
			inputFile = "<stdin>"
		} else {
			var f *os.File
			f, err = os.Open(inputFile)
			if err == nil {
				defer f.Close()
				reader = f
			}
		}

		if reader != nil {
			if filetype == "csv" || filetype == "tsv" {
				err = LoadCSVFromReader(reader, c, filetype)
			} else {
				err = LoadPcap(reader, c)
			}
		}

		if err != nil {
			return fmt.Errorf("failed to load %s file %s: %w", filetype, inputFile, err)
		}
	}

	c.timing.StopParsing()

	if err := c.Finalise(); err != nil {
		return fmt.Errorf("failed to finalise collection: %w", err)
	}

	c.filesLoaded = files

	return nil
}
