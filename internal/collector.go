// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
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

func NewCollector(topCount, chunkSize int, verbose bool, date *time.Time, timing *TimingStats) *Collector {
	c := &Collector{
		topCount: topCount,
		chunkSize: func() uint {
			if chunkSize < 0 {
				return 0
			}
			return uint(chunkSize)
		}(),
		verbose:            verbose,
		current:            newDataset(),
		Result:             newDataset(),
		chunkCount:         0,
		timing:             timing,
		invalidDomainCount: 0,
		invalidRecordCount: 0,
		filesLoaded:        nil,
	}
	c.SetDate(date)
	return c
}

func (c *Collector) ProcessRecord(domainStr string, src IPAddress, queryCount uint64) {
	domain, err := getDomainName(domainStr, DefaultDNSDomainNameLabels)
	if err != nil {
		c.invalidDomainCount++
		return
	}

	c.current.updateStats(domain, src, queryCount, c.verbose)

	c.recordCount++
	if c.chunkSize != 0 && c.recordCount%c.chunkSize == 0 {
		c.migrateCurrent()
		c.chunkCount++
	}
}

func (c *Collector) migrateCurrent() {
	c.Result.Date = c.current.Date

	// Aggregate current dataset into result
	res, err := AggregateDatasets([]MagnitudeDataset{c.Result, c.current})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to aggregate datasets: %v\n", err)
		os.Exit(1)
	}
	res.Truncate(c.topCount)
	c.Result = res
	c.current = newDataset()
	c.current.Date = c.Result.Date // Keep the date from the result dataset

	// Run garbage collection to free memory
	runtime.GC()
}

// Since "current" is not public, we need a public method to set the date
func (c *Collector) SetDate(date *time.Time) {
	if date != nil {
		c.current.Date = &TimeWrapper{Time: date.UTC()}
	} else {
		now := time.Now().UTC()
		dateOnly := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		c.current.Date = &TimeWrapper{Time: dateOnly}
	}
	if c.Result.Date == nil {
		c.Result.Date = c.current.Date // Set the date for the result dataset if not already set
	}
}

func (c *Collector) finalise() {
	c.migrateCurrent()

	// Truncate the aggregated stats to the top N domains
	c.Result.Truncate(c.topCount)
	c.Result.finaliseStats()
}

// ProcessFiles processes multiple input files into collector.Result
func (c *Collector) ProcessFiles(files []string, filetype string) error {
	c.timing.StartParsing()

	// Process each input file
	for _, inputFile := range files {
		if c.verbose {
			fmt.Printf("Loading %s file: %s\n", filetype, inputFile)
		}

		var err error
		if filetype == "csv" {
			err = LoadCSVFile(inputFile, c)
		} else {
			err = LoadPcap(inputFile, c)
		}

		if err != nil {
			return fmt.Errorf("failed to load %s file %s: %w", filetype, inputFile, err)
		}
	}

	c.timing.StopParsing()

	c.finalise()

	c.filesLoaded = files

	return nil
}
