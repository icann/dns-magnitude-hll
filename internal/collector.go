// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"os"
	"runtime"
	"time"
)

type Collector struct {
	topCount  int
	chunkSize int
	verbose   bool
	current   MagnitudeDataset
	Result    MagnitudeDataset // Resulting dataset after processing
	count     int              // Count of processed records
	chunks    int              // Number of chunks processed
}

func NewCollector(topCount, chunkSize int, verbose bool, date *time.Time) *Collector {
	c := &Collector{
		topCount:  topCount,
		chunkSize: chunkSize,
		verbose:   verbose,
		current:   newDataset(),
		Result:    newDataset(),
		chunks:    0,
	}
	c.SetDate(date)
	return c
}

func (c *Collector) ProcessRecord(domain DomainName, src IPAddress, queryCount uint64) {
	c.current.updateStats(domain, src, queryCount, c.verbose)
	c.count++
	if c.chunkSize != 0 && c.count%c.chunkSize == 0 {
		c.migrateCurrent()
		c.chunks++
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
func (c *Collector) ProcessFiles(args []string, filetype string, timing *TimingStats) error {
	timing.StartParsing()

	// Process each input file
	for _, inputFile := range args {
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

	c.finalise()

	timing.StopParsing()

	return nil
}
