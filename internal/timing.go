// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"time"
)

// TimingStats tracks execution times for different phases of processing
type TimingStats struct {
	TotalStart     time.Time
	TotalElapsed   time.Duration
	ParsingStart   time.Time
	ParsingElapsed time.Duration
}

// NewTimingStats creates a new timing tracker
func NewTimingStats() *TimingStats {
	return &TimingStats{
		TotalStart: time.Now(),
	}
}

// StartParsing marks the beginning of the parsing phase
func (ts *TimingStats) StartParsing() {
	ts.ParsingStart = time.Now()
}

// StopParsing marks the end of the parsing phase
func (ts *TimingStats) StopParsing() {
	ts.ParsingElapsed = time.Since(ts.ParsingStart)
}

// Finish calculates the total elapsed time
func (ts *TimingStats) Finish() {
	ts.TotalElapsed = time.Since(ts.TotalStart)
}
