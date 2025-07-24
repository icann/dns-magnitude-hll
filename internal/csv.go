// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

func LoadCSVFile(filename string, date *time.Time) (MagnitudeDataset, time.Duration, error) {
	fmt.Printf("Loading CSV file: %s\n", filename)

	file, err := os.Open(filename)
	if err != nil {
		return MagnitudeDataset{}, 0, fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer file.Close()

	start := time.Now()
	dataset, err := LoadCSVFromReader(file, date)
	if err != nil {
		return MagnitudeDataset{}, 0, fmt.Errorf("failed to parse CSV: %w", err)
	}
	elapsed := time.Since(start)

	return dataset, elapsed, nil
}

func LoadCSVFromReader(reader io.Reader, date *time.Time) (MagnitudeDataset, error) {
	dataset := newDataset()

	// Set dataset date - use provided date or current time
	var datasetTime time.Time
	if date != nil {
		datasetTime = date.UTC()
	} else {
		datasetTime = time.Now().UTC()
	}
	dataset.Date = &TimeWrapper{Time: datasetTime}

	csvReader := csv.NewReader(reader)
	csvReader.Comment = '#'
	csvReader.TrimLeadingSpace = true
	csvReader.FieldsPerRecord = -1 // allow either 2 or 3 fields per record

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			line, _ := csvReader.FieldPos(0)
			return dataset, fmt.Errorf("failed to read CSV line %d: %w", line, err)
		}

		if err := processCSVRecord(&dataset, record); err != nil {
			line, _ := csvReader.FieldPos(0)
			return dataset, fmt.Errorf("failed to process CSV record at line %d: %w", line, err)
		}
	}

	dataset.finaliseStats()
	return dataset, nil
}

// processCSVRecord processes a single CSV record
func processCSVRecord(dataset *MagnitudeDataset, record []string) error {
	if len(record) < 2 {
		return fmt.Errorf("CSV record must have at least two fields (client, domain), got %d", len(record))
	}

	clientStr := strings.TrimSpace(record[0])
	domainStr := strings.TrimSpace(record[1])

	// Field 3 is an optional query count. Use 1 if not specified.
	var queryCount uint64 = 1
	if len(record) >= 3 && strings.TrimSpace(record[2]) != "" {
		parsed, err := strconv.Atoi(strings.TrimSpace(record[2]))
		if err != nil {
			return fmt.Errorf("invalid queries_count '%s': %w", record[2], err)
		}
		if parsed == 0 {
			return nil
		}
		if parsed < 0 {
			return fmt.Errorf("queries_count must be non-negative, got %d", parsed)
		}
		queryCount = uint64(parsed)
	}

	clientIP := newIPAddressFromString(clientStr)

	domainName, err := getDomainName(domainStr, DefaultDNSDomainNameLabels)
	if err != nil {
		// TODO: Log or otherwise handle errors?
		return nil
	}

	// Update statistics with the specified query count
	dataset.updateStats(domainName, clientIP, queryCount)

	return nil
}
