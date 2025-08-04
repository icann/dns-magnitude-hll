// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

func LoadCSVFile(filename string, collector *Collector) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer file.Close()

	reader, err := getReaderFromFile(file)
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	err = LoadCSVFromReader(reader, collector)
	if err != nil {
		return fmt.Errorf("failed to parse CSV: %w", err)
	}

	return nil
}

// Get a reader from a file. If the file is gzipped, it will return a gzip reader.
// This code is borrowed from the gopacket library (pcapgo).
func getReaderFromFile(file *os.File) (io.Reader, error) {
	// Check if the file is gzipped by reading the first two bytes.
	br := bufio.NewReader(file)
	gzipMagic, err := br.Peek(2)
	if err != nil {
		return nil, err
	}

	const magicGzip1 = 0x1f
	const magicGzip2 = 0x8b

	if gzipMagic[0] == magicGzip1 && gzipMagic[1] == magicGzip2 {
		return gzip.NewReader(br)
	}
	return br, nil
}

func LoadCSVFromReader(reader io.Reader, collector *Collector) error {
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
			collector.invalidRecordCount++
			continue
		}

		if err := processCSVRecord(collector, record); err != nil {
			collector.invalidRecordCount++
			line, _ := csvReader.FieldPos(0)
			return fmt.Errorf("failed to process CSV record at line %d: %w", line, err)
		}
	}

	return nil
}

// processCSVRecord processes a single CSV record
func processCSVRecord(collector *Collector, record []string) error {
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

	// Update statistics with the specified query count
	collector.ProcessRecord(domainStr, clientIP, queryCount)

	return nil
}
