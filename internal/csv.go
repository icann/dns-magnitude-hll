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
		return fmt.Errorf("failed to read CSV: %w", err)
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
	// Wrap reader so we can peek the first line and then re-compose the full stream.
	br := bufio.NewReader(reader)
	firstLine, err := br.ReadString('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to peek first line of CSV data: %w", err)
	}

	// Decide delimiter based on presence of a tab in the first line
	delimiter := ','
	if strings.Contains(firstLine, "\t") {
		delimiter = '\t'
	}

	// Recreate a reader that yields the first line we consumed followed by the remaining buffered data.
	fullReader := io.MultiReader(strings.NewReader(firstLine), br)

	csvReader := csv.NewReader(fullReader)
	csvReader.Comment = '#'
	csvReader.TrimLeadingSpace = true
	csvReader.FieldsPerRecord = -1    // allow either 2 or 3 fields per record
	csvReader.Comma = rune(delimiter) // set chosen delimiter

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

	// handle escaped octal/hex sequences like "\163\145" -> "se", "\x73\x65" -> "se"
	domainStr = unescapeDomain(domainStr)

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

	clientIP, err := NewIPAddressFromString(clientStr)
	if err != nil {
		return fmt.Errorf("invalid client IP address: %w", err)
	}

	// Update statistics with the specified query count
	if err := collector.ProcessRecord(domainStr, clientIP, queryCount); err != nil {
		return fmt.Errorf("failed to process record: %w", err)
	}

	return nil
}

// unescapeDomain decodes backslash-escaped octal and hex sequences in a domain string.
// Examples: "\163\145" -> "se", "\x73\x65" -> "se"
// Hex accepts only lowercase 'x' and 1-2 hex digits. Octal accepts 1-3 digits (0-7).
func unescapeDomain(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] != '\\' {
			b.WriteByte(s[i])
			i++
			continue
		}

		// s[i] == '\\'
		// If backslash is last char, emit it literally
		if i+1 >= len(s) {
			b.WriteByte('\\')
			i++
			continue
		}

		j := i + 1

		// Hex escape: \xHH (1-2 hex digits)
		if s[j] == 'x' {
			hexStart := j + 1
			hexEnd := hexStart
			for hexEnd < len(s) && hexEnd < hexStart+2 {
				ch := s[hexEnd]
				if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') {
					hexEnd++
				} else {
					break
				}
			}
			if hexEnd > hexStart {
				valStr := s[hexStart:hexEnd]
				if v, err := strconv.ParseInt(valStr, 16, 8); err == nil {
					b.WriteByte(byte(v))
					i = hexEnd
					continue
				}
			}
			// fallback: emit the 'x' literally
			b.WriteByte(s[j])
			i += 2
			continue
		}

		// Octal escape: up to 3 octal digits after backslash
		octStart := j
		octEnd := octStart
		for octEnd < len(s) && octEnd < octStart+3 && s[octEnd] >= '0' && s[octEnd] <= '7' {
			octEnd++
		}
		if octEnd > octStart {
			valStr := s[octStart:octEnd]
			if v, err := strconv.ParseInt(valStr, 8, 8); err == nil {
				b.WriteByte(byte(v))
				i = octEnd
				continue
			}
		}

		// No valid escape sequence found: emit the next char literally
		b.WriteByte(s[j])
		i += 2
	}
	return b.String()
}
