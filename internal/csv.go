// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func LoadCSVFromReader(reader io.Reader, collector *Collector, filetype string) error {
	reader1, err := getReader(reader)
	if err != nil {
		return fmt.Errorf("failed to get reader: %w", err)
	}

	// choose delimiter: if filetype == "tsv" use tab, otherwise default to comma
	delimiter := ','
	if filetype == "tsv" {
		delimiter = '\t'
	}

	csvReader := csv.NewReader(reader1)
	csvReader.Comment = '#'
	csvReader.TrimLeadingSpace = true
	csvReader.FieldsPerRecord = -1 // allow either 2 or 3 fields per record
	csvReader.Comma = delimiter    // use configured or overridden delimiter
	csvReader.LazyQuotes = true    // be forgiving with quotes

	firstLine := true

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			collector.invalidRecordCount++
			continue
		}

		if err := processCSVRecord(collector, record, firstLine); err != nil {
			line, _ := csvReader.FieldPos(0)
			return fmt.Errorf("failed to process CSV record at line %d: %w", line, err)
		}
		firstLine = false
	}

	return nil
}

// Get a reader from a file. If the file is gzipped, it will return a gzip reader.
// This code is borrowed from the gopacket library (pcapgo).
func getReader(reader io.Reader) (io.Reader, error) {
	// Check if the file is gzipped by reading the first two bytes.
	br := bufio.NewReader(reader)
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

// processCSVRecord processes a single CSV record
func processCSVRecord(collector *Collector, record []string, firstLine bool) error {
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
			if firstLine {
				// Special case: if the first line has an invalid client IP,
				// it might be a header row. Skip it silently.
				return nil
			}
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
		if firstLine {
			// Special case: if the first line has an invalid client IP,
			// it might be a header row. Skip it silently.
			return nil
		}
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
