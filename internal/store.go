// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/segmentio/go-hll"
)

// Marshal the HLLs in a MagnitudeDataset to CBOR format.
func (hw HLLWrapper) MarshalCBOR() ([]byte, error) {
	// Wrap the raw bytes in a CBOR binary encoding
	raw := hw.ToBytes()
	return cbor.Marshal(raw)
}

// UnmarshalCBOR decodes a CBOR-encoded []byte into an HLLWrapper.
func (hw *HLLWrapper) UnmarshalCBOR(data []byte) error {
	// First decode the CBOR-encoded []byte
	var raw []byte
	if err := cbor.Unmarshal(data, &raw); err != nil {
		return err
	}
	h, err := hll.FromBytes(raw)
	if err != nil {
		return err
	}
	hw.Hll = &h
	return nil
}

// Time is encoded as CBOR tag 1004 with string representation
func (tw TimeWrapper) MarshalCBOR() ([]byte, error) {
	tag := cbor.Tag{Number: 1004, Content: tw.Format(time.DateOnly)}
	return cbor.Marshal(tag)
}

func (tw *TimeWrapper) UnmarshalCBOR(data []byte) error {
	var tag cbor.Tag
	if err := cbor.Unmarshal(data, &tag); err != nil {
		return err
	}

	if tag.Number == 1004 {
		if dateStr, ok := tag.Content.(string); ok {
			if parsedDate, err := time.Parse(time.DateOnly, dateStr); err == nil {
				tw.Time = parsedDate
				return nil
			}
		}
	}

	return fmt.Errorf("unable to unmarshal TimeWrapper")
}

// WriteDNSMagFile writes the magnitudeDataset to a file in CBOR format.
// If filename is "-", writes to the provided stdout writer and returns "STDOUT".
func WriteDNSMagFile(stats MagnitudeDataset, filename string, stdout io.Writer) (string, error) {
	var file io.Writer
	var closeFunc func() error

	if filename == "-" {
		file = stdout
		closeFunc = func() error { return nil }
	} else {
		f, err := os.Create(filename)
		if err != nil {
			return "", err
		}
		file = f
		closeFunc = f.Close
	}
	defer func() { _ = closeFunc() }()

	enc := cbor.NewEncoder(file)
	err := enc.Encode(stats)
	if filename == "-" {
		return "STDOUT", err
	}
	return filename, err
}

// This structure is used when loading a sequence of datasets to avoid having them all in memory.
// Every loaded dataset is aggregated into the Result.
type DatasetSequence struct {
	numDomains int
	Count      int
	Result     MagnitudeDataset
	forceDate  bool
	logger     io.Writer
}

func NewDatasetSequence(numDomains int, date *time.Time, forceDate bool, logger io.Writer) *DatasetSequence {
	return &DatasetSequence{
		numDomains: numDomains,
		Count:      0,
		Result:     newDataset(date),
		forceDate:  forceDate,
		logger:     logger,
	}
}

// LoadDNSMagFile loads a magnitudeDataset from a CBOR file.
func (seq *DatasetSequence) LoadDNSMagFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	return seq.LoadDNSMagSequenceFromReader(file, fmt.Sprintf("%s#%%d", filename))
}

// LoadDNSMagSequenceFromReader loads all MagnitudeDatasets from a CBOR sequence reader.
// Sets extraSourceFilename to the filename plus a sequence number suffix for each dataset.
func (seq *DatasetSequence) LoadDNSMagSequenceFromReader(reader io.Reader, filenameFmt string) error {
	var buffer []byte
	readBuffer := make([]byte, 1024*1024) // 1MB read buffer to start with

	seqNum := 1
	for {
		// Try to read more data
		n, readErr := reader.Read(readBuffer)
		if n > 0 {
			buffer = append(buffer, readBuffer[:n]...)
		}

		// Try to unmarshal a dataset from the read buffer
		for len(buffer) > 0 {
			var this MagnitudeDataset

			remaining, err := cbor.UnmarshalFirst(buffer, &this)
			if err != nil {
				// If we can't unmarshal and have reached EOF, we fail
				if readErr == io.EOF {
					return fmt.Errorf("failed to unmarshal CBOR: %w", err)
				}
				// If we can't unmarshal but haven't hit EOF, we should read more data
				break
			}

			this.finaliseStats()
			this.extraSourceFilename = fmt.Sprintf(filenameFmt, seqNum)
			seqNum++

			if err := seq.addDataset(this); err != nil {
				return err
			}

			buffer = remaining
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("failed to read data: %w", readErr)
		}
	}

	// Check if there's any remaining data that couldn't be parsed
	if len(buffer) > 0 {
		return fmt.Errorf("remaining %d bytes in buffer could not be parsed as CBOR", len(buffer))
	}

	return nil
}

func (seq *DatasetSequence) addDataset(dataset MagnitudeDataset) error {
	// If forceDate is true and the dataset has a different date, log a warning and override it
	if seq.forceDate && dataset.Date != nil && seq.Result.Date != nil {
		if dataset.DateString() != seq.Result.DateString() {
			if seq.logger != nil {
				fmt.Fprintf(seq.logger, "Warning: Overriding date %s with forced date %s for dataset %s\n",
					dataset.DateString(), seq.Result.DateString(), dataset.extraSourceFilename)
			}
			dataset.SetDate(&seq.Result.Date.Time)
		}
	}

	if seq.Count == 0 {
		seq.Result = dataset
		seq.Count = 1
		return nil
	}

	aggregated, err := AggregateDatasets([]MagnitudeDataset{seq.Result, dataset})
	if err != nil {
		return fmt.Errorf("failed to aggregate datasets: %w", err)
	}

	// Truncate the stats to the top N domains
	aggregated.Truncate(seq.numDomains)

	seq.Result = aggregated
	seq.Count++

	return nil
}

// MarshalDatasetToCBOR marshals a dataset to CBOR bytes for testing
func MarshalDatasetToCBOR(dataset MagnitudeDataset) ([]byte, error) {
	return cbor.Marshal(dataset)
}
