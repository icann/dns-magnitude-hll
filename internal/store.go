// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"os"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/segmentio/go-hll"
)

// Marshal the HLLs in a MagnitudeDataset to CBOR format.
func (hw HLLWrapper) MarshalCBOR() ([]byte, error) {
	if hw.Hll == nil {
		return cbor.Marshal(nil)
	}
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
func WriteDNSMagFile(stats MagnitudeDataset, filename string) (string, error) {
	file, err := os.Create(filename)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()

	enc := cbor.NewEncoder(file)
	err = enc.Encode(stats)
	return filename, err
}

// LoadDNSMagFile loads a magnitudeDataset from a CBOR file.
func LoadDNSMagFile(filename string) (MagnitudeDataset, error) {
	var stats MagnitudeDataset

	file, err := os.Open(filename)
	if err != nil {
		return stats, err
	}
	defer func() { _ = file.Close() }()

	dec := cbor.NewDecoder(file)
	err = dec.Decode(&stats)

	stats.finaliseStats()

	return stats, err
}
