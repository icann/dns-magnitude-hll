package internal

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/bits"
	"os"
	"testing"
	"time"
)

func init() {
	InitStats()
}

func parseAddress(t *testing.T, ipStr, expectedHashInput string, expectedHash uint64, verbose bool) IPAddress {
	ip, err := NewIPAddressFromString(ipStr)
	if err != nil {
		t.Fatalf("Failed to parse IP address %s: %v", ipStr, err)
	}

	if hex.EncodeToString((ip.hashInput[:])) != expectedHashInput {
		t.Errorf("XXH3 hash input of %s = %s, want %s", ipStr, hex.EncodeToString(ip.hashInput[:]),
			expectedHashInput)
	}

	if ip.hash != expectedHash {
		t.Errorf("XXH3 hash of %s = 0x%x, want 0x%x", expectedHashInput, ip.hash, expectedHash)
	}

	if verbose {
		fmt.Printf("XXH3 hash of %s: 0x%x\n", expectedHashInput, ip.hash)
	}

	return ip
}

func showHllCalculation(ip IPAddress, hll *HLLWrapper) {
	hll.AddRaw(ip.hash)

	// Manually go through all the HLL calculations to get test vectors
	settings := hll.Settings()

	mBitsMask := uint64((1 << settings.Log2m) - 1)
	// amazingly elaborate way to cast regwidth to uint64 without linter complaints
	regWidth64 := uint64(0)
	if settings.Regwidth > 0 {
		regWidth64 = uint64(settings.Regwidth)
	}
	maxRegisterValue := uint64(1<<regWidth64) - 1
	pwMaxMask := ^uint64((1 << uint64(maxRegisterValue-1)) - 1)

	// Calculate index (LSB-first)
	index := uint64(ip.hash & mBitsMask)

	// Calculate substream value (upper bits after removing index bits)
	substreamValue := ip.hash >> settings.Log2m

	// Calculate position (pW) using trailing zeros. pWMaxMask prevents overflow.
	position := byte(1 + bits.TrailingZeros64(substreamValue|pwMaxMask))

	// Create a string here first to be able to align binary output below
	indexStr := fmt.Sprintf("%d (0x%x) (bin: ", index, index)

	// Format output using printTable
	rows := []TableRow{
		{"XXH3 Hashing", ""},
		{"IP address", ip.ipAddress.String()},
		{"Truncated IP", ip.truncatedIP.String()},
		{"Hash input", hex.EncodeToString(ip.hashInput[:])},
		{"Hash output", fmt.Sprintf("0x%x", ip.hash)},
		{"", ""},
		{"HLL Settings", ""},
		{"log2m", fmt.Sprintf("%d", settings.Log2m)},
		{"regwidth", fmt.Sprintf("%d", settings.Regwidth)},
		{"", ""},
		{"HLL computation", ""},
		{"Value (hex)", fmt.Sprintf("%016x", ip.hash)},
		{"Value (bin)", fmt.Sprintf("%064b", ip.hash)},
		{"Index (LSB)", fmt.Sprintf("%-49s %014b)", indexStr, index)},
		{"Substream value (bin)", fmt.Sprintf("%050b", substreamValue)},
		{"Max mask (bin)", fmt.Sprintf("%05b", pwMaxMask)},
		{"Trailing zeros", fmt.Sprintf("%d (from substream | max mask)", bits.TrailingZeros64(substreamValue|pwMaxMask))},
		{"Position", fmt.Sprintf("%d (trailing zeros + 1)", position)},
		{"", ""},
		{"HLL encoded as bytes", fmt.Sprintf("%x", hll.ToBytes())},
	}

	printTable(os.Stdout, rows)
}

func TestInteropVectors(t *testing.T) {
	tests := []struct {
		name              string
		ipStr             string
		expectedHashInput string
		expectedHash      uint64
	}{
		{
			name:              "IPv4 address",
			ipStr:             "192.0.2.1",
			expectedHashInput: "00000000000000000000ffffc0000200",
			expectedHash:      0xb15ce949ae6f3312,
		},
		{
			name:              "second IPv4 address",
			ipStr:             "192.168.1.1",
			expectedHashInput: "00000000000000000000ffffc0a80100",
			expectedHash:      0x39ca3847248ef94e,
		},
		{
			name:              "IPv6 address",
			ipStr:             "2001:503:ba3e::2:30",
			expectedHashInput: "20010503ba3e00000000000000000000",
			expectedHash:      0x1a8286592f9f366d,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := parseAddress(t, tt.ipStr, tt.expectedHashInput, tt.expectedHash, false)

			dataset := newDataset(nil)
			showHllCalculation(ip, dataset.AllClientsHll)
		})
	}
}

func TestInteropOneIP(t *testing.T) {
	domain := "example.com"

	ipStr := "192.0.2.1"
	expectedHashInput := "00000000000000000000ffffc0000200"
	expectedHash := uint64(0xb15ce949ae6f3312)
	src := parseAddress(t, ipStr, expectedHashInput, expectedHash, true)

	// Create dataset and update stats
	dataset := newDataset(nil)
	dataset.updateStats(domain, src, 1, false)
	dataset.finaliseStats()

	if dataset.AllQueriesCount != 1 {
		t.Errorf("AllQueriesCount = %d, want 1", dataset.AllQueriesCount)
	}

	hllBytes := dataset.AllClientsHll.ToBytes()

	expectedHex := "138e40cc4860"
	expectedBytes, err := hex.DecodeString(expectedHex)
	if err != nil {
		t.Fatalf("failed to decode expected hex: %v", err)
	}
	if !bytes.Equal(hllBytes, expectedBytes) {
		t.Errorf("AllClientsHll bytes = %x, want %x", hllBytes, expectedBytes)
	}

	fmt.Printf("AllClientsHll bytes: %x\n", hllBytes)
}

func TestInteropTwoIPs(t *testing.T) {
	domain := "example.com"

	ip4Str := "192.0.2.1"
	expectedHashInput4 := "00000000000000000000ffffc0000200"
	expectedHash4 := uint64(0xb15ce949ae6f3312)

	ip4 := parseAddress(t, ip4Str, expectedHashInput4, expectedHash4, true)

	ip6Str := "2001:503:ba3e::2:30"
	expectedHashInput6 := "20010503ba3e00000000000000000000"
	expectedHash6 := uint64(0x1a8286592f9f366d)
	ip6 := parseAddress(t, ip6Str, expectedHashInput6, expectedHash6, true)

	// Create dataset and update stats with both IPs
	dataset := newDataset(nil)
	dataset.updateStats(domain, ip4, 1, false)
	dataset.updateStats(domain, ip6, 1, false)
	dataset.finaliseStats()

	if dataset.AllQueriesCount != 2 {
		t.Errorf("AllQueriesCount = %d, want 2", dataset.AllQueriesCount)
	}

	hllBytes := dataset.AllClientsHll.ToBytes()

	expectedHex := "138e40cc487b368c"
	expectedBytes, err := hex.DecodeString(expectedHex)
	if err != nil {
		t.Fatalf("failed to decode expected hex: %v", err)
	}
	if !bytes.Equal(hllBytes, expectedBytes) {
		t.Errorf("AllClientsHll bytes = %x, want %x", hllBytes, expectedBytes)
	}

	fmt.Printf("AllClientsHll bytes: %x\n", hllBytes)
}

func TestInteropCollectorIntegration(t *testing.T) {
	// Test using the collector interface to ensure consistency
	testDate := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	timing := NewTimingStats()
	collector := NewCollector(DefaultDomainCount, 0, false, &testDate, timing)

	// Add the same IPs as in TestInteropTwoIPs
	ip4, err := NewIPAddressFromString("192.0.2.1")
	if err != nil {
		t.Fatalf("Failed to parse IPv4 address: %v", err)
	}

	ip6, err := NewIPAddressFromString("2001:503:ba3e::2:30")
	if err != nil {
		t.Fatalf("Failed to parse IPv6 address: %v", err)
	}

	err = collector.ProcessRecord("example.com", ip4, 1)
	if err != nil {
		t.Fatalf("Failed to process IPv4 record: %v", err)
	}

	err = collector.ProcessRecord("example.com", ip6, 1)
	if err != nil {
		t.Fatalf("Failed to process IPv6 record: %v", err)
	}

	collector.Finalise()
	dataset := collector.Result

	if dataset.AllQueriesCount != 2 {
		t.Errorf("AllQueriesCount = %d, want 2", dataset.AllQueriesCount)
	}

	// Verify HLL bytes match expected interop values
	hllBytes := dataset.AllClientsHll.ToBytes()
	expectedHex := "138e40cc487b368c"
	expectedBytes, err := hex.DecodeString(expectedHex)
	if err != nil {
		t.Fatalf("failed to decode expected hex: %v", err)
	}
	if !bytes.Equal(hllBytes, expectedBytes) {
		t.Errorf("Collector AllClientsHll bytes = %x, want %x", hllBytes, expectedBytes)
	}

	fmt.Printf("Collector AllClientsHll bytes: %x\n", hllBytes)
}
