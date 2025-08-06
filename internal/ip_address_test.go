package internal

import (
	"net/netip"
	"strings"
	"testing"
)

func TestNewIPAddressFromString_TruncatedIP(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		want        string
		expectError bool
	}{
		// test cases
		{"IPv4 192.0.2.1", "192.0.2.1", "192.0.2.0", false},
		{"IPv6 2001:503:ba3e::2:30", "2001:503:ba3e::2:30", "2001:503:ba3e::", false},
		{"Invalid IP", "not-an-ip", "", true},
		{"Empty string", "", "", true},
		{"Invalid IPv4", "192.168.1.256", "", true},
		{"Invalid IPv6", "2001:db8::gggg", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip, err := NewIPAddressFromString(tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for input %s, but got none", tt.input)
				}
				return
			}

			if err != nil {
				t.Fatalf("newIPAddressFromString failed: %v", err)
			}
			truncated, _ := netip.ParseAddr(tt.want)
			if ip.truncatedIP != truncated {
				t.Errorf("for %s, got truncated IP %s, want %s", tt.input, ip.truncatedIP, tt.want)
			}
		})
	}
}

func TestNewIPAddress_InvalidMasks(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		v4mask      int
		v6mask      int
		expectError bool
		errorMsg    string
	}{
		{
			name:        "invalid IPv4 mask too high",
			input:       "192.168.1.1",
			v4mask:      33, // Invalid for IPv4 (max is 32)
			v6mask:      48,
			expectError: true,
			errorMsg:    "invalid IPv4 address",
		},
		{
			name:        "invalid IPv4 mask negative",
			input:       "192.168.1.1",
			v4mask:      -1, // Invalid negative mask
			v6mask:      48,
			expectError: true,
			errorMsg:    "invalid IPv4 address",
		},
		{
			name:        "invalid IPv6 mask too high",
			input:       "2001:db8::1",
			v4mask:      24,
			v6mask:      129, // Invalid for IPv6 (max is 128)
			expectError: true,
			errorMsg:    "invalid IPv6 address",
		},
		{
			name:        "invalid IPv6 mask negative",
			input:       "2001:db8::1",
			v4mask:      24,
			v6mask:      -1, // Invalid negative mask
			expectError: true,
			errorMsg:    "invalid IPv6 address",
		},
		{
			name:        "valid masks",
			input:       "192.168.1.1",
			v4mask:      24,
			v6mask:      48,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, err := netip.ParseAddr(tt.input)
			if err != nil {
				t.Fatalf("Failed to parse test IP: %v", err)
			}

			result, err := newIPAddress(addr, tt.v4mask, tt.v6mask)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for invalid mask, but got none")
					return
				}
				if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain '%s', got: %v", tt.errorMsg, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for valid masks: %v", err)
					return
				}
				if result.ipAddress != addr {
					t.Errorf("Expected original IP to be preserved")
				}
			}
		})
	}
}
