package internal

import (
	"net/netip"
	"testing"
)

func TestNewIPAddressFromString_TruncatedIP(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		// test cases
		{"IPv4 192.0.2.1", "192.0.2.1", "192.0.2.0"},
		{"IPv6 2001:503:ba3e::2:30", "2001:503:ba3e::2:30", "2001:503:ba3e::"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := newIPAddressFromString(tt.input)
			truncated, _ := netip.ParseAddr(tt.want)
			if ip.truncatedIP != truncated {
				t.Errorf("for %s, got truncated IP %s, want %s", tt.input, ip.truncatedIP, tt.want)
			}
		})
	}
}
