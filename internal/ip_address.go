// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"net/netip"

	"github.com/zeebo/xxh3"
)

// IPAddress holds an IP address, its truncated version, and a hash of the truncated version.
type IPAddress struct {
	ipAddress   netip.Addr
	truncatedIP netip.Addr
	hashInput   [16]byte // 128 bits, 16 bytes
	hash        uint64
}

// NewIPAddress creates an IPAddress from a netip.Addr
func NewIPAddress(addr netip.Addr) (IPAddress, error) {
	return newIPAddress(addr, DefaultIPv4MaskLength, DefaultIPv6MaskLength)
}

// Internal function to make incorrect IP address handling testable
func newIPAddress(addr netip.Addr, v4mask, v6mask int) (IPAddress, error) {
	var truncated netip.Addr
	if addr.Is4() {
		prefix, err := addr.Prefix(v4mask)
		if err != nil {
			return IPAddress{}, fmt.Errorf("invalid IPv4 address: %w", err)
		}
		truncated = prefix.Addr()
	} else if addr.Is6() {
		prefix, err := addr.Prefix(v6mask)
		if err != nil {
			return IPAddress{}, fmt.Errorf("invalid IPv6 address: %w", err)
		}
		truncated = prefix.Addr()
	} else {
		// Don't think we can actually get here. Maybe from a malformed PCAP?
		return IPAddress{}, fmt.Errorf("invalid IP address: not IPv4 or IPv6")
	}
	hashInput := truncated.As16()
	hash := xxh3.Hash(hashInput[:])
	return IPAddress{
		ipAddress:   addr,
		truncatedIP: truncated,
		hashInput:   hashInput,
		hash:        hash,
	}, nil
}

// NewIPAddressFromString creates an IPAddress from a string
func NewIPAddressFromString(s string) (IPAddress, error) {
	addr, err := netip.ParseAddr(s)
	if err != nil {
		return IPAddress{}, fmt.Errorf("invalid IP address string '%s': %w", s, err)
	}
	return NewIPAddress(addr)
}
