// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
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

// newIPAddress creates an IPAddress from a netip.Addr
func newIPAddress(addr netip.Addr) IPAddress {
	var truncated netip.Addr
	if addr.Is4() {
		prefix, _ := addr.Prefix(24)
		truncated = prefix.Addr()
	} else if addr.Is6() {
		prefix, _ := addr.Prefix(48)
		truncated = prefix.Addr()
	} else {
		panic("invalid IP address")
	}
	hashInput := truncated.As16()
	hash := xxh3.Hash(hashInput[:])
	return IPAddress{
		ipAddress:   addr,
		truncatedIP: truncated,
		hashInput:   hashInput,
		hash:        hash,
	}
}

// newIPAddress creates an IPAddress from a string
func newIPAddressFromString(s string) IPAddress {
	addr, error := netip.ParseAddr(s)
	if error != nil {
		panic("invalid IP address")
	}
	return newIPAddress(addr)
}
