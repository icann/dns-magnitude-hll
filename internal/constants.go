// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import "regexp"

// Default number of top domains to collect/require
const DefaultDomainCount = 2500

// Number of labels in a DNS domain name to keep. Use 1 for just the TLD.
const DefaultDNSDomainNameLabels = 1

// Default number of (million) queries collected after which to aggregate results (to preserve memory)
const DefaultCollectDomainsChunk = 0

// IP address truncation mask lengths
const (
	DefaultIPv4MaskLength = 24
	DefaultIPv6MaskLength = 48
)

// regex for domain name validation. Pre-compiled for performance.
var DomainNameRegex = regexp.MustCompile("^(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})$")
