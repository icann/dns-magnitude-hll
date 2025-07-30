// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

// Default number of top domains to collect/require
const DefaultDomainCount = 2500

// Number of labels in a DNS domain name to keep. Use 1 for just the TLD.
const DefaultDNSDomainNameLabels = 1

// Default number of (million) queries collected after which to aggregate results (to preserve memory)
const DefaultCollectDomainsChunk = 0
