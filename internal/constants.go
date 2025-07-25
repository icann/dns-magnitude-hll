// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

// Default number of top domains to collect/require
const DefaultDomainCount = 2500

// Number of labels in a DNS domain name to keep. Use 1 for just the TLD.
const DefaultDNSDomainNameLabels = 1

// Get accurate number of clients in the dataset, per domain and globally. This uses much more memory.
const ExtraDatasetInformation = false
