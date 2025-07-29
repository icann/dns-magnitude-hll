// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"math"
	"net/netip"
	"slices"
	"time"

	"github.com/segmentio/go-hll"
)

// Need to wrap the hll.Hll type to use custom CBOR marshalling/unmarshalling
type HLLWrapper struct {
	*hll.Hll
}

// TimeWrapper wraps time.Time to provide custom CBOR marshaling as tag 1004
type TimeWrapper struct {
	time.Time
}

// Main data structure for storing domain statistics. This matches the structure of the CBOR files.
type MagnitudeDataset struct {
	Version           uint16                   `cbor:"version"`
	Date              *TimeWrapper             `cbor:"date"`              // UTC date of collection
	AllClientsHll     *HLLWrapper              `cbor:"all_clients_hll"`   // HLL for all unique source IPs
	AllClientsCount   uint64                   `cbor:"all_clients_count"` // Cardinality of GlobalHll
	AllQueriesCount   uint64                   `cbor:"all_queries_count"`
	Domains           map[DomainName]domainHll `cbor:"domains"`
	extraAllClients   map[netip.Addr]struct{}  // All clients, only used when printing stats in collect command
	extraV6Clients    map[netip.Addr]struct{}  // IPv6 clients, only used when printing stats in collect command
	extraDomainsCount uint64                   // Number of unique domains before any truncation
}

// Per-domain data
type domainHll struct {
	Domain          DomainName              `cbor:"domain"`        // Domain name
	Hll             *HLLWrapper             `cbor:"clients_hll"`   // HLL counter for unique source IPs
	ClientsCount    uint64                  `cbor:"clients_count"` // Number of clients querying this domain (cardinality of HLL)
	QueriesCount    uint64                  `cbor:"queries_count"` // Number of queries for this domain (absolute count)
	extraAllClients map[netip.Addr]struct{} // All clients, only used when printing stats to stdout
}

// Used to make a list of domains by count
type DomainMagnitude struct {
	Domain    DomainName
	Magnitude float64
	DomainHll *domainHll
}

func InitStats() error {
	// initialise the HLL defaults to not have to specify them every time we create a new HLL
	return hll.Defaults(hll.Settings{
		Log2m:             14, // chosen for < 1% error rate (~0.81%)
		Regwidth:          5,  // 5 bits per register, should be fine for number of clients < 10**10
		ExplicitThreshold: 0,
		SparseEnabled:     true,
	})
}

func newDataset() MagnitudeDataset {
	// Get current date without time (start of day in UTC)
	now := time.Now().UTC()
	dateOnly := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	return MagnitudeDataset{
		Version:           1,
		Date:              &TimeWrapper{Time: dateOnly},
		AllClientsHll:     &HLLWrapper{Hll: &hll.Hll{}},
		Domains:           make(map[DomainName]domainHll),
		AllClientsCount:   0,
		AllQueriesCount:   0,
		extraAllClients:   make(map[netip.Addr]struct{}),
		extraV6Clients:    make(map[netip.Addr]struct{}),
		extraDomainsCount: 0,
	}
}

func newDomain(domain DomainName) domainHll {
	result := domainHll{
		Domain:          domain,
		Hll:             &HLLWrapper{Hll: &hll.Hll{}},
		ClientsCount:    0,
		QueriesCount:    0,
		extraAllClients: make(map[netip.Addr]struct{}),
	}

	return result
}

func (dataset *MagnitudeDataset) SortedByMagnitude() []DomainMagnitude {
	var sorted []DomainMagnitude

	for _, this := range dataset.Domains {
		numSrcIPs := this.ClientsCount

		magnitude := (math.Log(float64(numSrcIPs)) / math.Log(float64(dataset.AllClientsCount))) * 10

		sorted = append(sorted, DomainMagnitude{this.Domain, magnitude, &this})
	}

	slices.SortFunc(sorted, func(a, b DomainMagnitude) int {
		return int(a.Magnitude*1000) - int(b.Magnitude*1000)
	})

	return sorted
}

// keeps only the top N domains by magnitude
func (dataset *MagnitudeDataset) Truncate(maxDomains int) error {
	if maxDomains <= 0 || len(dataset.Domains) <= maxDomains {
		return nil // Nothing to truncate
	}

	sorted := dataset.SortedByMagnitude()
	idx := max(len(sorted)-maxDomains, 0)

	topDomains := sorted[idx:]

	res := make(map[DomainName]domainHll)
	for _, dm := range topDomains {
		res[dm.Domain] = *dm.DomainHll
	}

	dataset.Domains = res
	return nil
}

// count a query for a domain and source IP address.
func (dataset *MagnitudeDataset) updateStats(domain DomainName, src IPAddress, queryCount uint64, verbose bool) {
	if domain == "" || queryCount == 0 {
		return
	}

	// Ensure domainHll exists for this domain
	dh, found := dataset.Domains[domain]
	if !found {
		dh = newDomain(domain)
	}

	// Add the source IP to the set of unique source IPs only if verbose mode is enabled
	// since it uses quite a bit of memory
	if verbose {
		dataset.extraAllClients[src.truncatedIP] = struct{}{}
		dh.extraAllClients[src.truncatedIP] = struct{}{}
		if src.ipAddress.Is6() {
			dataset.extraV6Clients[src.truncatedIP] = struct{}{}
		}
	}

	// Increase queriesCount
	dh.QueriesCount += queryCount
	dataset.AllQueriesCount += queryCount

	// count IP in the two HyperLogLogs
	dh.Hll.AddRaw(src.hash)
	dataset.AllClientsHll.AddRaw(src.hash)

	// Save updated domainHll back to the map
	dataset.Domains[domain] = dh
}

// update the clientsCount for each domain and the global clientsCount after all queries have been processed.
func (dataset *MagnitudeDataset) finaliseStats() {
	// for each domain, update the clientsCount with cardinality of the HyperLogLog
	for domain, dh := range dataset.Domains {
		dh.ClientsCount = dh.Hll.Cardinality()
		dataset.Domains[domain] = dh
	}
	// Update the global clientsCount
	dataset.AllClientsCount = dataset.AllClientsHll.Cardinality()
	// Store number of unique domains before any truncation
	dataset.extraDomainsCount = uint64(len(dataset.Domains))
}

func (dataset *MagnitudeDataset) DateString() string {
	return dataset.Date.Format(time.DateOnly)
}

func AggregateDatasets(datasets []MagnitudeDataset) (MagnitudeDataset, error) {
	if len(datasets) < 2 {
		return MagnitudeDataset{}, fmt.Errorf("no datasets to aggregate")
	}

	// Verify all input datasets have the same version and date
	for i, dataset := range datasets {
		if dataset.Version != datasets[0].Version {
			e := fmt.Errorf("version mismatch: dataset %d has version %d, expected %d", i, dataset.Version, datasets[0].Version)
			return MagnitudeDataset{}, e
		}
		if dataset.DateString() != datasets[0].DateString() {
			e := fmt.Errorf("date mismatch: dataset %d has date %s, expected %s", i, dataset.DateString(), datasets[0].DateString())
			return MagnitudeDataset{}, e
		}
	}

	res := newDataset()
	res.Date = datasets[0].Date

	// Aggregate global HLL
	for _, dataset := range datasets {
		if err := res.AllClientsHll.StrictUnion(*dataset.AllClientsHll.Hll); err != nil {
			return MagnitudeDataset{}, fmt.Errorf("failed to union all clients HLL: %w", err)
		}
	}

	// Aggregate precise client counts, if present
	for _, dataset := range datasets {
		for clientIP := range dataset.extraAllClients {
			res.extraAllClients[clientIP] = struct{}{}
		}
		for clientIP := range dataset.extraV6Clients {
			res.extraV6Clients[clientIP] = struct{}{}
		}
	}

	// Aggregate domain-level statistics
	for _, dataset := range datasets {
		res.AllQueriesCount += dataset.AllQueriesCount

		for domain, domainData := range dataset.Domains {
			// Fetch or initialise domainHll
			this, found := res.Domains[domain]
			if !found {
				this = newDomain(domain)
			}
			this.QueriesCount += domainData.QueriesCount
			if err := this.Hll.StrictUnion(*domainData.Hll.Hll); err != nil {
				return MagnitudeDataset{}, fmt.Errorf("failed to union HLL for domain %s: %w", domain, err)
			}

			// Aggregate domain query client information, if present
			for clientIP := range domainData.extraAllClients {
				this.extraAllClients[clientIP] = struct{}{}
			}

			res.Domains[domain] = this
		}
	}

	res.finaliseStats()

	return res, nil
}
