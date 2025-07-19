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

// Main data structure for storing domain statistics. This matches the structure of the CBOR files.
type MagnitudeDataset struct {
	Version           uint16                   `cbor:"version"`
	Date              *TimeWrapper             `cbor:"date"`			   // UTC date of collection
	GlobalHll         *HLLWrapper              `cbor:"global_hll"`	       // HLL for all unique source IPs
	AllClientsCount   uint64                   `cbor:"all_clients_count"`  // Cardinality of GlobalHll
	AllQueriesCount   uint64                   `cbor:"all_queries_count"`
	Domains           map[DomainName]domainHll `cbor:"domains"`
	extraAllClients   map[netip.Addr]struct{}  // All clients, only used when printing stats in collect command
	extraV6Clients    map[netip.Addr]struct{}  // IPv6 clients, only used when printing stats in collect command
	extraDomainsCount uint				   	   // Number of unique domains before any truncation
}

// Per-domain data
type domainHll struct {
	Domain          DomainName  `cbor:"domain"`        // Domain name
	Hll             *HLLWrapper `cbor:"hll"`           // HLL counter for unique source IPs
	ClientsCount    uint64      `cbor:"clients_count"` // Number of clients querying this domain (cardinality of HLL)
	QueriesCount    uint64      `cbor:"queries_count"` // Number of queries for this domain (absolute count)
	extraAllClients map[netip.Addr]struct{}			   // All clients, only used when printing stats to stdout
}

// Used to make a list of domains by count
type domainMagnitude struct {
	Domain    DomainName
	Magnitude float64
	DomainHll *domainHll
}

func InitStats() {
	// initialise the HLL defaults to not have to specify them every time we create a new HLL
	hll.Defaults(hll.Settings{
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
		GlobalHll:         &HLLWrapper{Hll: &hll.Hll{}},
		Domains:           make(map[DomainName]domainHll),
		AllClientsCount:   0,
		AllQueriesCount:   0,
		extraAllClients:   make(map[netip.Addr]struct{}),
		extraV6Clients:    make(map[netip.Addr]struct{}),
		extraDomainsCount: 0,
	}
}

func newDomain(domain DomainName) domainHll {
	return domainHll{
		Domain:          domain,
		Hll:             &HLLWrapper{Hll: &hll.Hll{}},
		ClientsCount:    0,
		QueriesCount:    0,
		extraAllClients: make(map[netip.Addr]struct{}),
	}
}

func (ds *MagnitudeDataset) SortedByMagnitude() []domainMagnitude {
	var sorted []domainMagnitude

	for _, this := range ds.Domains {
		numSrcIPs := this.ClientsCount

		magnitude := (math.Log(float64(numSrcIPs)) / math.Log(float64(ds.AllClientsCount))) * 10

		sorted = append(sorted, domainMagnitude{this.Domain, magnitude, &this})
	}

	slices.SortFunc(sorted, func(a, b domainMagnitude) int {
		return int(a.Magnitude*1000) - int(b.Magnitude*1000)
	})

	return sorted
}

// keeps only the top N domains by magnitude
func (ds *MagnitudeDataset) Truncate(maxDomains int) error {
	if maxDomains <= 0 || len(ds.Domains) <= maxDomains {
		return nil // Nothing to truncate
	}

	sorted := ds.SortedByMagnitude()
	idx := max(len(sorted) - maxDomains, 0)

	topDomains := sorted[idx:]

	res := make(map[DomainName]domainHll)
	for _, dm := range topDomains {
		res[dm.Domain] = *dm.DomainHll
	}

	ds.Domains = res
	return nil
}

// count a query for a domain and source IP address.
func (stats *MagnitudeDataset) updateStats(domain DomainName, src IPAddress) {
	if domain == "" {
		return
	}

	// Ensure domainHll exists for this domain
	dh, found := stats.Domains[domain]
	if !found {
		dh = newDomain(domain)
	}

	// Add the source IP to the set of unique source IPs. This set is only for validation
	// during development, and should be considered for removal later.
	stats.extraAllClients[src.truncatedIP] = struct{}{}
	dh.extraAllClients[src.truncatedIP] = struct{}{}
	if src.ipAddress.Is6() {
		stats.extraV6Clients[src.truncatedIP] = struct{}{}
	}

	// Increase queriesCount
	dh.QueriesCount++
	stats.AllQueriesCount++

	// count IP in the two HyperLogLogs
	dh.Hll.AddRaw(src.hash)
	stats.GlobalHll.AddRaw(src.hash)

	// Save updated domainHll back to the map
	stats.Domains[domain] = dh
}

// update the clientsCount for each domain and the global clientsCount after all queries have been processed.
func (stats *MagnitudeDataset) finaliseStats() {
	// for each domain, update the clientsCount with cardinality of the HyperLogLog
	for domain, dh := range stats.Domains {
		dh.ClientsCount = dh.Hll.Cardinality()
		stats.Domains[domain] = dh
	}
	// Update the global clientsCount
	stats.AllClientsCount = stats.GlobalHll.Cardinality()
	// Store number of unique domains before any truncation
	stats.extraDomainsCount = uint(len(stats.Domains))
}

func (stats *MagnitudeDataset) DateString() string {
	return stats.Date.Time.Format(time.DateOnly)
}

func AggregateDatasets(datasets []MagnitudeDataset, minDomains int) (MagnitudeDataset, error) {
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
		res.GlobalHll.StrictUnion(*dataset.GlobalHll.Hll)
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
			this.Hll.StrictUnion(*domainData.Hll.Hll)

			res.Domains[domain] = this
		}
	}

	res.finaliseStats()

	return res, nil
}
