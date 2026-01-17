// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"math"
	"net/netip"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/go-hll"
)

// Need to wrap the hll.Hll type to use custom CBOR marshalling/un-marshalling
type HLLWrapper struct {
	*hll.Hll
}

// TimeWrapper wraps time.Time to provide custom CBOR marshaling as tag 1004
type TimeWrapper struct {
	time.Time
}

// Main data structure for storing domain statistics. This matches the structure of the CBOR files.
type MagnitudeDataset struct {
	Version             uint16                    `cbor:"version"`
	Identifier          string                    `cbor:"id"`                // Unique identifier of the dataset
	Generator           string                    `cbor:"generator"`         // Generator identifier (e.g., the software creating the dataset)
	Date                *TimeWrapper              `cbor:"date"`              // UTC date of collection
	AllClientsHll       *HLLWrapper               `cbor:"all_clients_hll"`   // HLL for all unique source IPs
	AllClientsCount     uint64                    `cbor:"all_clients_count"` // Cardinality of GlobalHll
	AllQueriesCount     uint64                    `cbor:"all_queries_count"`
	Domains             map[DomainName]domainData `cbor:"domains"`
	extraAllClients     map[netip.Addr]struct{}   // All clients, only used when printing stats in collect command
	extraV6Clients      map[netip.Addr]struct{}   // IPv6 clients, only used when printing stats in collect command
	extraAllDomains     map[DomainName]struct{}   // All domains before any truncation
	extraSourceFilename string                    // Source filename when loaded from file
}

// Per-domain data
type domainData struct {
	Hll             *HLLWrapper             `cbor:"clients_hll"`   // HLL counter for unique source IPs
	ClientsCount    uint64                  `cbor:"clients_count"` // Number of clients querying this domain (cardinality of HLL)
	QueriesCount    uint64                  `cbor:"queries_count"` // Number of queries for this domain (absolute count)
	extraAllClients map[netip.Addr]struct{} // All clients, only used when printing stats
}

// Used to make a list of domains by count
type DomainMagnitude struct {
	Domain    DomainName
	Magnitude float64
	DomainHll *domainData
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

func newDataset(date *time.Time) MagnitudeDataset {
	dataset := MagnitudeDataset{
		Version:             1,
		Identifier:          uuid.New().String(),
		Generator:           fmt.Sprintf("dnsmag %s", Version),
		AllClientsHll:       &HLLWrapper{Hll: &hll.Hll{}},
		Domains:             make(map[DomainName]domainData),
		AllClientsCount:     0,
		AllQueriesCount:     0,
		extraAllClients:     make(map[netip.Addr]struct{}),
		extraV6Clients:      make(map[netip.Addr]struct{}),
		extraAllDomains:     make(map[DomainName]struct{}),
		extraSourceFilename: "",
	}

	dataset.SetDate(date)
	return dataset
}

func newDomain() domainData {
	result := domainData{
		Hll:             &HLLWrapper{Hll: &hll.Hll{}},
		ClientsCount:    0,
		QueriesCount:    0,
		extraAllClients: make(map[netip.Addr]struct{}),
	}

	return result
}

func (dataset *MagnitudeDataset) SetDate(date *time.Time) {
	if date == nil {
		now := time.Now().UTC()
		date = &now
	}
	var dateOnly = time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	dataset.Date = &TimeWrapper{Time: dateOnly}
}

func (dataset *MagnitudeDataset) SortedByMagnitude() []DomainMagnitude {
	var sorted []DomainMagnitude

	for name, this := range dataset.Domains {
		numSrcIPs := this.ClientsCount

		magnitude := (math.Log(float64(numSrcIPs)) / math.Log(float64(dataset.AllClientsCount))) * 10

		sorted = append(sorted, DomainMagnitude{name, magnitude, &this})
	}

	slices.SortFunc(sorted, func(a, b DomainMagnitude) int {
		// First sort by magnitude
		magnitudeDiff := int(a.Magnitude*1000) - int(b.Magnitude*1000)
		if magnitudeDiff != 0 {
			return magnitudeDiff
		}
		// If magnitudes are equal, sort by domain name
		if a.Domain < b.Domain {
			return -1
		} else if a.Domain > b.Domain {
			return 1
		}
		// not reached as long as dataset.Domains is a map
		panic("Encountered two domains with the same name. Impossible.")
	})

	return sorted
}

// keeps only the top N domains by magnitude
func (dataset *MagnitudeDataset) Truncate(maxDomains int) {
	if maxDomains <= 0 || len(dataset.Domains) <= maxDomains {
		return // Nothing to truncate
	}

	sorted := dataset.SortedByMagnitude()
	idx := max(len(sorted)-maxDomains, 0)

	topDomains := sorted[idx:]

	res := make(map[DomainName]domainData)
	for _, dm := range topDomains {
		res[dm.Domain] = *dm.DomainHll
	}

	dataset.Domains = res
}

// count a query for a domain and source IP address.
func (dataset *MagnitudeDataset) updateStats(domainStr string, src IPAddress, queryCount uint64, verbose bool) error {
	if queryCount == 0 {
		return nil
	}

	// Count queries and unique clients in the global HyperLogLog
	dataset.AllQueriesCount += queryCount
	dataset.AllClientsHll.AddRaw(src.hash)

	// Record extra information only if verbose mode is enabled, to preserve memory
	if verbose {
		// Add the source IP to the set of unique source IPs
		dataset.extraAllClients[src.truncatedIP] = struct{}{}

		if src.ipAddress.Is6() {
			dataset.extraV6Clients[src.truncatedIP] = struct{}{}
		}
	}

	// Parse and validate domain name
	domainName, err := getDomainName(domainStr, DefaultDNSDomainNameLabels)
	if err != nil {
		return fmt.Errorf("invalid domain name: %w", err)
	}

	if domainName == "." {
		// Don't include root domain queries in the per-domain stats
		return nil
	}

	// Record extra information only if verbose mode is enabled, to preserve memory
	if verbose {
		// Track all domains before truncation
		dataset.extraAllDomains[domainName] = struct{}{}
	}

	// Fetch (or initialise) domainHll for this domain
	domain, found := dataset.Domains[domainName]
	if !found {
		domain = newDomain()
	}

	// Record extra information only if verbose mode is enabled, to preserve memory
	if verbose {
		// Add the source IP to the set of unique source IPs
		domain.extraAllClients[src.truncatedIP] = struct{}{}
	}

	// Count queries for this domain
	domain.QueriesCount += queryCount

	// Add source IP top the domain specific HyperLogLog
	domain.Hll.AddRaw(src.hash)

	// Save updated domainHll back to the map
	dataset.Domains[domainName] = domain

	return nil
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
	// extraAllDomains is already populated during updateStats
}

func (dataset *MagnitudeDataset) DateString() string {
	return dataset.Date.Format(time.DateOnly)
}

func AggregateDatasets(datasets []MagnitudeDataset) (MagnitudeDataset, error) {
	if len(datasets) < 2 {
		return MagnitudeDataset{}, fmt.Errorf("no datasets to aggregate")
	}

	// Verify all input datasets have the same version and date
	for _, dataset := range datasets {
		if dataset.Version != datasets[0].Version {
			e := fmt.Errorf("version mismatch: dataset %s has version %d, expected %d", dataset.extraSourceFilename, dataset.Version, datasets[0].Version)
			return MagnitudeDataset{}, e
		}
		if dataset.DateString() != datasets[0].DateString() {
			e := fmt.Errorf("date mismatch: dataset %s has date %s, expected %s", dataset.extraSourceFilename, dataset.DateString(), datasets[0].DateString())
			return MagnitudeDataset{}, e
		}
	}

	res := newDataset(&datasets[0].Date.Time)

	// Aggregate global HLL
	for _, dataset := range datasets {
		if err := res.AllClientsHll.StrictUnion(*dataset.AllClientsHll.Hll); err != nil {
			return MagnitudeDataset{}, fmt.Errorf("failed to union all clients HLL: %w", err)
		}
	}

	// Aggregate precise client and domain information, if present (only present during collection)
	for _, dataset := range datasets {
		for clientIP := range dataset.extraAllClients {
			res.extraAllClients[clientIP] = struct{}{}
		}
		for clientIP := range dataset.extraV6Clients {
			res.extraV6Clients[clientIP] = struct{}{}
		}
		for domain := range dataset.extraAllDomains {
			res.extraAllDomains[domain] = struct{}{}
		}
	}

	// Aggregate domain-level statistics
	for _, dataset := range datasets {
		res.AllQueriesCount += dataset.AllQueriesCount

		for domain, domainData := range dataset.Domains {
			// Fetch or initialise domainHll
			this, found := res.Domains[domain]
			if !found {
				this = newDomain()
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
