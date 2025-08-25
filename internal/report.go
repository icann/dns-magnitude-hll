// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"github.com/google/uuid"
)

type Report struct {
	Identifier         string          `json:"id"`
	Date               string          `json:"date"`
	Source             string          `json:"source"`
	SourceType         string          `json:"sourceType"`
	TotalUniqueClients uint64          `json:"totalUniqueClients"`
	TotalQueryVolume   uint64          `json:"totalQueryVolume"`
	MagnitudeData      []MagnitudeData `json:"magnitudeData"`
}

type MagnitudeData struct {
	Domain        string  `json:"domain"`
	Magnitude     float64 `json:"magnitude"`
	UniqueClients uint64  `json:"uniqueClients"`
	QueryVolume   uint64  `json:"queryVolume"`
}

// GenerateReport creates a JSON report from a MagnitudeDataset
func GenerateReport(stats MagnitudeDataset, source, sourceType string) Report {
	var magnitudeData []MagnitudeData

	sortedDomains := stats.SortedByMagnitude()

	for _, dm := range sortedDomains {
		magnitudeData = append(magnitudeData, MagnitudeData{
			Domain:        string(dm.Domain),
			Magnitude:     dm.Magnitude,
			UniqueClients: dm.DomainHll.ClientsCount,
			QueryVolume:   dm.DomainHll.QueriesCount,
		})
	}

	report := Report{
		Date:               stats.DateString(),
		Identifier:         uuid.New().String(),
		Source:             source,
		SourceType:         sourceType,
		TotalUniqueClients: stats.AllClientsCount,
		TotalQueryVolume:   stats.AllQueriesCount,
		MagnitudeData:      magnitudeData,
	}

	return report
}
