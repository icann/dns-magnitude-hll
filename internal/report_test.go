package internal

import (
	"reflect"
	"testing"
)

func init() {
	InitStats()
}

func TestGenerateReport_HardcodedComparison(t *testing.T) {
	csvData := `192.168.1.10,example.com,5
192.168.2.20,example.org,3
10.0.0.5,example.com,2`

	collector, err := loadDatasetFromCSV(csvData, "2007-09-09", true)
	if err != nil {
		t.Fatalf("loadDatasetFromCSV failed: %v", err)
	}
	dataset := collector.Result

	actual := GenerateReport(dataset, "test-source", "authoritative")

	expected := Report{
		Date:               "2007-09-09",
		Identifier:         actual.Identifier,
		Generator:	    "dnsmag undefined",
		Source:             "test-source",
		SourceType:         "authoritative",
		TotalUniqueClients: 4,
		TotalQueryVolume:   10,
		MagnitudeData: []MagnitudeData{
			{
				Domain:        "org",
				Magnitude:     5,
				UniqueClients: 2,
				QueryVolume:   3,
			},
			{
				Domain:        "com",
				Magnitude:     7.92481250360578,
				UniqueClients: 3,
				QueryVolume:   7,
			},
		},
	}

	// Compare using reflect.DeepEqual
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Report mismatch:\nExpected: %+v\nActual: %+v", expected, actual)

		// Provide more detailed comparison for debugging
		if actual.Date != expected.Date {
			t.Errorf("Date: expected %s, got %s", expected.Date, actual.Date)
		}
		if actual.Source != expected.Source {
			t.Errorf("Source: expected %s, got %s", expected.Source, actual.Source)
		}
		if actual.SourceType != expected.SourceType {
			t.Errorf("SourceType: expected %s, got %s", expected.SourceType, actual.SourceType)
		}
		if actual.TotalQueryVolume != expected.TotalQueryVolume {
			t.Errorf("TotalQueryVolume: expected %d, got %d", expected.TotalQueryVolume, actual.TotalQueryVolume)
		}
		if !reflect.DeepEqual(actual.MagnitudeData, expected.MagnitudeData) {
			t.Errorf("MagnitudeData mismatch:\nExpected: %+v\nActual: %+v", expected.MagnitudeData, actual.MagnitudeData)
		}
	}
}
