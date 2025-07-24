// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"net/netip"
	"os"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
)

func LoadPcap(filename string, date *time.Time) (MagnitudeDataset, time.Duration, error) {
	fmt.Printf("Loading pcap file: %s\n", filename)

	file, err := os.Open(filename)
	if err != nil {
		return MagnitudeDataset{}, 0, fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer file.Close()

	reader, err := pcapgo.NewReader(file)
	if err != nil {
		return MagnitudeDataset{}, 0, fmt.Errorf("failed to create pcap reader: %w", err)
	}

	start := time.Now()
	stats, err := processPackets(reader, date)
	if err != nil {
		return MagnitudeDataset{}, 0, fmt.Errorf("failed to process packets: %w", err)
	}
	elapsed := time.Since(start)
	return stats, elapsed, nil
}

// Count DNS domain queries per domain and unique source IPs
func processPackets(reader *pcapgo.Reader, date *time.Time) (MagnitudeDataset, error) {
	dataset := newDataset()
	dateSet := false

	if date != nil {
		dataset.Date = &TimeWrapper{Time: date.UTC()}
		dateSet = true
	}

	packetSource := gopacket.NewPacketSource(reader, reader.LinkType())
	for packet := range packetSource.Packets() {
		if !dateSet {
			// Set the dataset date from first packet's timestamp if no date was provided
			packetTime := packet.Metadata().Timestamp
			dataset.Date = &TimeWrapper{Time: packetTime.UTC()}
			dateSet = true
		}

		if dnsLayer := packet.Layer(layers.LayerTypeDNS); dnsLayer != nil {
			dns, _ := dnsLayer.(*layers.DNS)

			src, err := extractSrcIP(packet)
			if err != nil {
				// Skip packets without valid source IP
				continue
			}

			for _, this := range dns.Questions {
				name, err := getDomainName(string(this.Name), DefaultDNSDomainNameLabels)
				if err != nil {
					// TODO: Log/analyse skipped domain names?
					continue
				}

				dataset.updateStats(name, src, 1)
			}
		}
	}

	dataset.finaliseStats()

	return dataset, nil
}

// extractSrcIP extracts the source IP address from a packet as IPAddress (masked)
func extractSrcIP(packet gopacket.Packet) (IPAddress, error) {
	if ip4 := packet.Layer(layers.LayerTypeIPv4); ip4 != nil {
		ip := ip4.(*layers.IPv4).SrcIP
		if ip4 := ip.To4(); ip4 != nil {
			addr, _ := netip.AddrFromSlice(ip4)
			return newIPAddress(addr), nil
		}
	} else if ip6 := packet.Layer(layers.LayerTypeIPv6); ip6 != nil {
		ip := ip6.(*layers.IPv6).SrcIP
		if ip16 := ip.To16(); ip16 != nil {
			addr, _ := netip.AddrFromSlice(ip16)
			return newIPAddress(addr), nil
		}
	}
	return IPAddress{}, fmt.Errorf("source IP not found in packet")
}
