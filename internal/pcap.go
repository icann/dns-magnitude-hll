// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"net/netip"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
)

func LoadPcap(filename string) (MagnitudeDataset, time.Duration) {
	fmt.Printf("Loading pcap file: %s\n", filename)

	handle, err := pcap.OpenOffline(filename)
	if err != nil {
		panic(err)
	}
	defer handle.Close()

	start := time.Now()
	stats := processPackets(handle)
	elapsed := time.Since(start)
	return stats, elapsed
}

// Count DNS domain queries per domain and unique source IPs
func processPackets(handle *pcap.Handle) MagnitudeDataset {
	dataset := newDataset()
	dateSet := false

	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	for packet := range packetSource.Packets() {
		// Set the dataset date from the first packet's timestamp
		if !dateSet {
			packetTime := packet.Metadata().Timestamp
			dataset.Date = &TimeWrapper{Time: packetTime.UTC()}
			dateSet = true
		}

		if dnsLayer := packet.Layer(layers.LayerTypeDNS); dnsLayer != nil {
			dns, _ := dnsLayer.(*layers.DNS)

			src := extractSrcIP(packet)

			for _, this := range dns.Questions {
				name, err := getDomainName(string(this.Name), DefaultDNSDomainNameLabels)
				if err != nil {
					// TODO: Log/analyse skipped domain names?
					continue
				}

				dataset.updateStats(name, src)
			}
		}
	}

	dataset.finaliseStats()

	return dataset
}

// extractSrcIP extracts the source IP address from a packet as IPAddress (masked)
func extractSrcIP(packet gopacket.Packet) IPAddress {
	if ip4 := packet.Layer(layers.LayerTypeIPv4); ip4 != nil {
		ip := ip4.(*layers.IPv4).SrcIP
		if ip4 := ip.To4(); ip4 != nil {
			addr, _ := netip.AddrFromSlice(ip4)
			return newIPAddress(addr)
		}
	} else if ip6 := packet.Layer(layers.LayerTypeIPv6); ip6 != nil {
		ip := ip6.(*layers.IPv6).SrcIP
		if ip16 := ip.To16(); ip16 != nil {
			addr, _ := netip.AddrFromSlice(ip16)
			return newIPAddress(addr)
		}
	}
	panic("Source IP not found in packet")
}
