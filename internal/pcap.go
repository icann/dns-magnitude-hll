// Author: Fredrik Thulin <fredrik@ispik.se>

package internal

import (
	"fmt"
	"io"
	"net/netip"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
)

func LoadPcap(reader io.Reader, collector *Collector) error {
	pcapReader, err := pcapgo.NewReader(reader)
	if err != nil {
		return fmt.Errorf("failed to create pcap reader: %w", err)
	}

	err = processPackets(pcapReader, collector)
	if err != nil {
		return fmt.Errorf("failed to process packets: %w", err)
	}

	return nil
}

// Count DNS domain queries per domain and unique source IPs
func processPackets(reader *pcapgo.Reader, collector *Collector) error {
	dateSet := false

	packetSource := gopacket.NewPacketSource(reader, reader.LinkType())
	for packet := range packetSource.Packets() {
		if !dateSet {
			// Set the dataset date from first packet's timestamp if no date was provided
			packetTime := packet.Metadata().Timestamp
			collector.SetDate(&packetTime)
			dateSet = true
		}

		if dnsLayer := packet.Layer(layers.LayerTypeDNS); dnsLayer != nil {
			dns, _ := dnsLayer.(*layers.DNS)

			src, err := extractSrcIP(packet)
			if err != nil {
				collector.invalidRecordCount++
				continue
			}

			for _, this := range dns.Questions {
				name := string(this.Name)

				if err := collector.ProcessRecord(name, src, 1); err != nil {
					return fmt.Errorf("failed to process record: %w", err)
				}
			}
		}
	}

	return nil
}

// extractSrcIP extracts the source IP address from a packet as IPAddress (masked)
func extractSrcIP(packet gopacket.Packet) (IPAddress, error) {
	if ip4 := packet.Layer(layers.LayerTypeIPv4); ip4 != nil {
		ip := ip4.(*layers.IPv4).SrcIP
		if ip4 := ip.To4(); ip4 != nil {
			addr, _ := netip.AddrFromSlice(ip4)
			return NewIPAddress(addr)
		}
	} else if ip6 := packet.Layer(layers.LayerTypeIPv6); ip6 != nil {
		ip := ip6.(*layers.IPv6).SrcIP
		if ip16 := ip.To16(); ip16 != nil {
			addr, _ := netip.AddrFromSlice(ip16)
			return NewIPAddress(addr)
		}
	}
	return IPAddress{}, fmt.Errorf("source IP not found in packet")
}
