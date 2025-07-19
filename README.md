# DNS Magnitude HLL Toolkit


## Terminology

- A _dataset_ is a CBOR-encoded dataset containing binary HLLs for all observed clients as well as one HLL for each observed TLD.
- A _report_ is a JSON-encoded report listing all observed TLDs and their respective magnitude score.

## Components

### Collector

The _collector_ is used to create a CBOR-encoded dataset based on input data in any of the following formats:

- [PCAP](https://en.wikipedia.org/wiki/Pcap) and GZIPed PCAP
- [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) text files (domain, client IP address)
- [C-DNS](https://datatracker.ietf.org/doc/html/rfc8618) (Compacted-DNS)

Input data is read from files specified on the command line.

Unique clients will be collected all queries where the rightmost DNS label is a possible top-level domain. This corresponds to the following regular expression:

    ^(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})$

#### Example Usage

    dnsmag collect --output data.cbor --top 2500 *.pcap

### Aggregator

The _aggregator_ is used to merge multiple set of datasets into a single dataset.

#### Example Usage

    dnsmag aggregate --output aggregate.cbor --top 2500 *.cbor

### Reporter

The _reporter_ creates a JSON formatted DNS Magnitude report from a dataset.

#### Example Usage

    dnsmag report --top 2500 --output report.json data.cbor


## HyperLogLog

Datasets contains HyperLogLog (HLL) data of observed clients. The HLLs are created using the parameters found below.

### HLL Encoding

HLLs are encoded per [Aggregate Knowledge HLL Storage specification](https://github.com/aggregateknowledge/hll-storage-spec) with the following parameters:

- log2m: 10
- Regwidth: 4
- Explicit Threshold: AutoExplicitThreshold
- Sparse Enabled: True

### HLL Hash Function

The hash function used for HLL is [XXH3](https://xxhash.com/) 64-bits with truncated binary IPv6 addresses (128 bits) as input. IPv4 addresses encoded as IPv4-mapped IPv6 addresses ([RFC 4291 section 2.5.5.2](https://datatracker.ietf.org/doc/html/rfc4291.html#section-2.5.5.2)).

#### IPv4 Test Vectors

- Input address “192.0.2.1”
- Truncated to 24 bits yields “192.0.2.0”
- Encoded as IPv6 address “::ffff:192.0.2.0”
- Hash input  “00 00 00 00 00 00 00 00 00 00 ff ff c0 00 02 00”
- XXH3 64-bit hash output “b1 5c e9 49 ae 6f 33 12”

#### IPv6 Test Vectors

- Input address “2001:503:ba3e::2:30”
- Truncate to 48 bits yields “2001:503:ba3e::”
- Hash input “20 01 05 03 ba 3e 00 00 00 00 00 00 00 00 00 00”.
- XXH3 64-bit hash output “1a 82 86 59 2f 9f 36 6d”
