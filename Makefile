all: build

parse:
	go run ./cmd/dnsmag parse testdata/20240614-090128_300.pcap

view:
	go run ./cmd/dnsmag view testdata/20240614-090128_300.out

build:
	go build -o dnsmag-app ./app
