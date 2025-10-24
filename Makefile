BINARY_NAME=	dnsmag
VERSION=	$(shell git describe --tags --always)
LDFLAGS=	-X dnsmag/internal.Version=$(VERSION)


all: build

build: $(BINARY_NAME)

$(BINARY_NAME): app internal
	go build -ldflags="$(LDFLAGS)" -o $(BINARY_NAME) ./app

test:
	go test -cover ./internal/ ./app/cmd/

test2: $(BINARY_NAME)
	./$(BINARY_NAME) collect --date 2000-01-01 --filetype pcap -o testdata/test1.cbor testdata/test1.pcap.gz
	./$(BINARY_NAME) collect --date 2000-01-01 --filetype csv -o testdata/test2.cbor testdata/test2.csv.gz
	./$(BINARY_NAME) collect --date 2000-01-01 --filetype tsv -o testdata/test2-tsv.cbor testdata/test2.tsv
	./$(BINARY_NAME) collect --date 2000-01-01 --filetype tsv -o testdata/test3.cbor testdata/test3.tsv
	./$(BINARY_NAME) aggregate -o testdata/aggregate.cbor.tmp testdata/test*.cbor
	./$(BINARY_NAME) report -s test -o testdata/report.json.tmp testdata/aggregate.cbor.tmp
	cat testdata/report.json.tmp

interop_vectors:
	go test -v -run ^TestInteropVector ./internal/

release:
	goreleaser release --snapshot --clean

clean:
	rm -f $(BINARY_NAME)
	rm -f testdata/*.tmp
	go clean
