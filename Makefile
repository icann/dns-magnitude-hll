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
	./$(BINARY_NAME) collect --filetype pcap -o testdata/test1.cbor testdata/test1.pcap.gz
	./$(BINARY_NAME) aggregate -o testdata/aggregate.cbor.tmp testdata/test1.cbor
	./$(BINARY_NAME) report -s test -o testdata/report.json.tmp testdata/aggregate.cbor.tmp
	cat testdata/report.json.tmp

interop_vectors:
	go test -v -run ^TestInteropVector ./internal/

clean:
	rm -f $(BINARY_NAME)
	rm -f testdata/*.tmp
	go clean
