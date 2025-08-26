BINARY=		dnsmag
VERSION=	$(shell git describe --tags --always)

all: build

build: $(BINARY)


$(BINARY): app internal
	go build -ldflags="-X dnsmag/app/cmd.Version=$(VERSION)" -o $(BINARY) ./app

test:
	go test -cover ./internal/ ./app/cmd/

interop_vectors:
	go test -v -run ^TestInteropVector ./internal/

clean:
	rm -f $(BINARY)
	go clean
