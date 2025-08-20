BINARY=		dnsmag


all: build

build: $(BINARY)

$(BINARY): app internal
	go build -o dnsmag ./app

test:
	go test -cover ./internal/ ./app/cmd/

interop_vectors:
	go test -v -run ^TestInteropVector ./internal/

clean:
	rm -f $(BINARY)
	go clean
