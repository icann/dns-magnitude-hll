BINARY_NAME=	dnsmag
VERSION=	$(shell git describe --tags --always)
LDFLAGS=	-X dnsmag/internal.Version=$(VERSION)


all: build

build: $(BINARY_NAME)

$(BINARY_NAME): app internal
	go build -ldflags="$(LDFLAGS)" -o $(BINARY_NAME) ./app

test:
	go test -cover ./internal/ ./app/cmd/

interop_vectors:
	go test -v -run ^TestInteropVector ./internal/

clean:
	rm -f $(BINARY_NAME)
	go clean
