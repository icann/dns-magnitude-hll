BINARY=		dnsmag


all: build

build: $(BINARY)

$(BINARY): app internal
	go build -o dnsmag ./app

clean:
	rm -f $(BINARY)
	go clean
