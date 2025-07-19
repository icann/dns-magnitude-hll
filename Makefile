BINARY=		dnsmag


all: build

build: $(BINARY)

$(BINARY):
	go build -o dnsmag ./app

clean:
	rm -f $(BINARY)
