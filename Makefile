.PHONY: build test clean

build:
	go build -o bin/miniraft ./cmd/miniraft/*.go

test:
	go test -i ./... && go test -v ./...

clean:
	rm -rf bin/*
