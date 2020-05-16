.PHONY: build test clean

build:
	go build -o bin/miniraft ./cmd/miniraft/*.go

test:
	go test -c ./pkg/raft && ./raft.test

clean:
	rm -rf bin/*
