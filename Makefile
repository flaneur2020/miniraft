.PHONY: build test clean

build:
	go build -o bin/miniraft ./cmd/miniraft/*.go

test:
	go test -v -c ./pkg/raft && ./raft.test
	# go test -v ./...

clean:
	rm -rf bin/*
