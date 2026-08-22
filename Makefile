BINARY := bin/systemd-transition-exporter
CMD := ./cmd/systemd-transition-exporter

.PHONY: all build test check clean tidy

all: check

# Build the actual executable. `go build ./...` only builds all packages and
# does not select a single output binary for a multi-package module.
build:
	mkdir -p bin
	go build -o $(BINARY) $(CMD)

# Run the unit tests for all packages.
test:
	go test ./...

# Verify tests and build the executable that will be deployed.
check: test build

# Synchronize module dependencies and go.sum.
tidy:
	go mod tidy

clean:
	rm -rf bin
