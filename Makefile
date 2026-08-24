BINARY := bin/systemd-transition-exporter
CMD := ./cmd/systemd-transition-exporter

.PHONY: all build build-rocky8 test check clean tidy

all: check

# Build the actual executable. `go build ./...` only builds all packages and
# does not select a single output binary for a multi-package module.
build:
	mkdir -p bin
	go build -o $(BINARY) $(CMD)

# Build a Linux amd64 binary suitable for Rocky Linux 8.x.
# CGO is disabled so the binary does not depend on the target system's glibc.
build-rocky8:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
		go build -trimpath -ldflags="-s -w" -o $(BINARY) $(CMD)

# Run the unit tests for all packages.
test:
	go test ./...

# Verify tests and build the executable that will be deployed.
check: test build

tidy:
	go mod tidy

clean:
	rm -rf bin
