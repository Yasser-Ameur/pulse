# Pulse broker build and verification targets.
#
# The full gate (CI) is: make fmt-check lint vet test-race
#
# Local note: this machine has no gcc, so the data-plane builds with
# CGO_ENABLED=0 (Pebble is pure Go). CI (ubuntu) additionally runs -race.

GOFLAGS  ?= -mod=mod
GOSUMDB  ?= off
GOPROXY  ?= off
CGO      ?= 0

export GOFLAGS GOSUMDB GOPROXY CGO_ENABLED

CGO_ENABLED := $(CGO)

VERSION ?= $(shell git describe --tags --always --dirty)
LDFLAGS := -s -w -X github.com/Yasser-Ameur/pulse/internal/server.Version=$(VERSION)

.PHONY: build
build:
	mkdir -p bin
	go build -ldflags="$(LDFLAGS)" -o bin/pulse-server ./cmd/pulse-server
	go build -ldflags="$(LDFLAGS)" -o bin/pulse-cli ./cmd/pulse-cli

.PHONY: test
test:
	go test ./...

.PHONY: test-race
test-race:
	go test -race -cover ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: bench-check
bench-check:
	cd bench && go vet ./... && go build ./...

.PHONY: fmt-check
fmt-check:
	@test -z "$$(gofmt -l ./internal ./cmd ./pkg ./tests ./examples ./bench)" || (echo "gofmt needed:"; gofmt -l ./internal ./cmd ./pkg ./tests ./examples ./bench; exit 1)

.PHONY: fmt
fmt:
	gofmt -w ./internal ./cmd ./pkg ./tests ./examples ./bench

.PHONY: lint
lint:
	golangci-lint run

.PHONY: coverage
coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Floor is 3 points under the total measured on 2026-09-02 (53.5%).
COVERAGE_FLOOR ?= 65

.PHONY: coverage-check
coverage-check:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | awk -v floor=$(COVERAGE_FLOOR) \
		'/^total:/ { pct = $$3 + 0; printf "total coverage: %.1f%% (floor %d%%)\n", pct, floor; \
		if (pct < floor) { print "coverage below floor"; exit 1 } }'

.PHONY: image
image:
	docker build -t pulse:$(VERSION) --build-arg VERSION=$(VERSION) .

.PHONY: clean
clean:
	rm -rf bin dist data coverage.out coverage.html
