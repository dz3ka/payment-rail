# Conduit developer Makefile. Targets assume Go 1.24+ and (for up/down) Docker.

BINARIES   := api ledger signer chainwatcher webhookd conduitctl
BIN_DIR    := bin

VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
VPKG       := github.com/dz3ka/payment-rail/internal/version
LDFLAGS    := -s -w \
	-X '$(VPKG).Version=$(VERSION)' \
	-X '$(VPKG).Commit=$(COMMIT)' \
	-X '$(VPKG).BuildDate=$(BUILD_DATE)'

.PHONY: all build test cover vet lint tidy sqlc up down clean $(BINARIES)

all: build

## build: compile all service binaries into ./bin
build: $(BINARIES)

$(BINARIES):
	go build -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$@ ./cmd/$@

## test: run the full test suite with the race detector
test:
	go test -race ./...

## cover: run tests with coverage and print a per-func summary
cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

## vet: run go vet across all packages
vet:
	go vet ./...

## lint: run golangci-lint (see .golangci.yml)
lint:
	golangci-lint run

## tidy: sync go.mod/go.sum
tidy:
	go mod tidy

## sqlc: regenerate the typed db package from db/migrations + db/query
sqlc:
	sqlc generate

## up: start the local dev stack (Postgres, Redpanda, OTel Collector)
up:
	docker compose up -d

## down: stop the dev stack and remove volumes
down:
	docker compose down -v

## clean: remove build and coverage artifacts
clean:
	rm -rf $(BIN_DIR) coverage.out
