.PHONY: build test lint run-local proto proto-lint load-test

## build: compile all packages
build:
	go build ./...

## test: run all tests
test:
	go test ./...

## lint: run golangci-lint
lint:
	golangci-lint run ./...

## run-local: start the full local cluster (nodes, coordinators, Prometheus, Grafana)
run-local:
	docker compose -f deployments/docker-compose/docker-compose.yml up --build

## proto: regenerate Go bindings from proto sources
proto:
	buf generate

## proto-lint: lint proto source files
proto-lint:
	buf lint proto

## load-test: run load test against a live cluster (ADDR, DURATION, MODE are optional)
load-test:
	go run ./test/load \
		-addr=$(or $(ADDR),localhost:9001) \
		-duration=$(or $(DURATION),30s) \
		-mode=$(or $(MODE),both)

## help: print this help message
help:
	@grep -E '^##' Makefile | sed 's/## //'
