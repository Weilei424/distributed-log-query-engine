.PHONY: build test lint run-local proto proto-lint

## build: compile all packages
build:
	go build ./...

## test: run all tests
test:
	go test ./...

## lint: run golangci-lint
lint:
	golangci-lint run ./...

## run-local: verify the project compiles (Phase 1 runnable check)
run-local: build

## proto: regenerate Go bindings from proto sources
proto:
	buf generate

## proto-lint: lint proto source files
proto-lint:
	buf lint proto

## help: print this help message
help:
	@grep -E '^##' Makefile | sed 's/## //'
