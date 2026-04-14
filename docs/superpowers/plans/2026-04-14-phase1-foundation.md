# Phase 1 — Project Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scaffold the full repository structure, Go module, build tooling, protobuf API contracts, and Docker Compose stub so that `go build ./...`, `make lint`, `make test`, and `make proto-lint` all pass cleanly.

**Architecture:** All packages are stubs or placeholders in Phase 1 — no business logic is implemented. Proto files define the ingest and query service contracts. Generated Go bindings are committed to the repo. `pkg/types` holds a plain Go `LogEntry` struct that internal packages will use without importing generated proto code directly.

**Tech Stack:** Go 1.24, buf v2, protoc-gen-go, protoc-gen-go-grpc, golangci-lint, Docker Compose v2

---

## Prerequisites

The following tools must be installed before running any tasks:

```bash
# buf CLI — https://buf.build/docs/installation
# macOS
brew install bufbuild/buf/buf
# Linux / WSL
BUF_VERSION=1.32.0
curl -sSL "https://github.com/bufbuild/buf/releases/download/v${BUF_VERSION}/buf-Linux-x86_64" -o /usr/local/bin/buf && chmod +x /usr/local/bin/buf

# protoc plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# golangci-lint — https://golangci-lint.run/usage/install/
# macOS
brew install golangci-lint
# Linux / WSL
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.59.1
```

Verify:
```bash
buf --version       # >= 1.32.0
golangci-lint --version
protoc-gen-go --version
```

---

## Task 1: Initialize Go module and update .gitignore

**Files:**
- Create: `go.mod`
- Modify: `.gitignore`

- [ ] **Step 1: Initialize the Go module**

```bash
cd /path/to/distributed-log-query-engine
go mod init github.com/Weilei424/distributed-log-query-engine
```

Verify `go.mod` was created:
```
module github.com/Weilei424/distributed-log-query-engine

go 1.24
```

- [ ] **Step 2: Append build and data entries to .gitignore**

Add the following block to the bottom of the existing `.gitignore`:

```gitignore
# Build outputs
/bin/
*.test
*.out
*.prof

# Local node data
/data/

# OS
.DS_Store
Thumbs.db
```

- [ ] **Step 3: Confirm go.mod and .gitignore look correct**

```bash
cat go.mod
```

Expected output:
```
module github.com/Weilei424/distributed-log-query-engine

go 1.24
```

**Suggested commit messages:**
- `go.mod` — `init: initialize Go module at github.com/Weilei424/distributed-log-query-engine`
- `.gitignore` — `chore: add build outputs and local data to gitignore`

---

## Task 2: Create pkg/types/log_entry.go

**Files:**
- Create: `pkg/types/log_entry.go`

- [ ] **Step 1: Create the types package directory and file**

Create `pkg/types/log_entry.go` with the following content:

```go
// Package types defines shared domain types used across internal packages.
// These types are intentionally decoupled from generated protobuf code so that
// core packages can be tested and reasoned about without proto dependencies.
package types

// LogEntry represents a single log record in the system.
// Timestamp and ReceivedAt are Unix nanoseconds.
// Timestamp is assigned by the producer; ReceivedAt is assigned by the ingest path.
type LogEntry struct {
	ID         string
	Timestamp  int64
	ReceivedAt int64
	Service    string
	Level      string
	Message    string
	Fields     map[string]string
}
```

- [ ] **Step 2: Verify the package compiles**

```bash
go build ./pkg/...
```

Expected: no output, exit 0.

**Suggested commit message:**
- `pkg/types/log_entry.go` — `feat: add LogEntry core type in pkg/types`

---

## Task 3: Create internal package doc.go placeholders

**Files (all Create):**
- `internal/api/doc.go`
- `internal/cluster/doc.go`
- `internal/config/doc.go`
- `internal/coordinator/doc.go`
- `internal/ingest/doc.go`
- `internal/index/doc.go`
- `internal/metadata/doc.go`
- `internal/replication/doc.go`
- `internal/storage/doc.go`
- `internal/observability/doc.go`

- [ ] **Step 1: Create internal/api/doc.go**

```go
// Package api contains generated protobuf bindings and gRPC service definitions.
// Subpackage gen/ is produced by buf and should not be edited manually.
package api
```

- [ ] **Step 2: Create internal/cluster/doc.go**

```go
// Package cluster manages cluster membership, node discovery, and heartbeat tracking.
package cluster
```

- [ ] **Step 3: Create internal/config/doc.go**

```go
// Package config defines configuration loading and validation for node and coordinator processes.
package config
```

- [ ] **Step 4: Create internal/coordinator/doc.go**

```go
// Package coordinator implements the distributed query fan-out and result aggregation logic.
package coordinator
```

- [ ] **Step 5: Create internal/ingest/doc.go**

```go
// Package ingest handles log record validation, shard routing, and write forwarding.
package ingest
```

- [ ] **Step 6: Create internal/index/doc.go**

```go
// Package index maintains an in-memory inverted index for keyword and time-range lookup.
package index
```

- [ ] **Step 7: Create internal/metadata/doc.go**

```go
// Package metadata manages cluster state including the node registry, shard ownership map,
// and replica assignments. Consistency is maintained through Raft-backed leadership.
package metadata
```

- [ ] **Step 8: Create internal/replication/doc.go**

```go
// Package replication handles asynchronous log record replication from primary to replica nodes.
package replication
```

- [ ] **Step 9: Create internal/storage/doc.go**

```go
// Package storage implements append-only segment file management for durable local log persistence.
package storage
```

- [ ] **Step 10: Create internal/observability/doc.go**

```go
// Package observability exposes Prometheus metrics and structured logging utilities
// shared across all long-running services.
package observability
```

- [ ] **Step 11: Verify all packages compile**

```bash
go build ./internal/...
```

Expected: no output, exit 0.

**Suggested commit message:**
- `internal/*/doc.go` (×10) — `chore: add package doc.go placeholders for all internal packages`

---

## Task 4: Create cmd stubs

**Files:**
- Create: `cmd/node/main.go`
- Create: `cmd/coordinator/main.go`
- Create: `cmd/cli/main.go`

- [ ] **Step 1: Create cmd/node/main.go**

```go
package main

import (
	"fmt"
	"os"
)

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "node-local"
	}
	fmt.Printf("node starting: id=%s\n", nodeID)
}
```

- [ ] **Step 2: Create cmd/coordinator/main.go**

```go
package main

import (
	"fmt"
	"os"
)

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "coordinator-local"
	}
	fmt.Printf("coordinator starting: id=%s\n", nodeID)
}
```

- [ ] **Step 3: Create cmd/cli/main.go**

```go
package main

import "fmt"

func main() {
	fmt.Println("dlqe cli")
}
```

- [ ] **Step 4: Verify all cmd packages compile**

```bash
go build ./cmd/...
```

Expected: no output, exit 0.

**Suggested commit message:**
- `cmd/node/main.go`, `cmd/coordinator/main.go`, `cmd/cli/main.go` — `chore: add stub main packages for node, coordinator, and cli`

---

## Task 5: Create Makefile and golangci-lint config

**Files:**
- Create: `Makefile`
- Create: `.golangci.yml`

- [ ] **Step 1: Create Makefile**

```makefile
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
	buf lint

## help: print this help message
help:
	@grep -E '^##' Makefile | sed 's/## //'
```

- [ ] **Step 2: Create .golangci.yml**

```yaml
run:
  timeout: 5m

linters:
  enable:
    - errcheck
    - govet
    - staticcheck
    - unused
    - gofmt

linters-settings:
  errcheck:
    check-type-assertions: true
```

- [ ] **Step 3: Verify make build passes**

```bash
make build
```

Expected: no output, exit 0.

- [ ] **Step 4: Verify make test passes**

```bash
make test
```

Expected: output like `ok  	github.com/Weilei424/distributed-log-query-engine/...` or `[no test files]` for each package. Exit 0.

**Suggested commit messages:**
- `Makefile` — `chore: add Makefile with build, test, lint, proto, and run-local targets`
- `.golangci.yml` — `chore: add golangci-lint config with errcheck, govet, staticcheck, unused, gofmt`

---

## Task 6: Create proto source files

**Files:**
- Create: `proto/buf.yaml`
- Create: `proto/logengine/v1/log_entry.proto`
- Create: `proto/logengine/v1/ingest.proto`
- Create: `proto/logengine/v1/query.proto`

- [ ] **Step 1: Create proto/buf.yaml**

```yaml
version: v2
lint:
  use:
    - DEFAULT
breaking:
  use:
    - FILE
```

- [ ] **Step 2: Create proto/logengine/v1/log_entry.proto**

```protobuf
syntax = "proto3";

package logengine.v1;

option go_package = "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1;logengine";

// LogEntry represents a single log record stored and queried by the system.
message LogEntry {
  // Unique identifier for the log entry.
  string id = 1;

  // Producer-assigned Unix timestamp in nanoseconds.
  int64 timestamp = 2;

  // Ingest-path-assigned Unix timestamp in nanoseconds.
  int64 received_at = 3;

  // Name of the service that produced the log.
  string service = 4;

  // Log severity level (e.g. INFO, WARN, ERROR).
  string level = 5;

  // Human-readable log message.
  string message = 6;

  // Arbitrary structured metadata fields.
  map<string, string> fields = 7;
}
```

- [ ] **Step 3: Create proto/logengine/v1/ingest.proto**

```protobuf
syntax = "proto3";

package logengine.v1;

import "logengine/v1/log_entry.proto";

option go_package = "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1;logengine";

// IngestService accepts log entries from producers.
service IngestService {
  // Ingest writes a single log entry to the owning shard.
  rpc Ingest(IngestRequest) returns (IngestResponse);

  // IngestBatch writes multiple log entries in a single request.
  rpc IngestBatch(IngestBatchRequest) returns (IngestBatchResponse);
}

message IngestRequest {
  LogEntry entry = 1;
}

message IngestResponse {
  // The assigned ID of the accepted entry.
  string id = 1;

  // True if the entry was durably written.
  bool ok = 2;
}

message IngestBatchRequest {
  repeated LogEntry entries = 1;
}

message IngestBatchResponse {
  // Number of entries accepted and durably written.
  int32 accepted = 1;

  // Number of entries rejected due to validation or write failure.
  int32 rejected = 2;
}
```

- [ ] **Step 4: Create proto/logengine/v1/query.proto**

```protobuf
syntax = "proto3";

package logengine.v1;

import "logengine/v1/log_entry.proto";

option go_package = "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1;logengine";

// QueryService executes search queries across the cluster.
service QueryService {
  // Query searches for log entries matching the given criteria.
  rpc Query(QueryRequest) returns (QueryResponse);
}

message QueryRequest {
  // Keyword to search for in log messages.
  string keyword = 1;

  // Filter by service name. Empty means all services.
  string service = 2;

  // Lower bound of the time range (Unix nanoseconds). 0 means unbounded.
  int64 start_time = 3;

  // Upper bound of the time range (Unix nanoseconds). 0 means unbounded.
  int64 end_time = 4;

  // Maximum number of entries to return. 0 uses server default (100).
  int32 limit = 5;

  // Number of entries to skip for pagination.
  int32 offset = 6;
}

message QueryResponse {
  // Matching log entries, sorted by timestamp descending.
  repeated LogEntry entries = 1;

  // Total number of matching entries across all nodes (before limit/offset).
  int32 total = 2;

  // True if one or more nodes did not respond within the deadline.
  bool partial = 3;

  // Total wall-clock time for the query in milliseconds.
  int64 took_ms = 4;
}
```

- [ ] **Step 5: Run proto lint to verify the proto files are valid**

```bash
buf lint proto/
```

Expected: no output, exit 0.

**Suggested commit messages:**
- `proto/buf.yaml` — `chore: add buf module config with DEFAULT lint and FILE breaking rules`
- `proto/logengine/v1/log_entry.proto` — `feat: define LogEntry protobuf message`
- `proto/logengine/v1/ingest.proto` — `feat: define IngestService protobuf contract`
- `proto/logengine/v1/query.proto` — `feat: define QueryService protobuf contract`

---

## Task 7: Configure buf codegen and generate Go bindings

**Files:**
- Create: `buf.gen.yaml` (repo root)
- Generated: `internal/api/gen/logengine/v1/` (committed)

- [ ] **Step 1: Create buf.gen.yaml at repo root**

```yaml
version: v2
plugins:
  - local: protoc-gen-go
    out: internal/api/gen
    opt:
      - paths=source_relative
  - local: protoc-gen-go-grpc
    out: internal/api/gen
    opt:
      - paths=source_relative
      - require_unimplemented_servers=false
inputs:
  - directory: proto
```

`require_unimplemented_servers=false` prevents compile errors from unimplemented server methods in Phases 1–7 before the full gRPC server is wired.

- [ ] **Step 2: Run buf generate**

```bash
buf generate
```

Expected: no output, exit 0. The following files should be created:
```
internal/api/gen/logengine/v1/log_entry.pb.go
internal/api/gen/logengine/v1/ingest.pb.go
internal/api/gen/logengine/v1/ingest_grpc.pb.go
internal/api/gen/logengine/v1/query.pb.go
internal/api/gen/logengine/v1/query_grpc.pb.go
```

Verify:
```bash
ls internal/api/gen/logengine/v1/
```

- [ ] **Step 3: Run go mod tidy to pull in grpc and protobuf dependencies**

```bash
go mod tidy
```

Expected: `go.mod` and `go.sum` updated with `google.golang.org/grpc` and `google.golang.org/protobuf`.

Verify `go.mod` now contains:
```bash
grep "google.golang.org" go.mod
```

Expected output (versions may differ):
```
google.golang.org/grpc v1.x.x
google.golang.org/protobuf v1.x.x
```

- [ ] **Step 4: Verify everything compiles including generated code**

```bash
make build
```

Expected: no output, exit 0.

- [ ] **Step 5: Verify make proto-lint passes**

```bash
make proto-lint
```

Expected: no output, exit 0.

**Suggested commit messages:**
- `buf.gen.yaml` — `chore: add buf codegen config targeting internal/api/gen`
- `internal/api/gen/logengine/v1/*.go` — `chore: add buf-generated Go bindings for logengine/v1`
- `go.mod`, `go.sum` — `chore: add grpc and protobuf dependencies via go mod tidy`

---

## Task 8: Create Docker Compose stub and placeholder directories

**Files:**
- Create: `deployments/docker-compose/docker-compose.yml`
- Create: `deployments/docker-compose/Dockerfile.node`
- Create: `deployments/docker-compose/Dockerfile.coordinator`
- Create: `deployments/kubernetes/.gitkeep`
- Create: `test/integration/.gitkeep`
- Create: `test/fixtures/.gitkeep`

- [ ] **Step 1: Create deployments/docker-compose/Dockerfile.node**

```dockerfile
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o /node ./cmd/node

FROM alpine:3.19
COPY --from=builder /node /node
ENTRYPOINT ["/node"]
```

- [ ] **Step 2: Create deployments/docker-compose/Dockerfile.coordinator**

```dockerfile
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o /coordinator ./cmd/coordinator

FROM alpine:3.19
COPY --from=builder /coordinator /coordinator
ENTRYPOINT ["/coordinator"]
```

- [ ] **Step 3: Create deployments/docker-compose/docker-compose.yml**

```yaml
version: "3.8"

services:
  node-1:
    build:
      context: ../..
      dockerfile: deployments/docker-compose/Dockerfile.node
    environment:
      - NODE_ID=node-1
    ports:
      - "50051:50051"
    volumes:
      - data-node-1:/data

  node-2:
    build:
      context: ../..
      dockerfile: deployments/docker-compose/Dockerfile.node
    environment:
      - NODE_ID=node-2
    ports:
      - "50052:50051"
    volumes:
      - data-node-2:/data

  node-3:
    build:
      context: ../..
      dockerfile: deployments/docker-compose/Dockerfile.node
    environment:
      - NODE_ID=node-3
    ports:
      - "50053:50051"
    volumes:
      - data-node-3:/data

  coordinator-1:
    build:
      context: ../..
      dockerfile: deployments/docker-compose/Dockerfile.coordinator
    environment:
      - NODE_ID=coordinator-1
    ports:
      - "50050:50050"

volumes:
  data-node-1:
  data-node-2:
  data-node-3:
```

- [ ] **Step 4: Create placeholder files for empty directories**

Create `deployments/kubernetes/.gitkeep` — empty file.
Create `test/integration/.gitkeep` — empty file.
Create `test/fixtures/.gitkeep` — empty file.

- [ ] **Step 5: Validate Docker Compose config is syntactically valid**

```bash
docker compose -f deployments/docker-compose/docker-compose.yml config --quiet
```

Expected: exit 0. (Images won't exist yet — that's fine. This just validates the YAML structure.)

**Suggested commit messages:**
- `deployments/docker-compose/Dockerfile.node` — `chore: add node image Dockerfile stub`
- `deployments/docker-compose/Dockerfile.coordinator` — `chore: add coordinator image Dockerfile stub`
- `deployments/docker-compose/docker-compose.yml` — `chore: add Docker Compose stub for 3-node + coordinator local cluster`
- `deployments/kubernetes/.gitkeep`, `test/integration/.gitkeep`, `test/fixtures/.gitkeep` — `chore: add placeholder files for empty directories`

---

## Task 9: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Replace README.md contents**

```markdown
# distributed-log-query-engine

A distributed log query engine written in Go. Ingests logs from multiple producers, stores them across nodes using append-only segment files, and executes queries in parallel across the cluster.

Built to demonstrate distributed systems fundamentals: partitioning, replication, cluster coordination, fault tolerance, and distributed query execution.

---

## Architecture

| Component | Responsibility |
|-----------|---------------|
| **Ingest path** | Validates log records, computes shard placement, forwards to primary storage node |
| **Storage node** | Appends logs to local segment files, maintains in-memory index, serves local queries |
| **Metadata / control plane** | Node registry, shard ownership map, Raft-backed leader election |
| **Query coordinator** | Fan-out queries to relevant nodes, enforces deadlines, merges and paginates results |
| **Background workers** | Compaction, index rebuilds, replica catch-up |

See [`docs/planning/ARCHITECTURE_NOTES.md`](docs/planning/ARCHITECTURE_NOTES.md) for full design decisions and system model.

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Go | 1.24+ | https://go.dev/dl/ |
| buf | 1.32+ | `brew install bufbuild/buf/buf` |
| protoc-gen-go | latest | `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest` |
| protoc-gen-go-grpc | latest | `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest` |
| golangci-lint | 1.59+ | https://golangci-lint.run/usage/install/ |
| Docker | 24+ | https://docs.docker.com/get-docker/ |

---

## Setup

```bash
git clone https://github.com/Weilei424/distributed-log-query-engine
cd distributed-log-query-engine

go mod download
make proto
make build
make test
make lint
```

---

## Development Commands

| Command | Description |
|---------|-------------|
| `make build` | Compile all packages |
| `make test` | Run all tests |
| `make lint` | Run golangci-lint |
| `make run-local` | Verify project compiles (Phase 1) |
| `make proto` | Regenerate Go bindings from proto sources |
| `make proto-lint` | Lint proto source files |

---

## Project Phases

| Phase | Goal | Status |
|-------|------|--------|
| 1 | Foundation — repo scaffold, Go module, proto contracts | In progress |
| 2 | Single-node ingestion and storage engine | Not started |
| 3 | Single-node indexing and query engine | Not started |
| 4 | Multi-node cluster formation and metadata coordination | Not started |
| 5 | Distributed ingestion, partitioning, and replication | Not started |
| 6 | Distributed query fan-out and result aggregation | Not started |
| 7 | Observability, deployment, and reliability | Not started |
| 8 | Stretch goals and resume polish | Not started |

See [`docs/planning/IMPLEMENTATION_PLAN.md`](docs/planning/IMPLEMENTATION_PLAN.md) for full phase descriptions and success criteria.

---

## Docs

- [`docs/planning/IMPLEMENTATION_PLAN.md`](docs/planning/IMPLEMENTATION_PLAN.md) — phase roadmap
- [`docs/planning/ARCHITECTURE_NOTES.md`](docs/planning/ARCHITECTURE_NOTES.md) — design decisions and system model
- [`docs/planning/BACKLOG.md`](docs/planning/BACKLOG.md) — executable checklist
```

**Suggested commit message:**
- `README.md` — `docs: update README with architecture overview, prerequisites, and setup instructions`

---

## Task 10: Final verification pass

**Files:** none created — validation only.

- [ ] **Step 1: Run make build**

```bash
make build
```

Expected: no output, exit 0.

- [ ] **Step 2: Run make test**

```bash
make test
```

Expected: all packages report `ok` or `[no test files]`. Exit 0.

- [ ] **Step 3: Run make lint**

```bash
make lint
```

Expected: no lint errors. Exit 0.
If lint reports errors in generated files under `internal/api/gen/`, add this to `.golangci.yml`:

```yaml
issues:
  exclude-dirs:
    - internal/api/gen
```

Then re-run `make lint`.

- [ ] **Step 4: Run make proto-lint**

```bash
make proto-lint
```

Expected: no output, exit 0.

- [ ] **Step 5: Validate Docker Compose**

```bash
docker compose -f deployments/docker-compose/docker-compose.yml config --quiet
```

Expected: exit 0.

- [ ] **Step 6: Verify directory structure is complete**

```bash
find . -name "*.go" | grep -v vendor | sort
```

Expected to see files under: `cmd/`, `internal/`, `pkg/types/`, `internal/api/gen/`.

- [ ] **Step 7: Update BACKLOG.md Phase 1 checklist**

Mark the following items `[x]` in `docs/planning/BACKLOG.md`:
- `Repository folder structure created`
- `Go module initialized`
- `Makefile with targets`
- `Protobuf definitions`
- `Initial gRPC service contracts`
- `pkg/types package with LogEntry`
- `Basic README updated`
- `Docker Compose file`
- `Confirm make run-local starts successfully`

---

## Success Criteria Checklist

- [ ] `go build ./...` passes with no errors
- [ ] `make lint` passes with no errors
- [ ] `make test` passes (no tests yet, command exits 0)
- [ ] `make proto` generates Go bindings cleanly
- [ ] `make proto-lint` passes with no errors
- [ ] All `internal/` packages exist with `doc.go` placeholders
- [ ] `pkg/types.LogEntry` is defined and compiles
- [ ] Docker Compose YAML is valid (`docker compose config` exits 0)
- [ ] README reflects current setup steps accurately
- [ ] `docs/planning/BACKLOG.md` Phase 1 items updated
