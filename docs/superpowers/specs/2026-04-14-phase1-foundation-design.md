# Phase 1 — Project Foundation and System Design

**Date:** 2026-04-14
**Phase:** 1 of 8
**Status:** Approved

---

## Overview

Phase 1 establishes the repository scaffold, Go module, build tooling, protobuf API contracts, and local development baseline for the distributed log query engine. No production logic is implemented in this phase. The end state is a repository where `go build ./...`, `make lint`, and `make test` all pass cleanly, and the API contracts are defined and generated.

---

## Goals

- Create the full repository folder structure matching the architecture layout
- Initialize the Go module at `github.com/Weilei424/distributed-log-query-engine`
- Set up Makefile with standard developer targets
- Define protobuf message types and service contracts for ingest and query paths
- Generate Go bindings from proto using `buf`
- Create `pkg/types` with a plain Go `LogEntry` struct decoupled from generated proto code
- Add a Docker Compose stub for local multi-node development
- Update README with setup instructions and project pointers

---

## Non-Goals

- No gRPC server implementation (Phase 2)
- No storage, indexing, or query logic (Phases 2–3)
- No cluster coordination (Phase 4)
- No Prometheus, Grafana, or Kubernetes (Phase 7)

---

## Repository Layout

```
.
├── cmd/
│   ├── node/
│   │   └── main.go              # stub: imports compile check, exits 0
│   ├── coordinator/
│   │   └── main.go              # stub
│   └── cli/
│       └── main.go              # stub
├── internal/
│   ├── api/         doc.go
│   ├── cluster/     doc.go
│   ├── config/      doc.go
│   ├── coordinator/ doc.go
│   ├── ingest/      doc.go
│   ├── index/       doc.go
│   ├── metadata/    doc.go
│   ├── replication/ doc.go
│   ├── storage/     doc.go
│   └── observability/ doc.go
├── pkg/
│   └── types/
│       └── log_entry.go         # plain Go LogEntry struct
├── proto/
│   ├── buf.yaml
│   ├── buf.gen.yaml
│   └── logengine/v1/
│       ├── log_entry.proto
│       ├── ingest.proto
│       └── query.proto
├── internal/api/gen/            # buf-generated Go code (gitignored or committed)
├── deployments/
│   ├── docker-compose/
│   │   └── docker-compose.yml
│   └── kubernetes/
│       └── .gitkeep
├── test/
│   ├── integration/
│   │   └── .gitkeep
│   └── fixtures/
│       └── .gitkeep
├── Makefile
├── .golangci.yml
├── .gitignore
└── README.md
```

### Layout rules (enforced)
- `cmd/` binaries are thin stubs in Phase 1
- All business logic lives under `internal/`
- `pkg/types` holds shared Go types only — no framework imports
- Generated proto code lives under `internal/api/gen/` and is separated from source proto files

---

## Go Module

| Field | Value |
|-------|-------|
| Module path | `github.com/Weilei424/distributed-log-query-engine` |
| Go version | `1.24` |
| Phase 1 external deps | None beyond buf-generated gRPC libraries |

---

## Makefile Targets

| Target | Command | Purpose |
|--------|---------|---------|
| `make build` | `go build ./...` | Compile all packages |
| `make test` | `go test ./...` | Run all tests |
| `make lint` | `golangci-lint run ./...` | Static analysis |
| `make run-local` | `go build ./...` | Phase 1 runnable check (same as build) |
| `make proto` | `buf generate` | Regenerate Go bindings from proto |
| `make proto-lint` | `buf lint` | Lint proto files |

---

## Linting

**Tool:** `golangci-lint`
**Config:** `.golangci.yml` at repo root

Enabled linters:
- `errcheck` — unchecked errors
- `govet` — vet checks
- `staticcheck` — advanced static analysis
- `unused` — unused code
- `gofmt` — formatting

No aggressive linters enabled in Phase 1. Add stricter rules as the codebase grows.

---

## Protobuf Definitions

**Tooling:** `buf`
**Source:** `proto/logengine/v1/`
**Output:** `internal/api/gen/`

### buf.yaml
- Module name: `buf.build/weilei424/distributed-log-query-engine`
- Lint ruleset: `DEFAULT`
- Breaking change detection: `FILE`

### buf.gen.yaml
Plugins:
- `protoc-gen-go` → generates message types
- `protoc-gen-go-grpc` → generates service stubs and client interfaces

### log_entry.proto

```protobuf
syntax = "proto3";
package logengine.v1;

message LogEntry {
  string id                    = 1;
  int64  timestamp             = 2;  // producer-assigned Unix nanoseconds
  int64  received_at           = 3;  // ingest-assigned Unix nanoseconds
  string service               = 4;
  string level                 = 5;
  string message               = 6;
  map<string, string> fields   = 7;
}
```

### ingest.proto

```protobuf
syntax = "proto3";
package logengine.v1;

import "logengine/v1/log_entry.proto";

service IngestService {
  rpc Ingest(IngestRequest) returns (IngestResponse);
  rpc IngestBatch(IngestBatchRequest) returns (IngestBatchResponse);
}

message IngestRequest  { LogEntry entry = 1; }
message IngestResponse { string id = 1; bool ok = 2; }

message IngestBatchRequest  { repeated LogEntry entries = 1; }
message IngestBatchResponse { int32 accepted = 1; int32 rejected = 2; }
```

### query.proto

```protobuf
syntax = "proto3";
package logengine.v1;

import "logengine/v1/log_entry.proto";

service QueryService {
  rpc Query(QueryRequest) returns (QueryResponse);
}

message QueryRequest {
  string keyword    = 1;
  string service    = 2;
  int64  start_time = 3;  // Unix nanoseconds, 0 = unbounded
  int64  end_time   = 4;  // Unix nanoseconds, 0 = unbounded
  int32  limit      = 5;
  int32  offset     = 6;
}

message QueryResponse {
  repeated LogEntry entries = 1;
  int32  total              = 2;
  bool   partial            = 3;  // true if one or more nodes did not respond
  int64  took_ms            = 4;
}
```

---

## pkg/types — LogEntry

Plain Go struct mirroring the proto shape. Used by `internal/` packages so core logic does not import generated proto code directly.

```go
// pkg/types/log_entry.go
package types

type LogEntry struct {
    ID         string            
    Timestamp  int64             // Unix nanoseconds, producer-assigned
    ReceivedAt int64             // Unix nanoseconds, ingest-assigned
    Service    string            
    Level      string            
    Message    string            
    Fields     map[string]string 
}
```

---

## Docker Compose Stub

**Path:** `deployments/docker-compose/docker-compose.yml`

Three `node` services and one `coordinator` service. All stubs — they reference the `cmd/node` and `cmd/coordinator` binaries that will be built in later phases.

Each node service has:
- `NODE_ID` env var (`node-1`, `node-2`, `node-3`)
- A named volume for future segment storage (`data-node-1`, etc.)
- gRPC port exposed (`50051`, `50052`, `50053`)

Coordinator service has:
- `NODE_ID: coordinator-1`
- Port `50050` exposed

No Prometheus, Grafana, or health checks in Phase 1.

---

## README Updates

Sections to add or update:

1. **Project description** — one paragraph describing the distributed log query engine
2. **Architecture overview** — one-liner per major component (ingest, storage, index, metadata, coordinator)
3. **Prerequisites** — Go 1.24+, Docker, `buf`, `golangci-lint`
4. **Setup**
   ```bash
   go mod download
   make proto
   make build
   make test
   make lint
   ```
5. **Docs pointers**
   - `docs/planning/IMPLEMENTATION_PLAN.md` — phase roadmap
   - `docs/planning/ARCHITECTURE_NOTES.md` — design decisions and system model

---

## Success Criteria

- [ ] `go build ./...` passes with no errors
- [ ] `make lint` passes with no errors
- [ ] `make test` passes (no tests yet, but the command succeeds)
- [ ] `make proto` generates Go bindings cleanly
- [ ] `make proto-lint` passes
- [ ] All `internal/` packages exist with `doc.go` placeholders
- [ ] `pkg/types.LogEntry` is defined and compiles
- [ ] Docker Compose file is valid (`docker compose config` passes)
- [ ] README reflects current setup steps accurately
