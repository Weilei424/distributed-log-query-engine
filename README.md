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
| Go | 1.22+ | https://go.dev/dl/ |
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
| 1 | Foundation — repo scaffold, Go module, proto contracts | Complete |
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
