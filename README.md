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
make build
make test
make lint
```

> `make proto` is only needed if you modify `.proto` files. Generated bindings are already committed.

---

## Running the Full Stack

Start the complete multi-node cluster with Prometheus and Grafana:

```bash
make run-local
```

This runs all three storage nodes, three coordinator replicas, Prometheus, and Grafana via Docker Compose.

| Service | URL |
|---------|-----|
| Grafana (Log Engine dashboard) | http://localhost:3000 |
| Prometheus | http://localhost:9095 |

Run a load test against the live cluster:

```bash
make load-test                                   # both ingest and query, 30s
make load-test ADDR=localhost:9001 DURATION=20s MODE=ingest
make load-test ADDR=localhost:9001 DURATION=20s MODE=query
```

To walk through a full failure and recovery scenario, see [`docs/runbooks/failure-demo.md`](docs/runbooks/failure-demo.md).

---

## Development Commands

| Command | Description |
|---------|-------------|
| `make build` | Compile all packages |
| `make test` | Run all tests |
| `make lint` | Run golangci-lint |
| `make run-local` | Start the full local cluster (nodes, coordinators, Prometheus, Grafana) |
| `make load-test` | Run a load test against a live cluster |
| `make proto` | Regenerate Go bindings from proto sources |
| `make proto-lint` | Lint proto source files |

---

## Project Phases

| Phase | Goal | Status |
|-------|------|--------|
| 1 | Foundation — repo scaffold, Go module, proto contracts | Complete |
| 2 | Single-node ingestion and storage engine | Complete |
| 3 | Single-node indexing and query engine | Complete |
| 4 | Multi-node cluster formation and metadata coordination | Complete |
| 5 | Distributed ingestion, partitioning, and replication | Complete |
| 6 | Distributed query fan-out and result aggregation | Complete |
| 7 | Observability, deployment, and reliability | Complete |
| 8 | Stretch goals and resume polish | Not started |

See [`docs/planning/IMPLEMENTATION_PLAN.md`](docs/planning/IMPLEMENTATION_PLAN.md) for full phase descriptions and success criteria.

---

## Docs

- [`docs/planning/IMPLEMENTATION_PLAN.md`](docs/planning/IMPLEMENTATION_PLAN.md) — phase roadmap
- [`docs/planning/ARCHITECTURE_NOTES.md`](docs/planning/ARCHITECTURE_NOTES.md) — design decisions and system model
- [`docs/planning/BACKLOG.md`](docs/planning/BACKLOG.md) — executable checklist
