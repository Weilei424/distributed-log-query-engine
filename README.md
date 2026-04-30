# Distributed Log Query Engine

A distributed log storage and query system built in Go. Designed to demonstrate core distributed systems concepts — sharded ingestion, segment-based storage, Raft-backed coordination, and parallel query fan-out — in a codebase small enough to explain end-to-end.

## Architecture

[Architecture diagram](docs/architecture/diagram.md)

| Component | Responsibility |
|-----------|---------------|
| **Ingest path** | Validates entries, routes by `hash(namespace:service) % shards`, forwards to primary node |
| **Storage node** | Append-only segment files, in-memory inverted index, optional bloom filter sidecars |
| **Coordinator (Raft)** | Node registry, shard ownership map, leader election, query fan-out |
| **Query executor** | Boolean query parser (AND/OR, field:value), index-accelerated local search |
| **Background workers** | Configurable merge compaction, retention eviction, replica catch-up via segment transfer |

## Features

| Feature | Details |
|---------|---------|
| Boolean query language | `error AND level:ERROR`, `timeout OR "connection failed"`, field filters |
| Namespace isolation | Tenant-aware shard routing; namespace filter propagated through full fan-out path |
| Bloom filter pruning | Optional `.bloom` sidecar per segment; eliminates segments with guaranteed no-match |
| Query result cache | TTL + LRU on coordinator; skips fan-out for repeated identical queries |
| Segment compaction | Merge pass (size threshold) + retention pass (age cutoff), both configurable via env |
| Segment file transfer | `ListSegments` + streaming `TransferSegment` RPC for replica catch-up |
| Partial results | Coordinator marks response `partial=true` when any node times out; never blocks |
| Observability | Prometheus metrics on all nodes and coordinator; structured zap logs with request IDs |

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
| 8 | Stretch goals and resume polish | Complete |

See [`docs/planning/IMPLEMENTATION_PLAN.md`](docs/planning/IMPLEMENTATION_PLAN.md) for full phase descriptions and success criteria.

---

## Docs

- [`docs/planning/IMPLEMENTATION_PLAN.md`](docs/planning/IMPLEMENTATION_PLAN.md) — phase roadmap
- [`docs/planning/ARCHITECTURE_NOTES.md`](docs/planning/ARCHITECTURE_NOTES.md) — design decisions and system model
- [`docs/planning/BACKLOG.md`](docs/planning/BACKLOG.md) — executable checklist
- [`docs/architecture/diagram.md`](docs/architecture/diagram.md) — Mermaid architecture diagram
- [`docs/benchmarks/bloom-filter-results.md`](docs/benchmarks/bloom-filter-results.md) — bloom filter benchmark results

## Design Tradeoffs

- **Async replication** — the primary acknowledges writes before the replica confirms, favouring ingest throughput over strict durability. Under primary failure before replication, the replica may be behind.
- **In-memory index** — the inverted index lives in RAM and is rebuilt from segments on restart. This bounds startup time by data size but avoids a persistent index dependency.
- **Partial query results** — when a storage node times out, the coordinator returns what it has with `partial=true`. This keeps queries fast under degraded conditions at the cost of completeness.
- **Bloom filters as optional sidecar** — enabled via `BLOOM_ENABLED=true`; off by default to keep the common case simple. Sidecars are written atomically on segment rotation.
- **Namespace routing, not isolation** — namespace affects shard placement and is a filter predicate, not a hard storage boundary. Cross-namespace queries on a single node are possible.
