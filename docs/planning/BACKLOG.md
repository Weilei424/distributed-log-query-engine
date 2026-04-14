# Backlog

## Status Legend
- [ ] Not started
- [~] In progress
- [x] Complete

---

## Phase 1 — Project Foundation and System Design

### Status: In progress

- [x] Repository initialized with initial commit
- [x] `IMPLEMENTATION_PLAN.md` created in `docs/planning/`
- [x] `ARCHITECTURE_NOTES.md` created in `docs/planning/`
- [x] `CLAUDE.md` created with coding standards, working rules, and phase execution rules
- [ ] Repository folder structure created (`cmd/`, `internal/`, `pkg/`, `proto/`, `deployments/`, `test/`)
- [ ] Go module initialized (`go.mod`, `go.sum`)
- [ ] Makefile with targets: `make test`, `make lint`, `make run-local`
- [ ] Protobuf definitions for log ingestion and query API (`proto/`)
- [ ] Initial gRPC service contracts for ingest and query
- [ ] `pkg/types` package with core `LogEntry` type
- [ ] Basic README updated with setup instructions and project scope
- [ ] Docker Compose file for local multi-node development (`deployments/docker-compose/`)
- [ ] Confirm `make run-local` starts a single node successfully

---

## Phase 2 — Single Node Ingestion and Storage Engine

- [ ] `internal/storage` package: segment file struct and append logic
- [ ] Segment rotation by size threshold
- [ ] Segment rotation by time window
- [ ] Segment header and metadata format defined and versioned
- [ ] Write-ahead behavior: data is durable after local append
- [ ] `cmd/node` binary wired to storage package
- [ ] gRPC ingest endpoint (`Ingest` RPC) accepting `LogEntry` records
- [ ] Validation of log entry schema on ingest
- [ ] `received_at` timestamp assigned on ingest
- [ ] Node restart restores segments and does not lose written logs
- [ ] Unit tests: segment append, read-back correctness
- [ ] Unit tests: segment rotation trigger
- [ ] `make test` passes

---

## Phase 3 — Single Node Indexing and Query Engine

- [ ] `internal/index` package: in-memory inverted index
- [ ] Index updated on every successful segment append
- [ ] Keyword token extraction and lookup
- [ ] Time range index for segment-level pruning
- [ ] Optional service-name to segment mapping
- [ ] Query parser for keyword and time range parameters
- [ ] `internal/query` package: local query executor
- [ ] Result sorting by timestamp
- [ ] Pagination support (limit and offset or cursor)
- [ ] gRPC query endpoint (`Query` RPC) wired to local index and segment scan
- [ ] Index stays consistent with newly ingested data
- [ ] Unit tests: index insert and keyword lookup
- [ ] Unit tests: time range pruning correctness
- [ ] Unit tests: query parser
- [ ] Integration test: ingest then query on single node returns correct results
- [ ] `make test` passes

---

## Phase 4 — Multi-Node Cluster Formation and Metadata Coordination

- [ ] `internal/metadata` package: cluster state and node registry
- [ ] Node self-registration on startup
- [ ] Heartbeat mechanism for liveness tracking
- [ ] Shard ownership map stored in metadata layer
- [ ] Raft-backed metadata leader election (HashiCorp Raft or equivalent)
- [ ] Metadata leader serves cluster state reads
- [ ] Node rejoin behavior after restart
- [ ] `cmd/coordinator` binary or coordinator role within node binary
- [ ] Cluster status endpoint or CLI view showing healthy and unhealthy nodes
- [ ] Unit tests: shard ownership assignment logic
- [ ] Integration test: three-node cluster forms and shows all nodes in registry
- [ ] Integration test: node restart rejoins cluster correctly
- [ ] `make test` passes

---

## Phase 5 — Distributed Ingestion, Partitioning, and Replication

- [ ] `internal/ingest` package: routing logic for shard placement
- [ ] Hash-based shard routing by service name or stream key
- [ ] Write forwarded to primary shard owner
- [ ] `internal/replication` package: async replica write
- [ ] Primary acknowledges after local durable append
- [ ] Replica receives copy of written log entry asynchronously
- [ ] Documented consistency tradeoff for v1 async replication
- [ ] Node restart triggers replica catch-up if lagging
- [ ] Unit tests: shard routing determinism
- [ ] Integration test: logs distributed across nodes match partitioning strategy
- [ ] Integration test: replica receives copy of primary write
- [ ] Integration test: single node failure does not make all relevant logs unavailable
- [ ] `make test` passes

---

## Phase 6 — Distributed Query Fan-Out and Result Aggregation

- [ ] `internal/coordinator` package: fan-out query orchestration
- [ ] Coordinator resolves relevant shards and target nodes from metadata
- [ ] Parallel query dispatch to storage nodes
- [ ] Per-node deadline and timeout enforcement
- [ ] Partial result collection with partial-success flag
- [ ] Merge, sort, and paginate aggregated results
- [ ] Response includes result count, latency, and partial-success metadata
- [ ] Query coordinator debug logs showing: nodes targeted, nodes responded, timeouts, merge duration
- [ ] Unit tests: merge and sort logic correctness
- [ ] Unit tests: partial result handling when one node times out
- [ ] Integration test: end-to-end distributed query returns results from multiple nodes
- [ ] Integration test: one unavailable node returns partial result with flag set
- [ ] `make test` passes

---

## Phase 7 — Observability, Deployment, and Reliability

- [ ] Prometheus metrics exposed by all long-running services
- [ ] Metrics: ingestion rate, ingestion failure count, append latency
- [ ] Metrics: active segment size, number of mounted segments, index token count
- [ ] Metrics: local query latency, fan-out latency, timeout count, partial response count
- [ ] Metrics: heartbeat health status, replication lag
- [ ] Structured logs with stable keys: timestamp, level, component, node_id, request_id, shard_id, message
- [ ] Request IDs propagated through write and query paths
- [ ] Grafana dashboard: ingestion throughput, query p50/p95 latency, node health, segment growth, error counts
- [ ] Dockerized services for all node types
- [ ] Docker Compose updated for full multi-node local cluster with Prometheus and Grafana
- [ ] Kubernetes manifests or Helm chart for cluster deployment demo (`deployments/kubernetes/`)
- [ ] Load test scripts for ingestion and query benchmarking
- [ ] Failure scenario: node crash with observable recovery behavior
- [ ] `make run-local` starts full stack locally
- [ ] `make test` passes

---

## Phase 8 — Stretch Goals and Resume Polish

- [ ] Bloom filters for segment skipping during query pruning
- [ ] Compaction job for merging or archiving older segments
- [ ] More expressive query language: AND, OR, field filters
- [ ] Query result caching for repeated requests
- [ ] Multi-tenant namespace or isolation support
- [ ] Architecture diagram added to docs
- [ ] README polished for public portfolio use
- [ ] Resume bullets drafted based on measurable project outcomes
- [ ] At least one advanced feature benchmarked with before/after numbers

---

## Done

- [x] Repository created
- [x] `IMPLEMENTATION_PLAN.md` written
- [x] `ARCHITECTURE_NOTES.md` written
- [x] `CLAUDE.md` written with coding standards and working rules
