# Backlog

## Status Legend
- [ ] Not started
- [~] In progress
- [x] Complete

---

## Phase 1 — Project Foundation and System Design

### Status: Complete

**Plan:** `docs/superpowers/plans/2026-04-14-phase1-foundation.md`
**Spec:** `docs/superpowers/specs/2026-04-14-phase1-foundation-design.md`

- [x] Repository initialized with initial commit
- [x] `IMPLEMENTATION_PLAN.md` created in `docs/planning/`
- [x] `ARCHITECTURE_NOTES.md` created in `docs/planning/`
- [x] `CLAUDE.md` created with coding standards, working rules, and phase execution rules
- [x] `go.mod` initialized at `github.com/Weilei424/distributed-log-query-engine`, Go 1.22
- [x] `.gitignore` updated with build outputs and local data paths
- [x] `pkg/types/log_entry.go` — plain Go `LogEntry` struct (decoupled from proto)
- [x] `internal/*/doc.go` placeholders for all 10 internal packages
- [x] `cmd/node/main.go`, `cmd/coordinator/main.go`, `cmd/cli/main.go` stubs
- [x] `Makefile` with `build`, `test`, `lint`, `run-local`, `proto`, `proto-lint` targets
- [x] `.golangci.yml` with errcheck, govet, staticcheck, unused, gofmt
- [x] `proto/buf.yaml` — buf module config with DEFAULT lint and FILE breaking rules
- [x] `proto/logengine/v1/log_entry.proto` — LogEntry message
- [x] `proto/logengine/v1/ingest.proto` — IngestService with Ingest and IngestBatch RPCs
- [x] `proto/logengine/v1/query.proto` — QueryService with Query RPC
- [x] `buf.gen.yaml` at repo root targeting `internal/api/gen/`
- [x] `buf generate` produces Go bindings under `internal/api/gen/logengine/v1/`
- [x] `go mod tidy` adds grpc and protobuf dependencies
- [x] `deployments/docker-compose/Dockerfile.node` and `Dockerfile.coordinator` stubs
- [x] `deployments/docker-compose/docker-compose.yml` — 3 node + 1 coordinator stubs
- [x] `deployments/kubernetes/.gitkeep`, `test/integration/.gitkeep`, `test/fixtures/.gitkeep`
- [x] `README.md` updated with architecture overview, prerequisites, setup commands, phase table
- [x] `make build` passes — `go build ./...` exits 0
- [x] `make test` passes — exits 0 (no tests yet)
- [x] `make lint` passes — no lint errors
- [x] `make proto-lint` passes — no proto lint errors
- [x] `docker compose config` validates Docker Compose YAML

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
