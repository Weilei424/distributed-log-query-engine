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
- [x] `go.mod` initialized at `github.com/Weilei424/distributed-log-query-engine`, Go 1.24
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

**Plan:** `docs/superpowers/plans/2026-04-14-phase2-storage-ingest.md`
**Spec:** `docs/superpowers/specs/2026-04-14-phase2-storage-ingest-design.md`

### Status: Complete

- [x] `internal/storage/record.go` — `WriteRecord`/`ReadRecord` length-prefix framing
- [x] `internal/storage/segment.go` — `OpenSegment`, `Append` (with fsync), `Size`, `Close`
- [x] `internal/storage/manager.go` — `NewManager`, `Append`, `SegmentPaths`, `Close`, size-based rotation, restart recovery
- [x] Segment files named as zero-padded 20-digit sequence numbers (`*.seg`)
- [x] `internal/ingest/server.go` — `IngestService` gRPC server with validation and `ReceivedAt` assignment
- [x] `protoToEntry` helper in `internal/ingest/server.go` keeps storage free of proto API types
- [x] `cmd/node/main.go` — real gRPC listener, env var config, graceful shutdown on SIGINT/SIGTERM
- [x] Unit tests: `record_test.go`, `segment_test.go`, `manager_test.go`
- [x] Unit tests: `ingest/server_test.go` covering validation, batch counts
- [x] Integration test: `test/integration/ingest_test.go` — ingest then restart then verify all records on disk
- [x] `make build` passes
- [x] `make test` passes — all unit and integration tests green
- [x] `make lint` passes

---

## Phase 3 — Single Node Indexing and Query Engine

**Plan:** `docs/superpowers/plans/2026-04-16-phase3-index-query.md`
**Spec:** `docs/superpowers/specs/2026-04-16-phase3-index-query-design.md`

### Status: Complete

- [x] `internal/index` package: in-memory inverted index
- [x] Index updated on every successful segment append
- [x] Keyword token extraction (lowercase, strip non-alphanumeric) and lookup
- [x] Time range index for segment-level pruning
- [x] Service-name to segment mapping
- [x] `internal/query` package: local query executor
- [x] Query parser for keyword and time range parameters (`pkg/types/query.go`)
- [x] Result sorting by timestamp descending
- [x] Pagination support (limit and offset)
- [x] gRPC query endpoint (`Query` RPC) wired to local index and segment scan
- [x] Index rebuilt from segment files on node startup (before accepting traffic)
- [x] Index stays consistent with newly ingested data
- [x] Unit tests: index insert and keyword lookup
- [x] Unit tests: time range pruning correctness
- [x] Unit tests: storage read path (`ReadSegment`, `ReadSegments`, `ActiveSegmentPath`)
- [x] Unit tests: query executor (keyword, time range, pagination, sort, no-filter)
- [x] Integration test: ingest then query on single node returns correct results
- [x] Integration test: index rebuilds correctly after node restart
- [x] `make test` passes

---

## Phase 4 — Multi-Node Cluster Formation and Metadata Coordination

**Plan:** `docs/superpowers/plans/2026-04-16-phase4-cluster-metadata.md`
**Spec:** `docs/superpowers/specs/2026-04-16-phase4-cluster-metadata-design.md`

### Status: Complete

- [x] `internal/metadata/state.go` — NodeRecord, ShardRecord, ClusterState types with deep-copy clone
- [x] `internal/metadata/fsm.go` — Raft FSM: Apply, Snapshot, Restore; CmdRegisterNode, CmdUpdateHeartbeat, CmdMarkUnhealthy; deterministic shard assignment (timestamps in payload, not in Apply)
- [x] `internal/metadata/server.go` — gRPC ClusterService: RegisterNode, Heartbeat (leader-only), GetClusterState (any replica)
- [x] `internal/metadata/liveness.go` — leader-only liveness checker; marks stale nodes unhealthy and releases their shards
- [x] `proto/logengine/v1/cluster.proto` — ClusterService RPC definitions; buf generate produces Go bindings
- [x] `internal/cluster/client.go` — ClusterClient with multi-address round-robin on FAILED_PRECONDITION; ParseAddrs helper
- [x] `internal/cluster/heartbeat.go` — HeartbeatSender with Beater interface; ticker-based goroutine, stops on context cancel
- [x] `cmd/coordinator/main.go` — full coordinator binary: Raft bootstrap (BoltDB + TCP transport), gRPC ClusterService, HTTP /status, liveness checker, graceful shutdown
- [x] `cmd/node/main.go` — storage node updated: cluster registration with COORDINATOR_ADDRS, heartbeat sender goroutine, degraded mode if coordinator unreachable
- [x] `deployments/docker-compose/docker-compose.yml` — 3 coordinator services with Raft env vars + 3 node services with COORDINATOR_ADDRS
- [x] Unit tests: FSM RegisterNode, UpdateHeartbeat, MarkUnhealthy, SnapshotRestore (6 tests)
- [x] Unit test: HeartbeatSender stops cleanly on context cancel
- [x] Integration test: node registers and appears healthy in cluster state
- [x] Integration test: GetClusterState returns all registered nodes
- [x] Integration test: node restart rejoins cluster with shard reassignment
- [x] Integration test: missed heartbeats mark node unhealthy and release shards
- [x] `make test` passes
- [x] `make lint` passes
- [x] `make build` passes

---

## Phase 5 — Distributed Ingestion, Partitioning, and Replication

**Plan:** `docs/superpowers/plans/2026-04-17-phase5-distributed-ingest-replication.md`
**Spec:** `docs/superpowers/specs/2026-04-17-phase5-distributed-ingest-replication-design.md`

### Status: Complete

- [x] `internal/metadata/state.go` — add `ReplicaNode string` to `ShardRecord`
- [x] `internal/metadata/fsm.go` — replace greedy assignment with `rebalancePrimary()` + `assignReplicas()`; clear replica slots in `applyMarkUnhealthy`; `rebalancePrimary()` called in `applyMarkUnhealthy` so surviving nodes claim orphaned shards
- [x] `internal/metadata/server.go` — include `ReplicaNode` in `GetClusterState` response
- [x] `proto/logengine/v1/cluster.proto` — add `replica_node` field to `ShardInfo`
- [x] `proto/logengine/v1/ingest.proto` — add `ReplicateEntry` and `FetchShardEntries` RPCs
- [x] `buf generate` — regenerated Go bindings
- [x] `internal/ingest/router.go` — `ShardID(service, totalShards) int` using FNV-1a hash
- [x] `internal/ingest/convert.go` — exported `ProtoToEntry`, `EntryToProto`, `GenerateID`
- [x] `internal/ingest/orchestrator.go` — `IngestionOrchestrator`: routes, forwards, triggers async replication
- [x] `internal/ingest/server.go` — thin adapter; `ReplicateEntry` and `FetchShardEntries` handlers; `NewLocalServer` for single-node use
- [x] `internal/cluster/client.go` — add `GetClusterState` method and `protoToClusterState` helper
- [x] `internal/cluster/state_cache.go` — `ClusterStateReader` interface + `StateCache` polling implementation
- [x] `internal/replication/replicator.go` — async replication worker with per-replica buffered channel
- [x] `cmd/node/main.go` — wire state cache, replicator, orchestrator; `runCatchUp` before serving; `TOTAL_SHARDS` env var
- [x] Unit tests: `router_test.go` (determinism, range, distribution, empty)
- [x] Unit tests: `replicator_test.go` (delivery, non-blocking, clean stop)
- [x] Unit tests: `fsm_test.go` extended (two-node share, replica assignment, mark-unhealthy clears replica, surviving node claims primary shards)
- [x] Integration test: routing forwards write to correct primary node
- [x] Integration test: async replication delivers copy to replica node
- [x] Integration test: primary failure leaves data available on replica
- [x] Integration test: restarted replica fetches missed entries via `FetchShardEntries`
- [x] `make test` passes
- [x] `make lint` passes
- [x] `make build` passes

---

## Phase 6 — Distributed Query Fan-Out and Result Aggregation

**Plan:** `docs/superpowers/plans/2026-04-26-phase6-distributed-query-fanout.md`
**Spec:** `docs/superpowers/specs/2026-04-26-phase6-distributed-query-fanout-design.md`

### Status: Complete

- [x] Shard ownership reassignment (Phase 5 rebalancePrimary) does not migrate physical data; distributed queries fan-out to all nodes and merge to compensate
- [x] `pkg/types/query.go` — add `Partial bool` to `QueryResult`
- [x] `internal/query/server.go` — forward `result.Partial` in gRPC response
- [x] `internal/coordinator/merge.go` — `MergeResults`: sort, dedup by ID, paginate; `nodeResult` and `mergeOutput` types
- [x] `internal/coordinator/node_client.go` — lazy gRPC `QueryServiceClient` pool
- [x] `internal/coordinator/fanout.go` — `ClusterStateProvider` interface; `FanOutExecutor`: fan-out to all healthy nodes, collect via buffered channel, debug logging
- [x] `internal/coordinator/query_server.go` — `FanOutQueryServer`: thin gRPC adapter over `FanOutExecutor`
- [x] `cmd/coordinator/main.go` — wire `FanOutExecutor` and `FanOutQueryServer`; `NODE_QUERY_TIMEOUT_MS` and `FAN_OUT_LIMIT` env vars
- [x] Unit tests: `TestMergeResults_Sort`, `TestMergeResults_TieBreaker`, `TestMergeResults_Dedup`, `TestMergeResults_Pagination`, `TestMergeResults_Partial`, `TestMergeResults_AllFailed`
- [x] Unit tests: `TestFanOutExecutor_MergesFromTwoNodes`, `TestFanOutExecutor_SkipsUnhealthyNodes`, `TestFanOutExecutor_PartialOnNodeFailure`
- [x] Integration test: `TestDistributedQuery_AllNodes` — coordinator returns merged results from both nodes
- [x] Integration test: `TestDistributedQuery_PartialFailure` — one node down returns partial=true with surviving node's data
- [x] `test/integration/phase5_node_test.go` — register `QueryService` on test nodes; add `queryClient()` helper
- [x] `make test` passes
- [x] `make lint` passes

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
- [ ] Segment file transfer catch-up (Option C): transfer full closed segment files from primary to replica on restart, replacing entry-by-entry fetch for nodes down for extended periods

---

## Done

- [x] Repository created
- [x] `IMPLEMENTATION_PLAN.md` written
- [x] `ARCHITECTURE_NOTES.md` written
- [x] `CLAUDE.md` written with coding standards and working rules
