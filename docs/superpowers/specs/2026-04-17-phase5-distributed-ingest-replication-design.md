# Phase 5 Design: Distributed Ingestion, Partitioning, and Replication

**Date:** 2026-04-17
**Phase:** 5
**Status:** Approved

---

## Overview

Phase 5 distributes log writes across nodes using hash-based shard routing and adds one-replica asynchronous replication. Any node can receive a write from a client; if it does not own the target shard it forwards the write to the primary owner. After a successful local write, the primary asynchronously replicates the entry to its assigned replica node. Nodes that restart after downtime run a lightweight catch-up before accepting traffic.

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Routing location | Each storage node | Any node routes to the primary owner. Realistic for interviews; mirrors Kafka broker redirect behavior. |
| Ingestion architecture | Separate `IngestionOrchestrator` | Thin RPC handler, distributed logic isolated in orchestrator. Industry-standard pattern (Kafka `ReplicaManager`, TiKV `RaftStore`). |
| Replica assignment | Stored in FSM, one replica per shard | Explicit and observable. Assigned during `CmdRegisterNode`; cleared during `CmdMarkUnhealthy`. |
| Replication RPC | Dedicated `ReplicateEntry` RPC on `IngestService` | Clean separation from client-facing `Ingest`. No forwarding loops. Replica bypasses routing logic. |
| Replication durability | Async, best-effort | Primary acknowledges after local durable append. Documented gap: entries undelivered to replica at primary crash time are lost. |
| Catch-up on restart | Lightweight entry fetch | Replica asks primary for entries since its latest local `received_at` timestamp per shard. Runs before node accepts traffic. |
| Catch-up v2 (deferred) | Segment file transfer | Full segment file transfer for nodes down for extended periods. Deferred to Phase 8. |

---

## Component Map

```
internal/
  ingest/
    router.go          — ShardID(service, totalShards) pure hash function
    orchestrator.go    — IngestionOrchestrator: routing, forwarding, replication trigger
    server.go          — thin gRPC adapter; delegates to orchestrator
  cluster/
    state_cache.go     — polls GetClusterState from coordinator; exposes ClusterStateReader interface
  metadata/
    state.go           — ShardRecord gains ReplicaNode string field
    fsm.go             — applyRegisterNode assigns replicas; applyMarkUnhealthy clears them
  replication/
    replicator.go      — async worker: buffered channel per replica, background drain goroutine
proto/
  logengine/v1/
    ingest.proto       — add ReplicateEntry RPC and FetchShardEntries RPC
cmd/
  node/
    main.go            — wire state cache, replicator, new orchestrator constructor; run catch-up before serving
test/
  integration/
    phase5_routing_test.go
    phase5_replication_test.go
    phase5_failure_test.go
    phase5_catchup_test.go
```

---

## Section 1: Data Model Changes

### `ShardRecord`

```go
type ShardRecord struct {
    ShardID     int
    PrimaryNode string // node ID; empty if unowned
    ReplicaNode string // node ID; empty if no replica assigned
}
```

Single replica slot for v1. Expanding to multiple replicas later requires only a struct change and updated FSM logic.

### FSM: `applyRegisterNode`

Existing behavior: claim all unowned shards as primary.

New behavior (appended after primary assignment):
- For each shard where the registering node is NOT the primary AND the shard has no replica:
  → assign the registering node as `ReplicaNode`

Deterministic — no new Raft command types required.

### FSM: `applyMarkUnhealthy`

Existing behavior: clear primary ownership and release shards.

New behavior (appended):
- For each shard where the unhealthy node is the `ReplicaNode`:
  → clear `ReplicaNode` to empty string

---

## Section 2: Shard Router

**File:** `internal/ingest/router.go`

```go
func ShardID(service string, totalShards int) int
```

- Uses FNV-1a hash over the service name, modulo `totalShards`
- Pure function — no state, no coordination
- Deterministic: given the same inputs on any node, produces the same shard ID

---

## Section 3: Cluster State Cache

**File:** `internal/cluster/state_cache.go`

Storage nodes do not have direct FSM access. The cache polls `GetClusterState` from the coordinator at a configurable interval (default 5s) and holds the result in memory.

```go
type ClusterStateReader interface {
    State() metadata.ClusterState
}
```

The orchestrator reads from this interface. The write path never blocks on a live coordinator RPC.

If the coordinator is unreachable during a poll, the cache retains its last known state and logs a warning.

---

## Section 4: IngestionOrchestrator

**File:** `internal/ingest/orchestrator.go`

Owns all distributed write logic. The ingest `Server` calls `orchestrator.HandleIngest(ctx, req)` for every incoming write.

### Local write path (this node is primary for the target shard)

1. Write entry to local storage via `manager.AppendWithPath`
2. Update local index via `idx.Add`
3. Enqueue async replication to replica node via `replicator.Enqueue`
4. Return success

### Forward path (this node is not primary)

1. Compute `shardID = ShardID(entry.Service, totalShards)`
2. Look up primary node address from `ClusterStateReader`
3. Dial primary node and call its `Ingest` RPC
4. Return primary's response to the caller
5. No local write, no replication (primary handles both)

### No primary available

Return `codes.Unavailable`. Client retries.

### Ingest `Server` after refactor

```go
func (s *Server) Ingest(ctx context.Context, req *logengine.IngestRequest) (*logengine.IngestResponse, error) {
    return s.orchestrator.HandleIngest(ctx, req)
}
```

All existing validation (nil entry, empty service, empty message) moves into the orchestrator before routing.

---

## Section 5: Replication

### Proto additions — `ingest.proto`

```proto
rpc ReplicateEntry(ReplicateEntryRequest) returns (ReplicateEntryResponse);
rpc FetchShardEntries(FetchShardEntriesRequest) returns (FetchShardEntriesResponse);

message ReplicateEntryRequest {
    LogEntry entry    = 1;
    int32   shard_id = 2;
}

message ReplicateEntryResponse {
    bool ok = 1;
}

message FetchShardEntriesRequest {
    int32 shard_id      = 1;
    int64 since_unix_ns = 2;
}

message FetchShardEntriesResponse {
    repeated LogEntry entries = 1;
}
```

### Replicator — `internal/replication/replicator.go`

- One buffered channel per known replica address
- One background goroutine per channel draining entries and sending `ReplicateEntry` RPCs
- `Enqueue(entry, replicaAddr)` is non-blocking; drops and logs if channel is full
- On shutdown: drain in-flight entries with a short deadline, then close connections

### `ReplicateEntry` handler on `Server`

- Bypasses the orchestrator entirely
- Writes directly to local storage + index
- Defensively verifies `shard_id` matches a shard this node owns as replica
- Returns `codes.FailedPrecondition` if shard ownership does not match

### Consistency tradeoff (documented)

Primary acknowledges after local durable append. Replica write is asynchronous. In the window between primary write and replica delivery, a primary crash loses undelivered entries. This is the v1 durability gap — explicitly documented, not hidden.

---

## Section 6: Catch-up on Restart

### `FetchShardEntries` handler on `Server`

The primary handles this request by:
1. Scanning local segments for entries where `ShardID(entry.Service, totalShards) == shard_id` and `entry.ReceivedAt > since_unix_ns`
2. Returning entries in `received_at` ascending order

Full scan acceptable for v1 — this only runs at node startup, not on the hot write path.

### Catch-up sequence in `cmd/node/main.go`

After re-registration and before the gRPC server begins accepting traffic:

1. Fetch current cluster state from coordinator
2. For each shard where this node is the replica:
   a. Find latest `ReceivedAt` in local storage for that shard
   b. Call `FetchShardEntries` on the primary with `since_unix_ns = latestLocalTimestamp`
   c. Write received entries to local storage + index via the same path as `ReplicateEntry`
3. If primary is unreachable: log warning, skip catch-up for that shard, continue startup
4. Start accepting traffic

### Phase 8 note

Segment file transfer catch-up deferred to Phase 8. Transfers closed segment files wholesale — faster for nodes down for extended periods. Relevant when compaction and segment lifecycle management are in place.

---

## Section 7: Testing

### Unit tests

**`internal/ingest/router_test.go`**
- Same service + same total shards → same shard ID (determinism)
- Distribution across shard range (no trivial clustering)
- Zero-length service name does not panic

**`internal/metadata/fsm_test.go`** (extend existing)
- Two nodes register: second node assigned as replica for first node's primary shards
- Node marked unhealthy: `ReplicaNode` cleared on affected shards
- Replica assignment survives snapshot → restore cycle

**`internal/replication/replicator_test.go`**
- `Enqueue` is non-blocking when channel has capacity
- Entry delivered to replica RPC handler
- Full channel drops entry and logs — does not block caller

### Integration tests

| File | Scenario |
|------|----------|
| `phase5_routing_test.go` | Ingest to non-owning node; entry appears in primary node's storage |
| `phase5_replication_test.go` | Ingest to primary; entry appears in replica node's storage after brief wait |
| `phase5_failure_test.go` | Primary stopped; replica still serves relevant logs |
| `phase5_catchup_test.go` | Replica stopped, primary ingests more, replica restarts, catch-up runs, all entries present |

---

## Success Criteria

- Logs are distributed across nodes according to hash-based shard routing
- Non-owning nodes forward writes to the correct primary without client coordination
- Replica nodes receive copies of primary writes asynchronously
- Losing the primary does not immediately make all relevant logs unavailable
- A restarted replica catches up before accepting traffic
- Partial failure behavior is predictable and documented
- All unit and integration tests pass (`make test`)
- Linting passes (`make lint`)
