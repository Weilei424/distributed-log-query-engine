# Phase 6: Distributed Query Fan-Out and Result Aggregation — Design Spec

**Date:** 2026-04-26
**Phase:** 6
**Status:** Approved

---

## 1. Goal

Enable the coordinator binary to execute queries across all storage nodes in parallel, merge and deduplicate partial results, and return a single coherent response to the client — with a clear `partial` flag when one or more nodes did not respond within the deadline.

---

## 2. Architecture Overview

The coordinator binary gains a second gRPC service: `QueryService` (already defined in `proto/logengine/v1/query.proto`). A new `FanOutQueryServer` in `internal/coordinator/` implements this service and delegates to a `FanOutExecutor`.

The coordinator already holds authoritative cluster state through the Raft FSM (`fsm.State()` returns `metadata.ClusterState`). No `StateCache` polling is needed on the coordinator side.

Fan-out targets **all healthy nodes** for every query. This is required because shard ownership reassignment in Phase 5 does not migrate physical data, so a node may hold data for shards it no longer owns according to cluster metadata. Querying only shard primaries would miss those entries.

No proto changes are needed. The existing `QueryRequest` / `QueryResponse` with `partial bool` is sufficient.

---

## 3. New Files

### `internal/coordinator/fanout.go`

`FanOutExecutor` — the fan-out and collection core.

**Dependencies:**
- `ClusterStateProvider` interface: `State() metadata.ClusterState` — satisfied by `*metadata.FSM`.
- `nodeClientPool` (see `node_client.go`) for cached gRPC connections.
- Per-node timeout: configurable via `NODE_QUERY_TIMEOUT_MS` env var (default 5000 ms).
- Fan-out limit: configurable via `FAN_OUT_LIMIT` env var (default 1000). Overrides the client's `limit` in requests sent to nodes so the global merge has enough data to correctly apply the client's `offset` and `limit`.

**Execute flow:**
1. Snapshot cluster state via `fsm.State()`.
2. Filter to nodes where `Status == NodeHealthy` and `Address != ""`.
3. Log targeted nodes (IDs and addresses).
4. Allocate a buffered channel of `len(healthyNodes)`.
5. Spawn one goroutine per healthy node. Each goroutine:
   - Derives a child context with the per-node deadline.
   - Dials or reuses the cached `QueryServiceClient` for the node's address.
   - Overrides `req.Limit` with `fanOutLimit` before sending.
   - Sends the node result (entries + error) into the channel.
6. Wait for all goroutines via `sync.WaitGroup`, then drain the channel.
7. Log which nodes responded, which timed out or errored.
8. Call `MergeResults`, log merge duration.
9. Return merged result.

### `internal/coordinator/merge.go`

`MergeResults(parts []nodeResult) mergeOutput` — pure function, no external dependencies.

**`nodeResult`:**
```go
type nodeResult struct {
    nodeID  string
    entries []*types.LogEntry
    total   int32
    err     error
}
```

**`mergeOutput`:**
```go
type mergeOutput struct {
    entries []*types.LogEntry
    total   int32
    partial bool
}
```

**Steps:**
1. Combine entries from all non-errored node results.
2. Deduplicate by `entry.ID` using a seen-set (`map[string]struct{}`). First occurrence wins.
3. Sort: timestamp descending, entry ID ascending as tie-breaker (matches local executor sort).
4. `total` = deduplicated candidate count before pagination (a lower bound — see Decision 7).
5. Apply client `offset` then `limit`.
6. `partial = true` if any node result had a non-nil `err`.

**Note on `total`:** `total` is always a lower bound. Each node returns at most `max(fan_out_limit, offset+limit)` entries; if a node's true match set exceeds that window, `total` will undercount. When `partial=true`, failed nodes are also excluded, making `total` a lower bound of a lower bound. See Architecture Notes Decision 7.

### `internal/coordinator/query_server.go`

`FanOutQueryServer` implementing `logengine.QueryServiceServer`.

Thin adapter:
- Validates `limit >= 0` and `offset >= 0`.
- Calls `FanOutExecutor.Execute(ctx, req)`.
- Converts `mergeOutput` to `logengine.QueryResponse` (entries, total, partial, took_ms).

Same structure as `internal/query/server.go` on the storage node side.

### `internal/coordinator/node_client.go`

gRPC client pool for `QueryService` connections to storage nodes.

- Keyed by address (`map[string]logengine.QueryServiceClient`).
- Lazy-initialized, protected by `sync.Mutex`.
- Not evicted within a process lifetime (cluster membership is stable within a query lifetime).
- Mirrors the client cache in `internal/ingest/orchestrator.go`.

### `cmd/coordinator/main.go` (modified)

Wire the new components:
- Instantiate `FanOutExecutor` with `fsm` and node client pool.
- Instantiate `FanOutQueryServer`.
- Call `logengine.RegisterQueryServiceServer(grpcSrv, fanOutQuerySrv)` before `grpcSrv.Serve`.
- Read `NODE_QUERY_TIMEOUT_MS` and `FAN_OUT_LIMIT` from env with defaults.

---

## 4. Fan-Out Limit Rationale

Each storage node applies its own `limit` before returning results. If a client requests `limit=10, offset=20`, and the coordinator sends `limit=10` to 3 nodes, each node returns up to 10 entries. A global sort across those 30 entries may not include the true global entries at positions 20–30.

By sending `fanOutLimit` (default 1000) to each node instead, the coordinator ensures enough candidate entries are available for correct global pagination. The client's original `limit` and `offset` are applied only after the global merge.

---

## 5. Debug Logging

Using `log.Printf` with stable prefixes. Required log events per query:
- `fanout: targeting N nodes: [id=addr, ...]`
- `fanout: node <id> responded: <N> entries`
- `fanout: node <id> timed out` or `fanout: node <id> error: <msg>`
- `fanout: merge took <Xms>, total=<N>, partial=<bool>`

---

## 6. Environment Variables (coordinator)

| Variable | Default | Purpose |
|---|---|---|
| `NODE_QUERY_TIMEOUT_MS` | `5000` | Per-node query deadline in milliseconds |
| `FAN_OUT_LIMIT` | `1000` | Limit sent to each node during fan-out |

---

## 7. Testing

### Unit tests (`internal/coordinator/`)

| Test | Verifies |
|---|---|
| `TestMergeResults_Sort` | Entries from 3 nodes sorted timestamp desc, ID asc tie-breaker |
| `TestMergeResults_Dedup` | Same entry ID from two nodes appears exactly once |
| `TestMergeResults_Pagination` | Offset and limit applied correctly after global sort |
| `TestMergeResults_Partial` | One node error → `partial=true`, other entries still present |
| `TestFanOutExecutor` | Two in-process gRPC servers, two-node ClusterState, verify merged response |

### Integration tests (`test/integration/`)

| Test | Verifies |
|---|---|
| `TestDistributedQuery_AllNodes` | Ingest to multiple nodes, query via coordinator, merged results contain entries from all nodes |
| `TestDistributedQuery_PartialFailure` | One node shut down, query via coordinator returns `partial=true` with live node results |

### Validation
- `make test` passes
- `make lint` passes

---

## 8. Out of Scope for Phase 6

- Replica fallback on primary timeout (Phase 7+ consideration)
- Streaming query responses
- Query result caching
- Service-filter-based fan-out targeting (fan-out to all nodes remains correct for v1)
- Data migration when shards are reassigned

---

## 9. Definition of Done

- `FanOutExecutor`, `MergeResults`, `FanOutQueryServer`, and `node_client.go` implemented
- All unit tests pass
- Both integration tests pass
- `cmd/coordinator/main.go` registers `QueryService`
- `make test` and `make lint` pass
- `docs/planning/BACKLOG.md` Phase 6 checklist updated to reflect completed items
