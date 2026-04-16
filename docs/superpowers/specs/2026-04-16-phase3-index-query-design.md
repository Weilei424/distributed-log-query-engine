# Phase 3 Design — Single Node Indexing and Query Engine

**Date:** 2026-04-16
**Phase:** 3
**Status:** Approved

---

## Overview

Phase 3 adds indexing and query execution to the single-node system built in Phase 2. After this phase a node can accept a gRPC `Query` RPC, use an in-memory inverted index to prune candidate segments, read matching records from disk, filter and sort them, and return a paginated response.

The query proto contract (`QueryRequest` / `QueryResponse`) was defined in Phase 1 and is unchanged.

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Index persistence across restart | None — rebuild on startup | Keeps v1 simple; persistence is a future upgrade |
| Keyword tokenization | Lowercase, strip non-alphanumeric chars | Case-insensitive matching with no dependencies |
| Index granularity | Segment-level pointers | Sufficient for small bounded segments; record-level offsets are a future upgrade |
| Keyword filter at query time | `strings.Contains` after index prune | Index prunes candidates; filter ensures correctness |

---

## New Packages and Files

### `internal/index`

Owns the in-memory inverted index. No proto dependencies.

**`index.go`**

```
Index struct
  mu              sync.RWMutex
  tokenSegments   map[string]map[string]struct{}   // token → set of segment paths
  serviceSegments map[string]map[string]struct{}   // service → set of segment paths
  segmentMeta     map[string]SegmentMeta           // path → {MinTime, MaxTime int64}

SegmentMeta struct
  MinTime int64
  MaxTime int64
```

**`func NewIndex() *Index`** — initializes all maps.

**`func (idx *Index) Add(entry *types.LogEntry, segmentPath string)`**
- Tokenizes `entry.Message`: `strings.ToLower`, split on non-alphanumeric characters, skip empty tokens.
- Registers each token in `tokenSegments[token]`.
- Registers `entry.Service` in `serviceSegments` if non-empty.
- Updates `segmentMeta[segmentPath].MinTime` and `MaxTime` from `entry.Timestamp`.
- Thread-safe: acquires write lock.

**`func (idx *Index) Resolve(keyword, service string, startTime, endTime int64) []string`**
- Starts with the full set of known segment paths (all keys in `segmentMeta`).
- If `keyword` is non-empty: intersect with `tokenSegments[token]` for each token in the keyword. If any token has no segments, return empty immediately.
- If `service` is non-empty: intersect with `serviceSegments[service]`.
- If `startTime > 0` or `endTime > 0`: prune segments where `meta.MaxTime < startTime` or `meta.MinTime > endTime`.
- Returns sorted slice of remaining paths (sort ensures deterministic scan order).
- Thread-safe: acquires read lock.

**`func (idx *Index) RebuildFromSegments(paths []string, readFn func(string) ([]*types.LogEntry, error)) error`**
- Iterates `paths` in order.
- Calls `readFn(path)` for each; returns wrapped error on failure.
- Calls `Add(entry, path)` for every returned entry.
- Called once at node startup before the gRPC server begins accepting traffic.

---

### `internal/storage` — Read Path Additions

**`func ReadSegment(path string) ([]*types.LogEntry, error)`** — package-level function.
- Opens the segment file read-only.
- Iterates records using the existing `ReadRecord` function from `record.go`.
- Deserializes each record as a proto `LogEntry`, converts to `*types.LogEntry` via a `protoToEntry` helper (private, mirrors the one in `ingest/server.go`).
- Returns all entries in file order.
- Returns an empty slice (no error) for a zero-byte segment.

**`func (m *Manager) ReadSegments(paths []string) ([]*types.LogEntry, error)`** — method on `Manager`.
- Calls `ReadSegment` for each path.
- Concatenates results in path order.
- Returns a wrapped error if any segment read fails.

**`func (m *Manager) ActiveSegmentPath() string`** — method on `Manager`.
- Returns the absolute path of the currently active segment.
- Acquires the manager mutex; safe for concurrent use.

---

### `pkg/types` — New Query Types

**`query.go`**

```go
type QueryRequest struct {
    Keyword   string
    Service   string
    StartTime int64  // Unix nanoseconds; 0 = unbounded
    EndTime   int64  // Unix nanoseconds; 0 = unbounded
    Limit     int32  // 0 = use server default (100)
    Offset    int32
}

type QueryResult struct {
    Entries []*LogEntry
    Total   int32  // count before limit/offset
    TookMs  int64
}
```

Decoupled from proto, same pattern as `LogEntry`.

---

### `internal/query`

**`executor.go`** — `LocalExecutor`

```go
type LocalExecutor struct {
    index   *index.Index
    manager *storage.Manager
}

func NewLocalExecutor(idx *index.Index, manager *storage.Manager) *LocalExecutor
```

**`func (e *LocalExecutor) Execute(ctx context.Context, req *types.QueryRequest) (*types.QueryResult, error)`**

1. Validate: clamp `Limit` to 100 if zero; return error if `Offset` is negative.
2. Call `e.index.Resolve(req.Keyword, req.Service, req.StartTime, req.EndTime)` → candidate segment paths.
3. Call `e.manager.ReadSegments(paths)` → raw entries.
4. Filter entries:
   - If `Keyword` non-empty: `strings.Contains(strings.ToLower(entry.Message), strings.ToLower(req.Keyword))`.
   - If `Service` non-empty: `entry.Service == req.Service`.
   - If `StartTime > 0`: `entry.Timestamp >= req.StartTime`.
   - If `EndTime > 0`: `entry.Timestamp <= req.EndTime`.
5. Sort filtered entries by `Timestamp` descending.
6. Set `Total = int32(len(filtered))`.
7. Apply `Offset` and `Limit` slice.
8. Record wall-clock duration; set `TookMs`.
9. Return `&types.QueryResult{Entries, Total, TookMs}`.

**`server.go`** — `QueryServer`

```go
type QueryServer struct {
    logengine.UnimplementedQueryServiceServer
    executor *LocalExecutor
}

func NewQueryServer(executor *LocalExecutor) *QueryServer
```

`Query(ctx, req *logengine.QueryRequest) (*logengine.QueryResponse, error)`
- Translates proto `QueryRequest` → `types.QueryRequest`.
- Calls `executor.Execute(ctx, req)`.
- Translates `types.QueryResult` → proto `QueryResponse`.
- Returns gRPC `Internal` status on executor error.

---

## Changes to Existing Files

### `internal/ingest/server.go`

- `Server` gains a new field: `idx *index.Index`.
- `NewServer` signature becomes `NewServer(manager *storage.Manager, idx *index.Index) *Server`.
- After a successful `manager.Append(entry)` call, add: `s.idx.Add(entry, s.manager.ActiveSegmentPath())`.
- `IngestBatch` is unchanged in behavior; the per-entry `Ingest` call handles index updates.

### `cmd/node/main.go`

New startup sequence (after manager is ready, before gRPC server starts listening):

1. `idx := index.NewIndex()`
2. `if err := idx.RebuildFromSegments(manager.SegmentPaths(), storage.ReadSegment); err != nil { ... }`
3. `ingestSrv := ingest.NewServer(manager, idx)`
4. `querySrv := query.NewQueryServer(query.NewLocalExecutor(idx, manager))`
5. Register both servers on the gRPC server.

---

## Data Flow

### Write path (updated)
```
gRPC Ingest → ingest.Server.Ingest()
  → storage.Manager.Append()          [persist to segment]
  → index.Index.Add(entry, segPath)   [update in-memory index]
```

### Query path (new)
```
gRPC Query → query.QueryServer.Query()
  → types.QueryRequest
  → LocalExecutor.Execute()
      → index.Resolve()               [prune to candidate segments]
      → manager.ReadSegments()        [read entries from disk]
      → filter + sort + paginate      [in memory]
  → types.QueryResult
  → proto QueryResponse
```

### Startup (updated)
```
cmd/node/main.go
  → storage.NewManager()
  → index.NewIndex()
  → index.RebuildFromSegments()       [warm index before accepting traffic]
  → register ingest + query servers
  → grpc.Serve()
```

---

## Error Handling

| Scenario | Behavior |
|----------|----------|
| `ReadSegment` fails on one segment | `ReadSegments` returns a wrapped error; `Execute` returns gRPC `Internal` |
| Index `Resolve` returns empty set | `Execute` returns empty result, not an error |
| `Offset` exceeds `Total` | Returns empty `Entries`, correct `Total` |
| `RebuildFromSegments` fails at startup | Node logs error and exits; it cannot serve correct queries with a partial index |
| Keyword has no matching token in index | `Resolve` returns empty immediately; no disk I/O |

---

## Testing Plan

### Unit tests — `internal/index/index_test.go`
- `Add` then `Resolve` returns the correct segment path for a matching token.
- `Resolve` with a non-matching token returns empty.
- `Resolve` with time range prunes segments whose bounds do not overlap.
- `Resolve` with service filter returns only matching segments.
- `RebuildFromSegments` produces the same state as manual `Add` calls.
- Concurrent `Add` and `Resolve` do not race (run with `-race`).

### Unit tests — `internal/storage/read_test.go`
- `ReadSegment` on a file written by `Segment.Append` returns all entries in order.
- `ReadSegment` on an empty segment returns an empty slice without error.
- `ReadSegment` on a nonexistent path returns a wrapped error.

### Unit tests — `internal/query/executor_test.go`
- `Execute` with a keyword returns only entries whose message contains it.
- `Execute` with a time range excludes out-of-bounds entries.
- `Execute` with `Limit=2, Offset=1` returns the correct page.
- `Execute` returns entries sorted by timestamp descending.
- `Execute` with no filters returns all entries.

### Integration test — `test/integration/query_test.go`
- Ingest entries with distinct keywords, services, and timestamps via gRPC.
- Query by keyword — verify only matching entries returned.
- Query by time range — verify boundary correctness.
- Query by service — verify filter works.
- Combined keyword + service + time range — verify all filters apply together.
- Restart the node; re-run all queries — verify index rebuilds and results match.

---

## Out of Scope for Phase 3

- Index persistence / snapshot (future upgrade)
- Record-level byte offset pointers in the index (future upgrade)
- Boolean query operators (AND / OR) — Phase 8 stretch goal
- Query result caching — Phase 8 stretch goal
- Distributed query fan-out — Phase 6
