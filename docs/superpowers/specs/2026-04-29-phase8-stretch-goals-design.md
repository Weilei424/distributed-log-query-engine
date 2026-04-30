# Phase 8 — Stretch Goals and Resume Polish: Design Spec

**Date:** 2026-04-29
**Phase:** 8
**Status:** Approved

---

## Overview

Phase 8 adds six technical features that increase the system's depth and interview value, followed by a portfolio polish pass. All phases 1–7 are complete. The system is fully functional: multi-node ingestion, distributed query fan-out, Raft-backed coordination, replication, and observability all work end-to-end.

Phase 8 does not change the core architecture. It extends existing packages with opt-in features and adds one background worker, one parser, one coordinator cache, and one streaming RPC.

---

## Build Order

Technical features are built first, in dependency order. Portfolio polish follows as a single pass.

1. Namespace support (routing-level)
2. Expressive query language (boolean query string parser)
3. Bloom filters (optional persisted sidecar)
4. Compaction (merge + retention background worker)
5. Query result caching (TTL + LRU on coordinator)
6. Segment file transfer catch-up
7. Portfolio polish pass

---

## Feature 1: Namespace Support (Routing-Level)

### Summary

Namespace is a first-class string field on `LogEntry`. It participates in shard routing. No storage-level separation — logs from different namespaces may coexist in the same segment file and are filtered at query time.

### Changes

**`pkg/types/log_entry.go`**
- Add `Namespace string` field to `LogEntry`

**`proto/logengine/v1/log_entry.proto`**
- Add `namespace` field to `LogEntry` message

**`proto/logengine/v1/ingest.proto`**
- No change (namespace flows through `LogEntry`)

**`proto/logengine/v1/query.proto`**
- Add `namespace` field to `QueryRequest`

**`internal/ingest/router.go`**
- Change shard ID computation: `hash(namespace + ":" + service) % total_shards`

**`internal/ingest/server.go`**
- Propagate namespace from proto to `LogEntry` via `ProtoToEntry`

**`internal/index/index.go`**
- Index entries carry namespace; keyword lookups accept optional namespace filter

**`internal/query/executor.go`**
- Apply namespace filter before keyword matching

**`internal/coordinator/fanout.go`**
- Pass namespace from coordinator `QueryRequest` to per-node `QueryRequest`

### Behavior

- Logs without a namespace field are treated as belonging to the empty namespace `""`
- Shard routing changes are backward-compatible: existing single-namespace deployments route identically if namespace is always `""`
- Queries without a namespace filter match all namespaces

### Tests

- Unit: router determinism with namespace in key
- Unit: index namespace filter — query with namespace returns only matching entries
- Integration: ingest logs into two namespaces, query each namespace, assert no cross-contamination

---

## Feature 2: Expressive Query Language (Boolean Query String Parser)

### Summary

A new parser in `internal/query/parser.go` converts a query string into a filter AST. The executor walks the AST against the index and applies field filters during the segment scan pass.

### Grammar

```
query     = or_expr
or_expr   = and_expr ( "OR" and_expr )*
and_expr  = atom ( "AND" atom )*
atom      = "(" or_expr ")" | field_term | bare_term
field_term = IDENT ":" IDENT
bare_term  = IDENT
```

- AND binds tighter than OR
- Operators are uppercase keywords
- Bare terms match against the full tokenized message
- Field terms match against specific `LogEntry` fields: `level`, `service`, `namespace`, `message`

### AST Node Types

```go
type AndNode  struct { Left, Right Node }
type OrNode   struct { Left, Right Node }
type TermNode struct { Token string }
type FieldNode struct { Field, Value string }
```

### Proto Change

**`proto/logengine/v1/query.proto`**
- Replace `keyword string` with `query_string string` in `QueryRequest`
- Backward compatibility: empty `query_string` behaves as match-all

### Changes

- `internal/query/parser.go` — tokenizer + recursive descent parser
- `internal/query/executor.go` — AST walker replacing single-keyword lookup
- `pkg/types/query.go` — update `Query` struct: `QueryString string` replaces `Keyword string`
- `internal/query/server.go` — map proto `query_string` to `Query.QueryString`
- `internal/coordinator/fanout.go` — propagate `query_string` to node requests
- `internal/coordinator/cache.go` (Feature 5) — cache key uses `query_string`

### Bloom Filter Integration

For AND nodes, all required `TermNode`/`FieldNode` values are checked against the bloom filter. A segment is skipped only if at least one required term is a definite miss. OR nodes are treated conservatively: skip only if every branch is a definite miss.

### Tests

- Unit: parser handles bare terms, field filters, AND, OR, grouping, empty string
- Unit: parser returns error on malformed input
- Unit: executor with AND/OR AST returns correct entries from test segments
- Unit: bloom skip logic for AND vs OR nodes

---

## Feature 3: Bloom Filters (Optional Persisted Sidecar)

### Summary

Each closed segment gets a `.bloom` sidecar file at rotation time. During query pruning, the executor checks bloom filters before deciding to scan a segment. Controlled by `BLOOM_ENABLED` env var (default `false`).

### File Layout

```
data/
  00000000000000000001.seg
  00000000000000000001.bloom   ← sidecar, written at rotation
  00000000000000000002.seg
  00000000000000000002.bloom
  00000000000000000003.seg     ← active, no sidecar yet
```

### Bloom Filter Parameters

- Hash functions: 5 (default)
- Target false-positive rate: 1% at expected segment token count
- Expected token count estimated from segment byte size at rotation time
- Library: `github.com/bits-and-blooms/bloom/v3`

### Changes

**`internal/storage/bloom.go`** (new)
- `BuildBloom(tokens []string) *bloom.BloomFilter`
- `WriteBloom(path string, bf *bloom.BloomFilter) error`
- `ReadBloom(path string) (*bloom.BloomFilter, error)`
- `BloomPath(segPath string) string` — returns `.bloom` sidecar path for a given `.seg` path

**`internal/storage/segment.go`**
- On rotation (close of active segment): if `BLOOM_ENABLED`, build bloom from all tokens in the closing segment and write sidecar

**`internal/storage/manager.go`**
- On startup: for each closed segment, if `BLOOM_ENABLED` and sidecar exists, load bloom into memory map keyed by segment path
- Expose `BloomFor(segPath string) *bloom.BloomFilter` method

**`internal/query/executor.go`**
- Before scanning a segment, call `BloomFor` and run bloom check per AST node (see Feature 2 integration above)

### Benchmark Target

The bloom filter is the primary benchmark for Phase 8:

- Script: `test/bench/bloom_benchmark.sh`
- Ingests a fixed dataset, runs N queries with `BLOOM_ENABLED=false`, then with `BLOOM_ENABLED=true`
- Reports: segments scanned per query, average query latency
- Results committed to `docs/benchmarks/bloom-filter-results.md`

### Tests

- Unit: `BuildBloom` contains inserted tokens, rejects non-inserted tokens with expected FP rate
- Unit: `WriteBloom` / `ReadBloom` round-trip
- Unit: `BloomPath` returns correct path
- Unit: executor skips segment when bloom returns definite miss for all required terms
- Integration: ingest → rotate → verify sidecar file exists → query with bloom enabled → assert correct results

---

## Feature 4: Compaction (Merge + Retention)

### Summary

A background worker runs two passes over closed segments on a configurable interval. Merge pass combines small segments. Retention pass deletes old segments. Both are configurable and independently disableable.

### Config (env vars)

| Variable | Default | Meaning |
|---|---|---|
| `COMPACT_INTERVAL_SECONDS` | `300` | How often the worker runs |
| `COMPACT_MERGE_THRESHOLD_BYTES` | `33554432` (32MB) | Segments smaller than this are merge candidates. `0` disables merging |
| `COMPACT_RETENTION_DAYS` | `7` | Segments whose newest entry is older than this are deleted. `0` disables retention |

### Merge Pass

1. List all closed segments, sorted chronologically
2. Collect contiguous runs of segments all below threshold
3. For each run of 2+ eligible segments: read all records, write to a new segment file, fsync
4. If `BLOOM_ENABLED`: build bloom from merged token set, write sidecar for new segment
5. Delete original segment files and their `.bloom` sidecars
6. Update in-memory index: remap entries from old segment paths to new segment path

### Retention Pass

1. For each closed segment, find the maximum timestamp among its entries
2. If `now - max_timestamp > retention_days * 86400s`: delete segment file and `.bloom` sidecar
3. Remove index entries pointing to deleted segments

### Locking

The worker acquires a write lock on the storage manager during each pass. Append operations block briefly. The lock is held for the duration of one pass (merge or retention), not both together.

### Changes

- `internal/storage/compaction.go` (new) — `Compactor` struct with `Start(ctx)` / `Stop()`
- `internal/storage/manager.go` — expose `ListClosedSegments()`, `RemapSegment()`, `DeleteSegment()`, `LoadSegment(path string)` methods for compaction and catch-up use
- `cmd/node/main.go` — instantiate and start `Compactor` alongside the storage manager

### Tests

- Unit: merge pass combines two small segments into one, deletes originals
- Unit: merge updates bloom sidecar when bloom is enabled
- Unit: retention pass deletes segments older than threshold, leaves newer ones
- Unit: retention with `COMPACT_RETENTION_DAYS=0` deletes nothing
- Integration: ingest → rotate multiple segments → run compaction → query returns same results

---

## Feature 5: Query Result Caching (TTL + LRU on Coordinator)

### Summary

The `FanOutExecutor` checks an in-memory cache before dispatching queries. Cache is keyed by normalized query parameters. Only non-partial results are cached.

### Cache Key

SHA-256 hash of the concatenation of:
- `query_string`
- `namespace`
- `time_from` (int64, nanoseconds)
- `time_to` (int64, nanoseconds)
- `limit` (int32)
- `offset` (int32)

All fields normalized before hashing (empty string for unset fields, 0 for unset integers).

### Config (env vars)

| Variable | Default | Meaning |
|---|---|---|
| `QUERY_CACHE_TTL_SECONDS` | `30` | Entry TTL |
| `QUERY_CACHE_MAX_ENTRIES` | `256` | Max cached entries before LRU eviction |

### Implementation

`internal/coordinator/cache.go` (new)
- Doubly-linked list + map — standard LRU pattern, no external dependency
- `Get(key string) (*CachedResult, bool)` — returns entry if present and not expired
- `Put(key string, result *CachedResult)` — inserts or updates, evicts LRU if at capacity
- `CachedResult` holds `QueryResponse` + `insertedAt time.Time`

`internal/coordinator/fanout.go`
- Before fan-out: compute key, call `cache.Get`; on hit return immediately
- After fan-out: if result is not partial, call `cache.Put`

### Tests

- Unit: cache hit returns cached result without calling fan-out
- Unit: expired entry (past TTL) is treated as miss
- Unit: LRU eviction when at max capacity
- Unit: partial results are not cached
- Integration: repeated identical query returns same results; second call measurably faster

---

## Feature 6: Segment File Transfer Catch-Up

### Summary

When a replica restarts and is missing entire closed segment files, it requests full file transfers from the primary via a streaming `TransferSegment` RPC instead of fetching entries one-by-one. Entry-level catch-up handles only the trailing active segment.

### New Proto RPC

**`proto/logengine/v1/ingest.proto`**

```protobuf
rpc TransferSegment(TransferSegmentRequest) returns (stream TransferSegmentResponse);

message TransferSegmentRequest {
  string segment_name = 1; // e.g. "00000000000000000002.seg"
  string shard_id     = 2;
}

message TransferSegmentResponse {
  bytes chunk = 1;
}
```

Chunk size: 64KB.

### Catch-Up Logic (`internal/ingest/catchup.go`)

Updated `runCatchUp` flow:

1. Call primary's `ListSegments` RPC (new, returns list of closed segment names for a shard) to get the primary's closed segment list
2. Compare against local closed segment files
3. For each segment present on primary but absent locally: call `TransferSegment`, stream chunks, write to local segment directory, fsync
4. After all missing closed segments are transferred, load them into the local storage manager and rebuild index entries for those segments
5. Proceed with existing entry-level catch-up for the active segment only

### New Proto RPC

**`ListSegments`** (also added to `IngestService`):

```protobuf
rpc ListSegments(ListSegmentsRequest) returns (ListSegmentsResponse);

message ListSegmentsRequest { string shard_id = 1; }
message ListSegmentsResponse { repeated string segment_names = 1; }
```

### Changes

- `proto/logengine/v1/ingest.proto` — add `ListSegments` and `TransferSegment` RPCs
- `internal/ingest/server.go` — implement `ListSegments` and `TransferSegment` handlers
- `internal/ingest/catchup.go` — updated `runCatchUp` with file-transfer path
- `internal/storage/manager.go` — `LoadSegment(path string)` already added in Feature 4; no additional change needed
- `cmd/node/main.go` — no change to startup sequence; catch-up already runs before serving

### Tests

- Unit: `ListSegments` returns only closed segment names
- Unit: `TransferSegment` streams all bytes of a segment file in fixed-size chunks
- Integration: replica restarted after missing two full closed segments → file transfer occurs → query returns all data

---

## Portfolio Polish Pass

Executed after all six technical features are complete.

### Architecture Diagram

- File: `docs/architecture/diagram.svg` (or `.png`)
- Shows: coordinator cluster (Raft), storage nodes (segments + index + bloom), ingest path (router → shard owner → replicator), query path (fan-out → merge → cache), namespace routing
- Committed to repo; embedded in README

### README Rewrite

Target audience: a senior engineer viewing the repo for the first time.

Sections:
1. One-paragraph project summary
2. Architecture diagram
3. Feature table (what's built, why it's interesting)
4. Prerequisites and `make run-local` quickstart
5. Tradeoffs and design decisions (3–5 bullets)
6. Phase summary table

Remove: internal implementation notes, agent instructions, in-progress status language.

### Bloom Filter Benchmark

- Script: `test/bench/bloom_benchmark.sh`
- Protocol: ingest 50,000 log entries across 10 segments, run 100 queries
- Run once with `BLOOM_ENABLED=false`, once with `BLOOM_ENABLED=true`
- Metrics: segments scanned per query (from metrics endpoint or log output), p50/p95 query latency
- Results: `docs/benchmarks/bloom-filter-results.md`

### Resume Bullets

File: `docs/planning/RESUME_BULLETS.md`

4–6 bullets in resume format, grounded in measurable outcomes from the project. Examples:

- Built a distributed log query engine in Go across N nodes with Raft-backed coordination, segment-based storage, and distributed query fan-out
- Implemented bloom filter-based segment pruning that reduced segment scans by X% (measured)
- Added a boolean query language parser (AND/OR/field filters) with a recursive descent parser and AST-driven index lookups
- Designed and implemented namespace-aware shard routing, per-tenant query isolation, and TTL+LRU query result caching
- Built a background compaction worker with configurable merge and retention policies over an append-only segment store

Actual numbers filled in after the benchmark runs.

---

## Definition of Done

Phase 8 is complete when:

- All six technical features have implementation, tests, and pass `make test` and `make lint`
- `BLOOM_ENABLED=true` and `BLOOM_ENABLED=false` both produce correct query results
- Compaction preserves query correctness (integration test)
- Namespace isolation prevents cross-namespace query leakage (integration test)
- Benchmark script runs and results are committed
- README is rewritten for a public audience with the architecture diagram embedded
- Resume bullets are committed to `docs/planning/RESUME_BULLETS.md`
- `docs/planning/BACKLOG.md` is fully updated
