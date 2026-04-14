# Phase 2 — Single Node Ingestion and Storage Engine

**Date:** 2026-04-14
**Phase:** 2 of 8
**Status:** Approved

---

## Overview

Phase 2 builds the core single-node log ingestion path and persistent storage layer. By the end of this phase a single node accepts log entries over gRPC, persists them durably to append-only segment files on disk, and survives a restart without losing any written records.

---

## Goals

- Implement `internal/storage` with segment-based append-only persistence
- Implement `internal/ingest` with a gRPC `IngestService` server
- Wire `cmd/node` into a real long-running process with graceful shutdown
- Prove durability: restart the node and confirm all records are still on disk
- All unit and integration tests pass under `make test`

---

## Non-Goals

- No indexing or query support (Phase 3)
- No cluster coordination or shard routing (Phase 4+)
- No replication (Phase 5)
- No Prometheus metrics (Phase 7)
- No time-based segment rotation (deferred to Phase 7)
- No `internal/config` package — env vars read directly in `main.go`
- No gRPC dial-level integration test — that comes in Phase 3

---

## Package Structure

```
internal/
  storage/
    record.go          # 4-byte length-prefix framing (read/write)
    record_test.go
    segment.go         # single open segment file
    segment_test.go
    manager.go         # active segment ownership, rotation, restart recovery
    manager_test.go
  ingest/
    server.go          # gRPC IngestServiceServer implementation
    server_test.go     # integration test (no gRPC wire)
cmd/
  node/
    main.go            # env config, gRPC server, graceful shutdown
test/
  integration/
    ingest_test.go     # end-to-end storage + ingest integration test
```

---

## Segment File Format

### Directory layout

```
data/
├── 00000000000000000001.seg   ← closed segment
├── 00000000000000000002.seg   ← closed segment
└── 00000000000000000003.seg   ← active segment (most recent)
```

Segment filenames are zero-padded 20-digit sequence numbers. The manager increments the counter on each rotation. On startup, `NewManager` scans the directory for `*.seg` files, sorts them lexicographically, and reopens the highest-numbered one as the active segment (or creates `00000000000000000001.seg` if none exist).

### Record layout

```
┌─────────────────┬──────────────────────────────────┐
│  4 bytes        │  N bytes                         │
│  uint32 BE      │  protobuf-serialized LogEntry     │
│  (record len N) │                                  │
└─────────────────┴──────────────────────────────────┘
```

No file header in Phase 2. The length-prefix format is self-describing — any reader can scan forward record by record. A file header with magic bytes and schema version can be added in Phase 7 when the format is declared stable.

### Rotation

When `Manager.Append` is called and the active segment's size would exceed `maxSegmentBytes` after the write, the current segment is closed and a new one with the next sequence number is opened before writing.

Default `maxSegmentBytes`: 67108864 (64 MB).

---

## Storage Package

### record.go

Pure I/O framing logic. No file handles or state.

```go
// WriteRecord writes data to w as a length-prefixed record.
// Format: [4-byte big-endian uint32 length][data bytes]
func WriteRecord(w io.Writer, data []byte) error

// ReadRecord reads one length-prefixed record from r.
// Returns io.EOF when r is exhausted.
func ReadRecord(r io.Reader) ([]byte, error)
```

### segment.go

Owns a single open `*os.File`. Calls `file.Sync()` after every append for durability.

```go
type Segment struct { /* unexported fields */ }

// OpenSegment opens or creates the segment file at path.
// Seeks to end of file so appends do not overwrite existing data.
func OpenSegment(path string) (*Segment, error)

// Append frames data using WriteRecord and syncs to disk.
func (s *Segment) Append(data []byte) error

// Size returns the current byte size of the segment file.
func (s *Segment) Size() int64

// Close closes the underlying file.
func (s *Segment) Close() error
```

### manager.go

Owns the data directory and the active segment. Thread-safe via a mutex.

```go
type Manager struct { /* unexported fields */ }

// NewManager opens or creates dir, scans for existing *.seg files,
// and reopens the most recent segment as active.
func NewManager(dir string, maxSegmentBytes int64) (*Manager, error)

// Append serializes entry to protobuf, appends to the active segment,
// and rotates if the size threshold is crossed.
func (m *Manager) Append(entry *types.LogEntry) error

// SegmentPaths returns the absolute paths of all segment files in
// sequence order. Used by the query path in Phase 3.
func (m *Manager) SegmentPaths() []string

// Close closes the active segment.
func (m *Manager) Close() error
```

---

## Ingest Package

### server.go

Implements the generated `logengine.IngestServiceServer` interface.

```go
type Server struct {
    logengine.UnimplementedIngestServiceServer
    manager *storage.Manager
}

func NewServer(manager *storage.Manager) *Server
```

**`Ingest` behavior:**
1. Validate: `entry.Service` and `entry.Message` must be non-empty; return `codes.InvalidArgument` otherwise
2. Assign `entry.ReceivedAt = time.Now().UnixNano()`
3. Convert proto `*logengine.LogEntry` → `types.LogEntry` via `protoToEntry`
4. Call `manager.Append`
5. Return `&IngestResponse{Id: entry.Id, Ok: true}` on success

**`IngestBatch` behavior:**
- Loop over entries, apply `Ingest` logic to each
- Count accepted vs rejected
- Return `&IngestBatchResponse{Accepted: n, Rejected: m}`
- Does not short-circuit on individual entry failure

**`protoToEntry` helper** (unexported, in server.go):
Converts `*logengine.LogEntry` → `types.LogEntry`. Keeps `internal/storage` free of proto imports.

---

## cmd/node

Replaces the Phase 1 stub. Reads configuration from environment variables:

| Env var | Default | Purpose |
|---------|---------|---------|
| `NODE_ID` | `node-local` | Node identifier for logs |
| `DATA_DIR` | `./data` | Directory for segment files |
| `GRPC_PORT` | `:50051` | gRPC listen address |
| `MAX_SEGMENT_BYTES` | `67108864` | Segment rotation threshold (bytes) |

**Startup sequence:**
1. Parse env vars
2. `storage.NewManager(dataDir, maxSegmentBytes)`
3. `ingest.NewServer(manager)`
4. `grpc.NewServer()`, register `IngestService`
5. `net.Listen("tcp", grpcPort)`
6. Log `node started: id=<NODE_ID> addr=<GRPC_PORT>`
7. `server.Serve(listener)` — blocks
8. On `SIGINT`/`SIGTERM`: `manager.Close()`, `server.GracefulStop()`

---

## Testing

### Unit tests

**record_test.go**
- Round-trip: write a record to `bytes.Buffer`, read it back, assert byte equality
- Empty buffer read returns `io.EOF`
- Truncated length header returns error
- Truncated data returns error

**segment_test.go**
- Append one record, close, reopen, read back — assert contents match
- `Size()` grows by expected amount after each append
- Multiple appends survive close + reopen in order

**manager_test.go**
- Append N records, `Close`, `NewManager` on same dir — all N records present
- Rotation: set `maxSegmentBytes=128`, append enough records to trigger rotation, assert two `*.seg` files exist
- `SegmentPaths()` returns paths in ascending sequence order
- Empty dir: `NewManager` creates `00000000000000000001.seg`

### Integration tests

**test/integration/ingest_test.go**
- Create `storage.Manager` in `t.TempDir()`
- Create `ingest.Server` backed by it
- Call `server.Ingest` and `server.IngestBatch` directly (no gRPC wire)
- Close manager, reopen, scan segments — confirm all entries are present and field values match

---

## Success Criteria

- [ ] `make build` passes
- [ ] `make test` passes — all unit and integration tests green
- [ ] `make lint` passes
- [ ] Node restart does not lose written log entries
- [ ] Segment rotation creates a new `*.seg` file when threshold is crossed
- [ ] `grpcurl` or equivalent can write log entries to the live node
- [ ] `BACKLOG.md` Phase 2 items updated
