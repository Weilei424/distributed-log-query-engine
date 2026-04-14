# Phase 2 — Single Node Ingestion and Storage Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the core single-node log ingestion path and persistent storage layer so that a node accepts log entries over gRPC and survives restart without losing data.

**Architecture:** Log records are serialized to protobuf and written as length-prefixed binary records to append-only segment files on disk. The storage layer is split into three focused units: record framing, single-segment file management, and a manager that owns the active segment and handles rotation. The gRPC IngestService server sits on top of the storage manager and is wired into a real long-running process in cmd/node.

**Tech Stack:** Go 1.22, google.golang.org/grpc, google.golang.org/protobuf/proto, standard library (os, io, sync, encoding/binary)

---

## Prerequisites

All dependencies are already in `go.mod` from Phase 1. Verify:

```bash
cd /mnt/d/projects/distributed-log-query-engine
grep "google.golang.org/grpc\|google.golang.org/protobuf" go.mod
```

Expected: both lines present.

---

## Task 1: Record framing (record.go)

**Files:**
- Create: `internal/storage/record.go`
- Create: `internal/storage/record_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/storage/record_test.go`:

```go
package storage_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func TestWriteReadRecord_RoundTrip(t *testing.T) {
	data := []byte("hello world")
	var buf bytes.Buffer
	if err := storage.WriteRecord(&buf, data); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	got, err := storage.ReadRecord(&buf)
	if err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestReadRecord_EOF(t *testing.T) {
	var buf bytes.Buffer
	_, err := storage.ReadRecord(&buf)
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestWriteReadRecord_EmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := storage.WriteRecord(&buf, []byte{}); err != nil {
		t.Fatalf("WriteRecord empty: %v", err)
	}
	got, err := storage.ReadRecord(&buf)
	if err != nil {
		t.Fatalf("ReadRecord empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty payload, got %q", got)
	}
}

func TestReadRecord_TruncatedLength(t *testing.T) {
	buf := bytes.NewReader([]byte{0x00, 0x00}) // only 2 of 4 bytes
	_, err := storage.ReadRecord(buf)
	if err == nil || err == io.EOF {
		t.Fatalf("expected non-EOF error for truncated length, got %v", err)
	}
}

func TestReadRecord_TruncatedData(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x00, 0x00, 0x0a}) // length = 10
	buf.Write([]byte{0x01, 0x02, 0x03})        // only 3 bytes of data
	_, err := storage.ReadRecord(&buf)
	if err == nil {
		t.Fatal("expected error for truncated data, got nil")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /mnt/d/projects/distributed-log-query-engine && go test ./internal/storage/...
```

Expected: compile error — `storage.WriteRecord` and `storage.ReadRecord` undefined.

- [ ] **Step 3: Create internal/storage/record.go**

```go
package storage

import (
	"encoding/binary"
	"fmt"
	"io"
)

// WriteRecord writes data to w as a length-prefixed record.
// Format: [4-byte big-endian uint32 length][data bytes]
func WriteRecord(w io.Writer, data []byte) error {
	length := uint32(len(data))
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return fmt.Errorf("write record length: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write record data: %w", err)
	}
	return nil
}

// ReadRecord reads one length-prefixed record from r.
// Returns io.EOF when r is exhausted at a record boundary.
// Returns a wrapped error (not io.EOF) if the stream is truncated mid-record.
func ReadRecord(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err // propagates io.EOF cleanly at record boundaries
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("read record data (expected %d bytes): %w", length, err)
	}
	return data, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /mnt/d/projects/distributed-log-query-engine && go test ./internal/storage/... -run TestWriteReadRecord -v
```

Expected: all record tests PASS.

```bash
cd /mnt/d/projects/distributed-log-query-engine && go test ./internal/storage/... -v
```

Expected: all tests PASS.

**Suggested commit messages:**
- `internal/storage/record.go` — `feat: add length-prefix record framing for segment files`
- `internal/storage/record_test.go` — `test: add unit tests for record framing`

---

## Task 2: Segment file (segment.go)

**Files:**
- Create: `internal/storage/segment.go`
- Create: `internal/storage/segment_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/storage/segment_test.go`:

```go
package storage_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func TestSegment_AppendAndReadBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000000000000000001.seg")

	seg, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	data := []byte("test record")
	if err := seg.Append(data); err != nil {
		t.Fatalf("Append: %v", err)
	}
	seg.Close()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open for read: %v", err)
	}
	defer f.Close()

	got, err := storage.ReadRecord(f)
	if err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestSegment_SizeGrows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000000000000000001.seg")

	seg, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()

	if seg.Size() != 0 {
		t.Errorf("initial size: got %d, want 0", seg.Size())
	}

	data := []byte("hello")
	if err := seg.Append(data); err != nil {
		t.Fatalf("Append: %v", err)
	}

	want := int64(4 + len(data)) // 4-byte header + data
	if seg.Size() != want {
		t.Errorf("size after append: got %d, want %d", seg.Size(), want)
	}
}

func TestSegment_MultipleAppendsSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000000000000000001.seg")

	records := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	seg, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	for _, r := range records {
		if err := seg.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	seg.Close()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open for read: %v", err)
	}
	defer f.Close()

	for i, want := range records {
		got, err := storage.ReadRecord(f)
		if err != nil {
			t.Fatalf("ReadRecord[%d]: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("record[%d]: got %q, want %q", i, got, want)
		}
	}
}

func TestSegment_ReopenAppendsToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000000000000000001.seg")

	seg, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment first: %v", err)
	}
	if err := seg.Append([]byte("before")); err != nil {
		t.Fatalf("Append before: %v", err)
	}
	seg.Close()

	seg2, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment second: %v", err)
	}
	if err := seg2.Append([]byte("after")); err != nil {
		t.Fatalf("Append after: %v", err)
	}
	seg2.Close()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open for read: %v", err)
	}
	defer f.Close()

	for _, want := range [][]byte{[]byte("before"), []byte("after")} {
		got, err := storage.ReadRecord(f)
		if err != nil {
			t.Fatalf("ReadRecord: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /mnt/d/projects/distributed-log-query-engine && go test ./internal/storage/... -run TestSegment -v
```

Expected: compile error — `storage.OpenSegment` and `*storage.Segment` undefined.

- [ ] **Step 3: Create internal/storage/segment.go**

```go
package storage

import (
	"fmt"
	"io"
	"os"
)

// Segment represents a single open segment file.
type Segment struct {
	file *os.File
	size int64
}

// OpenSegment opens or creates the segment file at path.
// If the file already exists it seeks to the end so appends do not overwrite existing data.
func OpenSegment(path string) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat segment %s: %w", path, err)
	}
	size := info.Size()
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("seek segment %s: %w", path, err)
	}
	return &Segment{file: f, size: size}, nil
}

// Append frames data as a length-prefixed record and syncs to disk.
func (s *Segment) Append(data []byte) error {
	if err := WriteRecord(s.file, data); err != nil {
		return fmt.Errorf("segment append: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("segment sync: %w", err)
	}
	s.size += int64(4 + len(data))
	return nil
}

// Size returns the current byte size of the segment file.
func (s *Segment) Size() int64 {
	return s.size
}

// Close closes the underlying file.
func (s *Segment) Close() error {
	return s.file.Close()
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /mnt/d/projects/distributed-log-query-engine && go test ./internal/storage/... -v
```

Expected: all tests PASS.

**Suggested commit messages:**
- `internal/storage/segment.go` — `feat: add segment file with append and sync`
- `internal/storage/segment_test.go` — `test: add unit tests for segment file`

---

## Task 3: Storage manager (manager.go)

**Files:**
- Create: `internal/storage/manager.go`
- Create: `internal/storage/manager_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/storage/manager_test.go`:

```go
package storage_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

func makeEntry(i int) *types.LogEntry {
	return &types.LogEntry{
		ID:      fmt.Sprintf("id-%d", i),
		Service: "test-svc",
		Level:   "INFO",
		Message: fmt.Sprintf("message %d", i),
	}
}

func countRecords(t *testing.T, paths []string) int {
	t.Helper()
	total := 0
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open segment %s: %v", filepath.Base(path), err)
		}
		for {
			_, err := storage.ReadRecord(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("ReadRecord in %s: %v", filepath.Base(path), err)
			}
			total++
		}
		f.Close()
	}
	return total
}

func TestManager_AppendAndRestart(t *testing.T) {
	dir := t.TempDir()

	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	const n = 10
	for i := 0; i < n; i++ {
		if err := m.Append(makeEntry(i)); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager reopen: %v", err)
	}
	t.Cleanup(func() { m2.Close() })

	total := countRecords(t, m2.SegmentPaths())
	if total != n {
		t.Errorf("expected %d records after restart, got %d", n, total)
	}
}

func TestManager_Rotation(t *testing.T) {
	dir := t.TempDir()

	m, err := storage.NewManager(dir, 128) // tiny threshold
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	for i := 0; i < 20; i++ {
		if err := m.Append(makeEntry(i)); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}

	paths := m.SegmentPaths()
	if len(paths) < 2 {
		t.Errorf("expected at least 2 segment files after rotation, got %d", len(paths))
	}
}

func TestManager_SegmentPathsOrdered(t *testing.T) {
	dir := t.TempDir()

	m, err := storage.NewManager(dir, 128)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	for i := 0; i < 20; i++ {
		if err := m.Append(makeEntry(i)); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}

	paths := m.SegmentPaths()
	for i := 1; i < len(paths); i++ {
		if filepath.Base(paths[i]) <= filepath.Base(paths[i-1]) {
			t.Errorf("paths not ascending: %s >= %s",
				filepath.Base(paths[i]), filepath.Base(paths[i-1]))
		}
	}
}

func TestManager_EmptyDirCreatesFirstSegment(t *testing.T) {
	dir := t.TempDir()

	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	paths := m.SegmentPaths()
	if len(paths) != 1 {
		t.Errorf("expected 1 segment in new dir, got %d", len(paths))
	}
	if filepath.Base(paths[0]) != "00000000000000000001.seg" {
		t.Errorf("expected '00000000000000000001.seg', got %q", filepath.Base(paths[0]))
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /mnt/d/projects/distributed-log-query-engine && go test ./internal/storage/... -run TestManager -v
```

Expected: compile error — `storage.NewManager` undefined.

- [ ] **Step 3: Create internal/storage/manager.go**

```go
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

const segmentNameFmt = "%020d.seg"

// Manager owns the data directory and the active segment.
// It is safe for concurrent use.
type Manager struct {
	mu              sync.Mutex
	dir             string
	maxSegmentBytes int64
	active          *Segment
	nextSeq         uint64
	paths           []string
}

// NewManager opens or creates dir, scans for existing *.seg files,
// and reopens the most recent one as the active segment.
// Creates the first segment if the directory is empty.
func NewManager(dir string, maxSegmentBytes int64) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", dir, err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil {
		return nil, fmt.Errorf("glob segments: %w", err)
	}
	sort.Strings(matches)

	m := &Manager{
		dir:             dir,
		maxSegmentBytes: maxSegmentBytes,
		paths:           matches,
		nextSeq:         uint64(len(matches)) + 1,
	}

	if len(matches) == 0 {
		if err := m.openNewSegment(); err != nil {
			return nil, err
		}
	} else {
		seg, err := OpenSegment(matches[len(matches)-1])
		if err != nil {
			return nil, fmt.Errorf("reopen active segment: %w", err)
		}
		m.active = seg
	}

	return m, nil
}

// Append serializes entry to protobuf and appends to the active segment.
// Rotates to a new segment if the size threshold would be crossed.
func (m *Manager) Append(entry *types.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	pb := &logengine.LogEntry{
		Id:         entry.ID,
		Timestamp:  entry.Timestamp,
		ReceivedAt: entry.ReceivedAt,
		Service:    entry.Service,
		Level:      entry.Level,
		Message:    entry.Message,
		Fields:     entry.Fields,
	}

	data, err := proto.Marshal(pb)
	if err != nil {
		return fmt.Errorf("marshal log entry: %w", err)
	}

	recordSize := int64(4 + len(data))
	if m.active.Size()+recordSize > m.maxSegmentBytes {
		if err := m.rotate(); err != nil {
			return fmt.Errorf("rotate segment: %w", err)
		}
	}

	return m.active.Append(data)
}

// SegmentPaths returns the absolute paths of all segment files in sequence order.
func (m *Manager) SegmentPaths() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.paths))
	copy(out, m.paths)
	return out
}

// Close closes the active segment.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != nil {
		return m.active.Close()
	}
	return nil
}

func (m *Manager) openNewSegment() error {
	name := fmt.Sprintf(segmentNameFmt, m.nextSeq)
	path := filepath.Join(m.dir, name)
	seg, err := OpenSegment(path)
	if err != nil {
		return fmt.Errorf("open new segment %s: %w", name, err)
	}
	m.active = seg
	m.paths = append(m.paths, path)
	m.nextSeq++
	return nil
}

func (m *Manager) rotate() error {
	if err := m.active.Close(); err != nil {
		return fmt.Errorf("close active segment before rotation: %w", err)
	}
	return m.openNewSegment()
}
```

- [ ] **Step 4: Run go mod tidy to pick up any new transitive deps**

```bash
cd /mnt/d/projects/distributed-log-query-engine && go mod tidy
```

Expected: no errors. `go.mod` and `go.sum` may update slightly.

- [ ] **Step 5: Run all storage tests to verify they pass**

```bash
cd /mnt/d/projects/distributed-log-query-engine && go test ./internal/storage/... -v
```

Expected: all tests PASS.

**Suggested commit messages:**
- `internal/storage/manager.go` — `feat: add segment manager with rotation and restart recovery`
- `internal/storage/manager_test.go` — `test: add unit tests for segment manager`

---

## Task 4: Ingest gRPC server (ingest/server.go)

**Files:**
- Create: `internal/ingest/server.go`
- Create: `internal/ingest/server_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/ingest/server_test.go`:

```go
package ingest_test

import (
	"context"
	"testing"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func newTestServer(t *testing.T) *ingest.Server {
	t.Helper()
	m, err := storage.NewManager(t.TempDir(), 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return ingest.NewServer(m)
}

func TestIngest_Success(t *testing.T) {
	srv := newTestServer(t)
	resp, err := srv.Ingest(context.Background(), &logengine.IngestRequest{
		Entry: &logengine.LogEntry{
			Id:      "abc123",
			Service: "test-svc",
			Message: "hello",
		},
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if !resp.Ok {
		t.Error("expected Ok=true")
	}
	if resp.Id != "abc123" {
		t.Errorf("expected Id=abc123, got %q", resp.Id)
	}
}

func TestIngest_NilEntry(t *testing.T) {
	srv := newTestServer(t)
	_, err := srv.Ingest(context.Background(), &logengine.IngestRequest{})
	if err == nil {
		t.Fatal("expected error for nil entry")
	}
}

func TestIngest_MissingService(t *testing.T) {
	srv := newTestServer(t)
	_, err := srv.Ingest(context.Background(), &logengine.IngestRequest{
		Entry: &logengine.LogEntry{Message: "no service"},
	})
	if err == nil {
		t.Fatal("expected error for missing service")
	}
}

func TestIngest_MissingMessage(t *testing.T) {
	srv := newTestServer(t)
	_, err := srv.Ingest(context.Background(), &logengine.IngestRequest{
		Entry: &logengine.LogEntry{Service: "svc"},
	})
	if err == nil {
		t.Fatal("expected error for missing message")
	}
}

func TestIngest_ReceivedAtIsSet(t *testing.T) {
	srv := newTestServer(t)
	_, err := srv.Ingest(context.Background(), &logengine.IngestRequest{
		Entry: &logengine.LogEntry{Service: "svc", Message: "msg"},
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	// ReceivedAt is verified in the integration test via disk read-back
}

func TestIngestBatch_CountsAcceptedAndRejected(t *testing.T) {
	srv := newTestServer(t)
	resp, err := srv.IngestBatch(context.Background(), &logengine.IngestBatchRequest{
		Entries: []*logengine.LogEntry{
			{Id: "1", Service: "svc", Message: "ok"},
			{Id: "2", Message: "missing service"},  // rejected
			{Id: "3", Service: "svc", Message: "ok"},
		},
	})
	if err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	if resp.Accepted != 2 {
		t.Errorf("expected Accepted=2, got %d", resp.Accepted)
	}
	if resp.Rejected != 1 {
		t.Errorf("expected Rejected=1, got %d", resp.Rejected)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /mnt/d/projects/distributed-log-query-engine && go test ./internal/ingest/... -v
```

Expected: compile error — `ingest.Server` and `ingest.NewServer` undefined.

- [ ] **Step 3: Create internal/ingest/server.go**

```go
package ingest

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// Server implements the gRPC IngestServiceServer interface.
type Server struct {
	logengine.UnimplementedIngestServiceServer
	manager *storage.Manager
}

// NewServer creates a new ingest Server backed by the given storage manager.
func NewServer(manager *storage.Manager) *Server {
	return &Server{manager: manager}
}

// Ingest writes a single log entry to the storage layer.
func (s *Server) Ingest(ctx context.Context, req *logengine.IngestRequest) (*logengine.IngestResponse, error) {
	if req.Entry == nil {
		return nil, status.Error(codes.InvalidArgument, "entry is required")
	}
	if req.Entry.Service == "" {
		return nil, status.Error(codes.InvalidArgument, "entry.service is required")
	}
	if req.Entry.Message == "" {
		return nil, status.Error(codes.InvalidArgument, "entry.message is required")
	}

	req.Entry.ReceivedAt = time.Now().UnixNano()

	entry := protoToEntry(req.Entry)
	if err := s.manager.Append(entry); err != nil {
		return nil, status.Errorf(codes.Internal, "append failed: %v", err)
	}

	return &logengine.IngestResponse{Id: req.Entry.Id, Ok: true}, nil
}

// IngestBatch writes multiple log entries to the storage layer.
// Does not short-circuit on individual entry failure.
func (s *Server) IngestBatch(ctx context.Context, req *logengine.IngestBatchRequest) (*logengine.IngestBatchResponse, error) {
	var accepted, rejected int32
	for _, pb := range req.Entries {
		_, err := s.Ingest(ctx, &logengine.IngestRequest{Entry: pb})
		if err != nil {
			rejected++
		} else {
			accepted++
		}
	}
	return &logengine.IngestBatchResponse{Accepted: accepted, Rejected: rejected}, nil
}

// protoToEntry converts a proto LogEntry to the internal types.LogEntry.
// Keeps internal/storage free of direct proto API dependencies.
func protoToEntry(pb *logengine.LogEntry) *types.LogEntry {
	return &types.LogEntry{
		ID:         pb.Id,
		Timestamp:  pb.Timestamp,
		ReceivedAt: pb.ReceivedAt,
		Service:    pb.Service,
		Level:      pb.Level,
		Message:    pb.Message,
		Fields:     pb.Fields,
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /mnt/d/projects/distributed-log-query-engine && go test ./internal/ingest/... -v
```

Expected: all tests PASS.

**Suggested commit messages:**
- `internal/ingest/server.go` — `feat: add gRPC IngestService server backed by storage manager`
- `internal/ingest/server_test.go` — `test: add unit tests for ingest server`

---

## Task 5: Wire cmd/node

**Files:**
- Modify: `cmd/node/main.go`

- [ ] **Step 1: Replace cmd/node/main.go**

```go
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"google.golang.org/grpc"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func main() {
	nodeID := envOrDefault("NODE_ID", "node-local")
	dataDir := envOrDefault("DATA_DIR", "./data")
	grpcAddr := envOrDefault("GRPC_PORT", ":50051")
	maxSegBytes := envInt64OrDefault("MAX_SEGMENT_BYTES", 64*1024*1024)

	manager, err := storage.NewManager(dataDir, maxSegBytes)
	if err != nil {
		log.Fatalf("storage.NewManager: %v", err)
	}

	ingestSrv := ingest.NewServer(manager)

	grpcSrv := grpc.NewServer()
	logengine.RegisterIngestServiceServer(grpcSrv, ingestSrv)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("net.Listen %s: %v", grpcAddr, err)
	}

	fmt.Printf("node started: id=%s addr=%s data=%s\n", nodeID, grpcAddr, dataDir)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()

	<-stop
	fmt.Println("shutting down...")
	grpcSrv.GracefulStop()
	if err := manager.Close(); err != nil {
		log.Printf("manager close: %v", err)
	}
	fmt.Println("node stopped")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt64OrDefault(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}
```

- [ ] **Step 2: Verify make build passes**

```bash
cd /mnt/d/projects/distributed-log-query-engine && make build
```

Expected: no output, exit 0.

- [ ] **Step 3: Smoke test — start node and verify it logs startup message**

```bash
cd /mnt/d/projects/distributed-log-query-engine
DATA_DIR=/tmp/dlqe-smoke go run ./cmd/node &
NODE_PID=$!
sleep 1
kill $NODE_PID 2>/dev/null
rm -rf /tmp/dlqe-smoke
```

Expected output contains: `node started: id=node-local addr=:50051`

**Suggested commit message:**
- `cmd/node/main.go` — `feat: wire cmd/node with gRPC server, storage manager, and graceful shutdown`

---

## Task 6: Integration test

**Files:**
- Create: `test/integration/ingest_test.go`

- [ ] **Step 1: Create test/integration/ingest_test.go**

```go
package integration_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func TestIngestAndPersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: ingest entries
	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	srv := ingest.NewServer(m)

	entries := []*logengine.LogEntry{
		{Id: "e1", Service: "svc-a", Level: "INFO", Message: "first message"},
		{Id: "e2", Service: "svc-b", Level: "WARN", Message: "second message"},
		{Id: "e3", Service: "svc-a", Level: "ERROR", Message: "third message"},
	}

	for _, e := range entries {
		if _, err := srv.Ingest(context.Background(), &logengine.IngestRequest{Entry: e}); err != nil {
			t.Fatalf("Ingest %s: %v", e.Id, err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: reopen and verify records on disk
	m2, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager reopen: %v", err)
	}
	t.Cleanup(func() { m2.Close() })

	var found []*logengine.LogEntry
	for _, path := range m2.SegmentPaths() {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", filepath.Base(path), err)
		}
		for {
			data, err := storage.ReadRecord(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("ReadRecord in %s: %v", filepath.Base(path), err)
			}
			var pb logengine.LogEntry
			if err := proto.Unmarshal(data, &pb); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			found = append(found, &pb)
		}
		f.Close()
	}

	if len(found) != len(entries) {
		t.Fatalf("expected %d records after restart, got %d", len(entries), len(found))
	}

	for i, want := range entries {
		got := found[i]
		if got.Id != want.Id {
			t.Errorf("record[%d] Id: got %q, want %q", i, got.Id, want.Id)
		}
		if got.Service != want.Service {
			t.Errorf("record[%d] Service: got %q, want %q", i, got.Service, want.Service)
		}
		if got.Message != want.Message {
			t.Errorf("record[%d] Message: got %q, want %q", i, got.Message, want.Message)
		}
		if got.ReceivedAt == 0 {
			t.Errorf("record[%d] ReceivedAt should be non-zero", i)
		}
	}
}

func TestIngestBatch_AllEntriesOnDisk(t *testing.T) {
	dir := t.TempDir()

	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	srv := ingest.NewServer(m)

	batch := &logengine.IngestBatchRequest{
		Entries: []*logengine.LogEntry{
			{Id: "b1", Service: "svc", Message: "batch one"},
			{Id: "b2", Service: "svc", Message: "batch two"},
			{Id: "b3", Service: "svc", Message: "batch three"},
		},
	}

	resp, err := srv.IngestBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	if resp.Accepted != 3 {
		t.Errorf("expected Accepted=3, got %d", resp.Accepted)
	}
	if resp.Rejected != 0 {
		t.Errorf("expected Rejected=0, got %d", resp.Rejected)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify all 3 are on disk
	m2, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager reopen: %v", err)
	}
	t.Cleanup(func() { m2.Close() })

	count := 0
	for _, path := range m2.SegmentPaths() {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", filepath.Base(path), err)
		}
		for {
			_, err := storage.ReadRecord(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("ReadRecord: %v", err)
			}
			count++
		}
		f.Close()
	}

	if count != 3 {
		t.Errorf("expected 3 records on disk, got %d", count)
	}
}
```

- [ ] **Step 2: Run integration tests**

```bash
cd /mnt/d/projects/distributed-log-query-engine && go test ./test/integration/... -v
```

Expected: all tests PASS.

**Suggested commit message:**
- `test/integration/ingest_test.go` — `test: add integration tests for ingest and persistence across restart`

---

## Task 7: Final verification and BACKLOG update

- [ ] **Step 1: Run make build**

```bash
cd /mnt/d/projects/distributed-log-query-engine && make build
```

Expected: no output, exit 0.

- [ ] **Step 2: Run make test**

```bash
cd /mnt/d/projects/distributed-log-query-engine && make test
```

Expected: all packages pass. Output includes:
```
ok  github.com/Weilei424/distributed-log-query-engine/internal/storage
ok  github.com/Weilei424/distributed-log-query-engine/internal/ingest
ok  github.com/Weilei424/distributed-log-query-engine/test/integration
```

- [ ] **Step 3: Run make lint**

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
cd /mnt/d/projects/distributed-log-query-engine && make lint
```

Expected: no lint errors, exit 0.

If lint reports errors in `internal/api/gen/`, verify `.golangci.yml` has:
```yaml
issues:
  exclude-dirs:
    - internal/api/gen
```

- [ ] **Step 4: Update BACKLOG.md**

Mark all Phase 2 checklist items `[x]` in `docs/planning/BACKLOG.md` and update Phase 2 status to `Complete`.

---

## Success Criteria

- [ ] `make build` passes
- [ ] `make test` passes — all unit and integration tests green
- [ ] `make lint` passes
- [ ] Node restart does not lose written log entries (verified by `TestIngestAndPersistAcrossRestart`)
- [ ] Segment rotation creates a new `*.seg` file when threshold is crossed (verified by `TestManager_Rotation`)
- [ ] `BACKLOG.md` Phase 2 items updated to complete
