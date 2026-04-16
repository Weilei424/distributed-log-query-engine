# Phase 3 — Single Node Indexing and Query Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an in-memory inverted index and local query executor to the single node so that keyword, service, and time-range queries return correct results from on-disk segment files.

**Architecture:** An `internal/index.Index` maps tokens and service names to the segment paths that contain them, and tracks per-segment time bounds for pruning. An `internal/query.LocalExecutor` resolves candidate segments via the index, reads them from disk, filters entries in memory, and paginates the result. The ingest server updates the index on every successful append; the node rebuilds the index from all segment files on startup before accepting traffic.

**Tech Stack:** Go, gRPC, Protocol Buffers, `google.golang.org/protobuf/proto` (already a dependency)

**Spec:** `docs/superpowers/specs/2026-04-16-phase3-index-query-design.md`

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `pkg/types/query.go` | `QueryRequest` and `QueryResult` types |
| Create | `internal/index/index.go` | `Index` struct: `Add`, `Resolve`, `RebuildFromSegments` |
| Create | `internal/index/index_test.go` | Unit tests for the index |
| Create | `internal/storage/read.go` | `ReadSegment`, `Manager.ReadSegments`, `Manager.ActiveSegmentPath` |
| Create | `internal/storage/read_test.go` | Unit tests for storage read path |
| Create | `internal/query/executor.go` | `LocalExecutor.Execute` |
| Create | `internal/query/executor_test.go` | Unit tests for executor |
| Create | `internal/query/server.go` | gRPC `QueryServer` |
| Modify | `internal/ingest/server.go` | Inject `*index.Index`; call `idx.Add` after append |
| Modify | `internal/ingest/server_test.go` | Update `NewServer` call sites |
| Modify | `test/integration/ingest_test.go` | Update `NewServer` call sites |
| Modify | `cmd/node/main.go` | Wire index, rebuild on start, register query server |
| Create | `test/integration/query_test.go` | End-to-end ingest + query + restart test |
| Modify | `docs/planning/BACKLOG.md` | Mark Phase 3 items complete |

---

## Task 1: Add QueryRequest and QueryResult types

**Files:**
- Create: `pkg/types/query.go`

- [ ] **Step 1: Create the file**

```go
package types

// QueryRequest describes the parameters for a local log query.
// StartTime and EndTime are Unix nanoseconds; zero means unbounded.
// Limit of zero uses the server default (100).
type QueryRequest struct {
	Keyword   string
	Service   string
	StartTime int64
	EndTime   int64
	Limit     int32
	Offset    int32
}

// QueryResult holds the output of a local log query.
type QueryResult struct {
	Entries []*LogEntry
	Total   int32 // total matching entries before limit/offset
	TookMs  int64
}
```

- [ ] **Step 2: Verify it builds**

```bash
cd /path/to/repo && go build ./pkg/types/...
```

Expected: exits 0, no output.

---

## Task 2: Implement the in-memory index

**Files:**
- Create: `internal/index/index.go`

- [ ] **Step 1: Write the index implementation**

```go
package index

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// nonAlphanumeric matches sequences of characters that are not lowercase letters or digits.
var nonAlphanumeric = regexp.MustCompile(`[^a-z0-9]+`)

// SegmentMeta records the observed timestamp bounds for a segment file.
type SegmentMeta struct {
	MinTime int64
	MaxTime int64
}

// Index is a thread-safe in-memory inverted index.
// It maps message tokens and service names to segment paths, and tracks
// per-segment time bounds for time-range pruning.
type Index struct {
	mu              sync.RWMutex
	tokenSegments   map[string]map[string]struct{} // token → set of segment paths
	serviceSegments map[string]map[string]struct{} // service → set of segment paths
	segmentMeta     map[string]SegmentMeta         // segment path → time bounds
}

// NewIndex returns an initialized empty Index.
func NewIndex() *Index {
	return &Index{
		tokenSegments:   make(map[string]map[string]struct{}),
		serviceSegments: make(map[string]map[string]struct{}),
		segmentMeta:     make(map[string]SegmentMeta),
	}
}

// tokenize lowercases s and splits on non-alphanumeric character sequences.
// Empty tokens are omitted.
func tokenize(s string) []string {
	lower := strings.ToLower(s)
	parts := nonAlphanumeric.Split(lower, -1)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Add registers entry in the index under segmentPath.
// Safe for concurrent use.
func (idx *Index) Add(entry *types.LogEntry, segmentPath string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, tok := range tokenize(entry.Message) {
		if idx.tokenSegments[tok] == nil {
			idx.tokenSegments[tok] = make(map[string]struct{})
		}
		idx.tokenSegments[tok][segmentPath] = struct{}{}
	}

	if entry.Service != "" {
		if idx.serviceSegments[entry.Service] == nil {
			idx.serviceSegments[entry.Service] = make(map[string]struct{})
		}
		idx.serviceSegments[entry.Service][segmentPath] = struct{}{}
	}

	meta, ok := idx.segmentMeta[segmentPath]
	if !ok {
		meta = SegmentMeta{MinTime: entry.Timestamp, MaxTime: entry.Timestamp}
	} else {
		if entry.Timestamp < meta.MinTime {
			meta.MinTime = entry.Timestamp
		}
		if entry.Timestamp > meta.MaxTime {
			meta.MaxTime = entry.Timestamp
		}
	}
	idx.segmentMeta[segmentPath] = meta
}

// Resolve returns the sorted set of segment paths that may contain entries
// matching the given keyword, service, and time range.
// Empty keyword or service, and zero time bounds, are ignored.
func (idx *Index) Resolve(keyword, service string, startTime, endTime int64) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Start with all known segments.
	candidates := make(map[string]struct{})
	for path := range idx.segmentMeta {
		candidates[path] = struct{}{}
	}

	// Intersect by keyword tokens (only when keyword is non-empty).
	if keyword != "" {
		for _, tok := range tokenize(keyword) {
			segs, ok := idx.tokenSegments[tok]
			if !ok {
				return nil
			}
			for path := range candidates {
				if _, found := segs[path]; !found {
					delete(candidates, path)
				}
			}
			if len(candidates) == 0 {
				return nil
			}
		}
	}

	// Intersect by service.
	if service != "" {
		segs := idx.serviceSegments[service]
		for path := range candidates {
			if _, found := segs[path]; !found {
				delete(candidates, path)
			}
		}
	}

	// Prune by time range.
	for path := range candidates {
		meta := idx.segmentMeta[path]
		if startTime > 0 && meta.MaxTime < startTime {
			delete(candidates, path)
			continue
		}
		if endTime > 0 && meta.MinTime > endTime {
			delete(candidates, path)
		}
	}

	paths := make([]string, 0, len(candidates))
	for path := range candidates {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// RebuildFromSegments repopulates the index from a list of segment files.
// readFn is called for each path to load its entries.
// Returns a wrapped error if any segment cannot be read.
func (idx *Index) RebuildFromSegments(paths []string, readFn func(string) ([]*types.LogEntry, error)) error {
	for _, path := range paths {
		entries, err := readFn(path)
		if err != nil {
			return fmt.Errorf("rebuild index from %s: %w", path, err)
		}
		for _, entry := range entries {
			idx.Add(entry, path)
		}
	}
	return nil
}
```

- [ ] **Step 2: Verify it builds**

```bash
go build ./internal/index/...
```

Expected: exits 0, no output.

---

## Task 3: Unit tests for internal/index

**Files:**
- Create: `internal/index/index_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package index_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

func makeEntry(id, service, message string, ts int64) *types.LogEntry {
	return &types.LogEntry{ID: id, Service: service, Message: message, Timestamp: ts}
}

func TestResolve_KeywordHit(t *testing.T) {
	idx := index.NewIndex()
	idx.Add(makeEntry("e1", "svc", "user login failed", 100), "/seg/a")

	paths := idx.Resolve("login", "", 0, 0)
	if len(paths) != 1 || paths[0] != "/seg/a" {
		t.Errorf("expected [\"/seg/a\"], got %v", paths)
	}
}

func TestResolve_KeywordMiss(t *testing.T) {
	idx := index.NewIndex()
	idx.Add(makeEntry("e1", "svc", "user login failed", 100), "/seg/a")

	paths := idx.Resolve("timeout", "", 0, 0)
	if len(paths) != 0 {
		t.Errorf("expected empty, got %v", paths)
	}
}

func TestResolve_CaseInsensitive(t *testing.T) {
	idx := index.NewIndex()
	idx.Add(makeEntry("e1", "svc", "User Login Failed", 100), "/seg/a")

	paths := idx.Resolve("login", "", 0, 0)
	if len(paths) != 1 || paths[0] != "/seg/a" {
		t.Errorf("expected [\"/seg/a\"] for lowercase keyword on uppercase message, got %v", paths)
	}
}

func TestResolve_TimeRangePrune(t *testing.T) {
	idx := index.NewIndex()
	idx.Add(makeEntry("e1", "svc", "alpha", 100), "/seg/a")
	idx.Add(makeEntry("e2", "svc", "alpha", 500), "/seg/b")

	// Only /seg/b has entries within [300, 600].
	paths := idx.Resolve("alpha", "", 300, 600)
	if len(paths) != 1 || paths[0] != "/seg/b" {
		t.Errorf("expected [\"/seg/b\"], got %v", paths)
	}
}

func TestResolve_ServiceFilter(t *testing.T) {
	idx := index.NewIndex()
	idx.Add(makeEntry("e1", "svc-a", "hello world", 100), "/seg/a")
	idx.Add(makeEntry("e2", "svc-b", "hello world", 200), "/seg/b")

	paths := idx.Resolve("hello", "svc-a", 0, 0)
	if len(paths) != 1 || paths[0] != "/seg/a" {
		t.Errorf("expected [\"/seg/a\"], got %v", paths)
	}
}

func TestResolve_NoFilters_ReturnsAllSegments(t *testing.T) {
	idx := index.NewIndex()
	idx.Add(makeEntry("e1", "svc", "foo", 100), "/seg/a")
	idx.Add(makeEntry("e2", "svc", "bar", 200), "/seg/b")

	paths := idx.Resolve("", "", 0, 0)
	if len(paths) != 2 {
		t.Errorf("expected 2 segments, got %v", paths)
	}
}

func TestRebuildFromSegments_ProducesCorrectIndex(t *testing.T) {
	data := map[string][]*types.LogEntry{
		"/seg/a": {makeEntry("e1", "svc", "foo bar", 100)},
		"/seg/b": {makeEntry("e2", "svc", "baz qux", 200)},
	}
	readFn := func(path string) ([]*types.LogEntry, error) {
		e, ok := data[path]
		if !ok {
			return nil, fmt.Errorf("unknown path: %s", path)
		}
		return e, nil
	}

	idx := index.NewIndex()
	if err := idx.RebuildFromSegments([]string{"/seg/a", "/seg/b"}, readFn); err != nil {
		t.Fatalf("RebuildFromSegments: %v", err)
	}

	if paths := idx.Resolve("foo", "", 0, 0); len(paths) != 1 || paths[0] != "/seg/a" {
		t.Errorf("expected /seg/a for 'foo', got %v", paths)
	}
	if paths := idx.Resolve("baz", "", 0, 0); len(paths) != 1 || paths[0] != "/seg/b" {
		t.Errorf("expected /seg/b for 'baz', got %v", paths)
	}
}

func TestRebuildFromSegments_ReadFnError(t *testing.T) {
	readFn := func(path string) ([]*types.LogEntry, error) {
		return nil, fmt.Errorf("read error")
	}
	idx := index.NewIndex()
	err := idx.RebuildFromSegments([]string{"/seg/a"}, readFn)
	if err == nil {
		t.Error("expected error from failing readFn, got nil")
	}
}

func TestAdd_Concurrent(t *testing.T) {
	idx := index.NewIndex()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := makeEntry(fmt.Sprintf("e%d", i), "svc", fmt.Sprintf("token%d message", i), int64(i))
			idx.Add(e, fmt.Sprintf("/seg/%d", i%3))
			idx.Resolve(fmt.Sprintf("token%d", i), "", 0, 0)
		}(i)
	}
	wg.Wait()
}
```

- [ ] **Step 2: Run the tests to make sure they pass**

```bash
go test -race ./internal/index/...
```

Expected: all tests PASS, no race conditions detected.

---

## Task 4: Add storage read path

**Files:**
- Create: `internal/storage/read.go`

- [ ] **Step 1: Write the read path**

```go
package storage

import (
	"fmt"
	"io"
	"os"

	"google.golang.org/protobuf/proto"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// ReadSegment reads all log entries from the segment file at path.
// Returns an empty slice without error for a zero-byte file.
func ReadSegment(path string) ([]*types.LogEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	defer f.Close()

	var entries []*types.LogEntry
	for {
		data, err := ReadRecord(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read record from %s: %w", path, err)
		}
		var pb logengine.LogEntry
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, fmt.Errorf("unmarshal record from %s: %w", path, err)
		}
		entries = append(entries, storageProtoToEntry(&pb))
	}
	return entries, nil
}

// storageProtoToEntry converts a proto LogEntry to *types.LogEntry.
func storageProtoToEntry(pb *logengine.LogEntry) *types.LogEntry {
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

// ReadSegments reads all entries from the given segment paths in order.
// Returns a wrapped error if any segment cannot be read.
func (m *Manager) ReadSegments(paths []string) ([]*types.LogEntry, error) {
	var all []*types.LogEntry
	for _, path := range paths {
		entries, err := ReadSegment(path)
		if err != nil {
			return nil, fmt.Errorf("read segments: %w", err)
		}
		all = append(all, entries...)
	}
	return all, nil
}

// ActiveSegmentPath returns the absolute path of the currently active segment.
// Returns an empty string if the manager is closed.
func (m *Manager) ActiveSegmentPath() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil || len(m.paths) == 0 {
		return ""
	}
	return m.paths[len(m.paths)-1]
}
```

- [ ] **Step 2: Verify it builds**

```bash
go build ./internal/storage/...
```

Expected: exits 0, no output.

---

## Task 5: Unit tests for storage read path

**Files:**
- Create: `internal/storage/read_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package storage_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

func TestReadSegment_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	now := time.Now().UnixNano()
	want := []*types.LogEntry{
		{ID: "a", Service: "svc", Level: "INFO", Message: "hello world", Timestamp: now},
		{ID: "b", Service: "svc", Level: "WARN", Message: "goodbye world", Timestamp: now + 1},
	}
	for _, e := range want {
		if err := m.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	paths := m.SegmentPaths()
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := storage.ReadSegment(paths[0])
	if err != nil {
		t.Fatalf("ReadSegment: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	if got[0].ID != "a" || got[1].ID != "b" {
		t.Errorf("unexpected IDs: %q %q", got[0].ID, got[1].ID)
	}
	if got[0].Message != "hello world" || got[1].Message != "goodbye world" {
		t.Errorf("unexpected messages: %q %q", got[0].Message, got[1].Message)
	}
}

func TestReadSegment_Empty(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "*.seg")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	name := f.Name()
	f.Close()

	entries, err := storage.ReadSegment(name)
	if err != nil {
		t.Fatalf("ReadSegment on empty file: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
}

func TestReadSegment_NotFound(t *testing.T) {
	_, err := storage.ReadSegment(filepath.Join(t.TempDir(), "missing.seg"))
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

func TestManager_ActiveSegmentPath(t *testing.T) {
	dir := t.TempDir()
	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	path := m.ActiveSegmentPath()
	if path == "" {
		t.Fatal("expected non-empty active segment path")
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("ActiveSegmentPath %q does not exist: %v", path, err)
	}
}

func TestManager_ReadSegments_MultipleSegments(t *testing.T) {
	dir := t.TempDir()
	// Small segment size forces rotation after the first entry.
	m, err := storage.NewManager(dir, 1)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "first"},
		{ID: "2", Service: "svc", Message: "second"},
	}
	for _, e := range entries {
		if err := m.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	paths := m.SegmentPaths()
	if len(paths) < 2 {
		t.Fatalf("expected at least 2 segments after forced rotation, got %d", len(paths))
	}

	got, err := m.ReadSegments(paths)
	if err != nil {
		t.Fatalf("ReadSegments: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries across segments, got %d", len(got))
	}
	if got[0].ID != "1" || got[1].ID != "2" {
		t.Errorf("unexpected IDs: %q %q", got[0].ID, got[1].ID)
	}
}
```

- [ ] **Step 2: Run the tests**

```bash
go test -race ./internal/storage/...
```

Expected: all tests PASS.

---

## Task 6: Implement query executor

**Files:**
- Create: `internal/query/executor.go`

- [ ] **Step 1: Write the executor**

```go
package query

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

const defaultLimit = 100

// LocalExecutor runs log queries against the local index and segment files.
type LocalExecutor struct {
	index   *index.Index
	manager *storage.Manager
}

// NewLocalExecutor returns a LocalExecutor backed by idx and manager.
func NewLocalExecutor(idx *index.Index, manager *storage.Manager) *LocalExecutor {
	return &LocalExecutor{index: idx, manager: manager}
}

// Execute runs req against the local index and returns matching log entries.
func (e *LocalExecutor) Execute(ctx context.Context, req *types.QueryRequest) (*types.QueryResult, error) {
	start := time.Now()

	if req.Limit == 0 {
		req.Limit = defaultLimit
	}
	if req.Offset < 0 {
		return nil, fmt.Errorf("offset must be non-negative")
	}

	paths := e.index.Resolve(req.Keyword, req.Service, req.StartTime, req.EndTime)

	var raw []*types.LogEntry
	if len(paths) > 0 {
		var err error
		raw, err = e.manager.ReadSegments(paths)
		if err != nil {
			return nil, fmt.Errorf("execute query: %w", err)
		}
	}

	kwLower := strings.ToLower(req.Keyword)
	filtered := make([]*types.LogEntry, 0, len(raw))
	for _, entry := range raw {
		if req.Keyword != "" && !strings.Contains(strings.ToLower(entry.Message), kwLower) {
			continue
		}
		if req.Service != "" && entry.Service != req.Service {
			continue
		}
		if req.StartTime > 0 && entry.Timestamp < req.StartTime {
			continue
		}
		if req.EndTime > 0 && entry.Timestamp > req.EndTime {
			continue
		}
		filtered = append(filtered, entry)
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Timestamp > filtered[j].Timestamp
	})

	total := int32(len(filtered))

	// Apply offset.
	offset := int(req.Offset)
	if offset > len(filtered) {
		offset = len(filtered)
	}
	filtered = filtered[offset:]

	// Apply limit.
	limit := int(req.Limit)
	if limit > len(filtered) {
		limit = len(filtered)
	}
	filtered = filtered[:limit]

	return &types.QueryResult{
		Entries: filtered,
		Total:   total,
		TookMs:  time.Since(start).Milliseconds(),
	}, nil
}
```

- [ ] **Step 2: Verify it builds**

```bash
go build ./internal/query/...
```

Expected: exits 0, no output.

---

## Task 7: Unit tests for LocalExecutor

**Files:**
- Create: `internal/query/executor_test.go`

- [ ] **Step 1: Write the failing tests**

```go
package query_test

import (
	"context"
	"testing"

	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// newExecutor creates a Manager with entries written and an Index populated from
// those entries, then returns a LocalExecutor over them.
func newExecutor(t *testing.T, entries []*types.LogEntry) *query.LocalExecutor {
	t.Helper()
	m, err := storage.NewManager(t.TempDir(), 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	idx := index.NewIndex()
	for _, e := range entries {
		if err := m.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
		idx.Add(e, m.ActiveSegmentPath())
	}
	return query.NewLocalExecutor(idx, m)
}

func TestExecute_KeywordFilter(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "user login failed", Timestamp: 100},
		{ID: "2", Service: "svc", Message: "disk write error", Timestamp: 200},
	}
	ex := newExecutor(t, entries)

	result, err := ex.Execute(context.Background(), &types.QueryRequest{Keyword: "login"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 1 {
		t.Errorf("expected Total=1, got %d", result.Total)
	}
	if len(result.Entries) != 1 || result.Entries[0].ID != "1" {
		t.Errorf("expected entry 1, got %+v", result.Entries)
	}
}

func TestExecute_KeywordCaseInsensitive(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "User Login Success", Timestamp: 100},
	}
	ex := newExecutor(t, entries)

	result, err := ex.Execute(context.Background(), &types.QueryRequest{Keyword: "login"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 1 {
		t.Errorf("expected Total=1 for case-insensitive keyword, got %d", result.Total)
	}
}

func TestExecute_TimeRangeFilter(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "alpha", Timestamp: 100},
		{ID: "2", Service: "svc", Message: "alpha", Timestamp: 500},
		{ID: "3", Service: "svc", Message: "alpha", Timestamp: 900},
	}
	ex := newExecutor(t, entries)

	result, err := ex.Execute(context.Background(), &types.QueryRequest{StartTime: 200, EndTime: 700})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 1 {
		t.Errorf("expected Total=1, got %d", result.Total)
	}
	if len(result.Entries) != 1 || result.Entries[0].ID != "2" {
		t.Errorf("expected only entry 2, got %+v", result.Entries)
	}
}

func TestExecute_Pagination(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "alpha", Timestamp: 100},
		{ID: "2", Service: "svc", Message: "alpha", Timestamp: 200},
		{ID: "3", Service: "svc", Message: "alpha", Timestamp: 300},
	}
	ex := newExecutor(t, entries)

	// Sorted descending: 300(ID=3), 200(ID=2), 100(ID=1). Offset=1 skips 300.
	result, err := ex.Execute(context.Background(), &types.QueryRequest{Limit: 2, Offset: 1})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 3 {
		t.Errorf("expected Total=3 (before pagination), got %d", result.Total)
	}
	if len(result.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result.Entries))
	}
	if result.Entries[0].ID != "2" || result.Entries[1].ID != "1" {
		t.Errorf("unexpected order: %q %q", result.Entries[0].ID, result.Entries[1].ID)
	}
}

func TestExecute_SortedDescending(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "event", Timestamp: 300},
		{ID: "2", Service: "svc", Message: "event", Timestamp: 100},
		{ID: "3", Service: "svc", Message: "event", Timestamp: 200},
	}
	ex := newExecutor(t, entries)

	result, err := ex.Execute(context.Background(), &types.QueryRequest{Keyword: "event"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result.Entries))
	}
	if result.Entries[0].ID != "1" || result.Entries[1].ID != "3" || result.Entries[2].ID != "2" {
		t.Errorf("wrong order: %q %q %q",
			result.Entries[0].ID, result.Entries[1].ID, result.Entries[2].ID)
	}
}

func TestExecute_NoFilters_ReturnsAll(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "alpha", Timestamp: 100},
		{ID: "2", Service: "svc", Message: "beta", Timestamp: 200},
	}
	ex := newExecutor(t, entries)

	result, err := ex.Execute(context.Background(), &types.QueryRequest{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 2 {
		t.Errorf("expected Total=2, got %d", result.Total)
	}
}

func TestExecute_OffsetBeyondTotal_ReturnsEmpty(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "alpha", Timestamp: 100},
	}
	ex := newExecutor(t, entries)

	result, err := ex.Execute(context.Background(), &types.QueryRequest{Offset: 10})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 1 {
		t.Errorf("expected Total=1, got %d", result.Total)
	}
	if len(result.Entries) != 0 {
		t.Errorf("expected 0 entries after offset exceeds total, got %d", len(result.Entries))
	}
}
```

- [ ] **Step 2: Run the tests**

```bash
go test -race ./internal/query/...
```

Expected: all tests PASS.

---

## Task 8: Implement the gRPC query server

**Files:**
- Create: `internal/query/server.go`

- [ ] **Step 1: Write the server**

```go
package query

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// QueryServer implements the gRPC QueryServiceServer interface.
type QueryServer struct {
	logengine.UnimplementedQueryServiceServer
	executor *LocalExecutor
}

// NewQueryServer returns a QueryServer backed by the given executor.
func NewQueryServer(executor *LocalExecutor) *QueryServer {
	return &QueryServer{executor: executor}
}

// Query handles a gRPC Query request.
func (s *QueryServer) Query(ctx context.Context, req *logengine.QueryRequest) (*logengine.QueryResponse, error) {
	start := time.Now()

	typesReq := &types.QueryRequest{
		Keyword:   req.Keyword,
		Service:   req.Service,
		StartTime: req.StartTime,
		EndTime:   req.EndTime,
		Limit:     req.Limit,
		Offset:    req.Offset,
	}

	result, err := s.executor.Execute(ctx, typesReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query failed: %v", err)
	}

	pbEntries := make([]*logengine.LogEntry, len(result.Entries))
	for i, e := range result.Entries {
		pbEntries[i] = &logengine.LogEntry{
			Id:         e.ID,
			Timestamp:  e.Timestamp,
			ReceivedAt: e.ReceivedAt,
			Service:    e.Service,
			Level:      e.Level,
			Message:    e.Message,
			Fields:     e.Fields,
		}
	}

	return &logengine.QueryResponse{
		Entries: pbEntries,
		Total:   result.Total,
		Partial: false,
		TookMs:  time.Since(start).Milliseconds(),
	}, nil
}
```

- [ ] **Step 2: Verify it builds**

```bash
go build ./internal/query/...
```

Expected: exits 0, no output.

---

## Task 9: Update ingest server to call index.Add after each append

**Files:**
- Modify: `internal/ingest/server.go`
- Modify: `internal/ingest/server_test.go`
- Modify: `test/integration/ingest_test.go`

- [ ] **Step 1: Update internal/ingest/server.go**

Replace the full file with:

```go
package ingest

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// Server implements the gRPC IngestServiceServer interface.
type Server struct {
	logengine.UnimplementedIngestServiceServer
	manager *storage.Manager
	idx     *index.Index
}

// NewServer creates a new ingest Server backed by the given storage manager and index.
func NewServer(manager *storage.Manager, idx *index.Index) *Server {
	return &Server{manager: manager, idx: idx}
}

// Ingest writes a single log entry to the storage layer and updates the index.
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

	entry := protoToEntry(req.Entry)
	entry.ReceivedAt = time.Now().UnixNano()

	// TODO: propagate ctx to manager.Append when storage layer supports cancellation.
	if err := s.manager.Append(entry); err != nil {
		return nil, status.Errorf(codes.Internal, "append failed: %v", err)
	}

	s.idx.Add(entry, s.manager.ActiveSegmentPath())

	return &logengine.IngestResponse{Id: req.Entry.Id, Ok: true}, nil
}

// IngestBatch writes multiple log entries to the storage layer.
// Does not short-circuit on individual entry failure.
func (s *Server) IngestBatch(ctx context.Context, req *logengine.IngestBatchRequest) (*logengine.IngestBatchResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	var accepted, rejected int32
	for _, pb := range req.Entries {
		_, err := s.Ingest(ctx, &logengine.IngestRequest{Entry: pb})
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.Internal {
				return nil, status.Errorf(codes.Internal, "storage failure during batch ingest: %v", err)
			}
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

- [ ] **Step 2: Update the newTestServer helper in internal/ingest/server_test.go**

At line 18–26 in `internal/ingest/server_test.go`, replace:

```go
func newTestServer(t *testing.T) *ingest.Server {
	t.Helper()
	m, err := storage.NewManager(t.TempDir(), 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return ingest.NewServer(m)
}
```

with:

```go
func newTestServer(t *testing.T) *ingest.Server {
	t.Helper()
	m, err := storage.NewManager(t.TempDir(), 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return ingest.NewServer(m, index.NewIndex())
}
```

Also add `"github.com/Weilei424/distributed-log-query-engine/internal/index"` to the import block, and update the direct `NewServer` call in `TestIngest_ReceivedAtIsSet` at line 95:

```go
// Before:
srv := ingest.NewServer(m)

// After:
srv := ingest.NewServer(m, index.NewIndex())
```

- [ ] **Step 3: Update test/integration/ingest_test.go**

In `TestIngestAndPersistAcrossRestart` (line 25) and `TestIngestBatch_AllEntriesOnDisk` (line 103), replace:

```go
srv := ingest.NewServer(m)
```

with:

```go
srv := ingest.NewServer(m, index.NewIndex())
```

Add `"github.com/Weilei424/distributed-log-query-engine/internal/index"` to the import block of that file.

- [ ] **Step 4: Run all ingest tests to confirm no regression**

```bash
go test -race ./internal/ingest/... ./test/integration/...
```

Expected: all tests PASS.

---

## Task 10: Wire index and query server in cmd/node/main.go

**Files:**
- Modify: `cmd/node/main.go`

- [ ] **Step 1: Replace cmd/node/main.go with the wired version**

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
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
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

	idx := index.NewIndex()
	if err := idx.RebuildFromSegments(manager.SegmentPaths(), storage.ReadSegment); err != nil {
		log.Fatalf("index rebuild: %v", err)
	}

	ingestSrv := ingest.NewServer(manager, idx)
	querySrv := query.NewQueryServer(query.NewLocalExecutor(idx, manager))

	grpcSrv := grpc.NewServer()
	logengine.RegisterIngestServiceServer(grpcSrv, ingestSrv)
	logengine.RegisterQueryServiceServer(grpcSrv, querySrv)

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

- [ ] **Step 2: Verify it builds**

```bash
go build ./cmd/node/...
```

Expected: exits 0, no output.

---

## Task 11: Integration test for single-node query

**Files:**
- Create: `test/integration/query_test.go`

- [ ] **Step 1: Write the integration tests**

```go
package integration_test

import (
	"context"
	"testing"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

func TestQuerySingleNode_Filters(t *testing.T) {
	dir := t.TempDir()
	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	idx := index.NewIndex()
	srv := ingest.NewServer(m, idx)
	ex := query.NewLocalExecutor(idx, m)
	ctx := context.Background()

	entries := []*logengine.LogEntry{
		{Id: "1", Service: "auth", Level: "INFO", Message: "user login success", Timestamp: 100},
		{Id: "2", Service: "db", Level: "ERROR", Message: "connection timeout", Timestamp: 200},
		{Id: "3", Service: "auth", Level: "WARN", Message: "user login failed", Timestamp: 300},
	}
	for _, e := range entries {
		if _, err := srv.Ingest(ctx, &logengine.IngestRequest{Entry: e}); err != nil {
			t.Fatalf("Ingest %s: %v", e.Id, err)
		}
	}

	t.Run("keyword filter", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{Keyword: "login"})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 2 {
			t.Errorf("expected Total=2, got %d", result.Total)
		}
	})

	t.Run("service filter", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{Service: "db"})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 1 || result.Entries[0].ID != "2" {
			t.Errorf("expected entry 2 for service=db, got %+v", result.Entries)
		}
	})

	t.Run("time range filter", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{StartTime: 150, EndTime: 350})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 2 {
			t.Errorf("expected Total=2 for time range [150,350], got %d", result.Total)
		}
	})

	t.Run("combined filters", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{
			Keyword:   "login",
			Service:   "auth",
			StartTime: 200,
			EndTime:   400,
		})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 1 || result.Entries[0].ID != "3" {
			t.Errorf("expected entry 3 for combined filters, got %+v", result.Entries)
		}
	})
}

func TestQuerySingleNode_IndexRebuildAfterRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Phase 1: ingest entries and close the node.
	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	idx := index.NewIndex()
	srv := ingest.NewServer(m, idx)

	entries := []*logengine.LogEntry{
		{Id: "1", Service: "auth", Level: "INFO", Message: "token expired", Timestamp: 100},
		{Id: "2", Service: "cache", Level: "WARN", Message: "cache miss rate high", Timestamp: 200},
	}
	for _, e := range entries {
		if _, err := srv.Ingest(ctx, &logengine.IngestRequest{Entry: e}); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: reopen and rebuild index from disk.
	m2, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager reopen: %v", err)
	}
	t.Cleanup(func() { m2.Close() })

	idx2 := index.NewIndex()
	if err := idx2.RebuildFromSegments(m2.SegmentPaths(), storage.ReadSegment); err != nil {
		t.Fatalf("RebuildFromSegments: %v", err)
	}
	ex := query.NewLocalExecutor(idx2, m2)

	t.Run("keyword query after restart", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{Keyword: "token"})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 1 || result.Entries[0].ID != "1" {
			t.Errorf("expected entry 1 for 'token', got %+v", result.Entries)
		}
	})

	t.Run("service query after restart", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{Service: "cache"})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 1 || result.Entries[0].ID != "2" {
			t.Errorf("expected entry 2 for service=cache, got %+v", result.Entries)
		}
	})

	t.Run("all entries present after restart", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 2 {
			t.Errorf("expected Total=2 after restart, got %d", result.Total)
		}
	})
}
```

- [ ] **Step 2: Run the integration tests**

```bash
go test -race ./test/integration/...
```

Expected: all tests PASS.

---

## Task 12: Full validation and backlog update

**Files:**
- Modify: `docs/planning/BACKLOG.md`

- [ ] **Step 1: Run the full test suite**

```bash
make test
```

Expected: exits 0, all tests green.

- [ ] **Step 2: Run the linter**

```bash
make lint
```

Expected: exits 0, no lint errors.

- [ ] **Step 3: Run the build**

```bash
make build
```

Expected: exits 0, no output.

- [ ] **Step 4: Update BACKLOG.md — mark Phase 3 items complete**

In `docs/planning/BACKLOG.md`, update the Phase 3 section header and all checklist items:

```markdown
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
```
