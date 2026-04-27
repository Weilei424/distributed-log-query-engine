# Phase 6: Distributed Query Fan-Out and Result Aggregation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `FanOutQueryServer` to the coordinator binary that fans out `QueryService` requests to all healthy storage nodes in parallel, merges and deduplicates results, and returns a single coherent response with a `partial` flag when any node misses the deadline.

**Architecture:** A `FanOutExecutor` in `internal/coordinator/` reads the Raft FSM's live `ClusterState`, spawns one goroutine per healthy node, collects results via a buffered channel, and calls a pure `MergeResults` function to sort, deduplicate, and paginate. `FanOutQueryServer` is a thin gRPC adapter over `FanOutExecutor`. No proto changes are required — the existing `QueryRequest`/`QueryResponse` with `partial bool` is sufficient.

**Tech Stack:** Go, gRPC (`QueryService` proto), `sync.WaitGroup`, `metadata.FSM.State()` (already returns `metadata.ClusterState`)

---

## File Map

**Created:**
- `internal/coordinator/merge.go` — `nodeResult`, `mergeOutput`, `MergeResults` pure function
- `internal/coordinator/merge_test.go` — unit tests for MergeResults
- `internal/coordinator/node_client.go` — lazy `QueryServiceClient` pool keyed by address
- `internal/coordinator/fanout.go` — `ClusterStateProvider` interface, `FanOutExecutor`
- `internal/coordinator/fanout_test.go` — unit tests for FanOutExecutor (in-process gRPC nodes)
- `internal/coordinator/query_server.go` — `FanOutQueryServer` (thin gRPC adapter)
- `test/integration/phase6_query_test.go` — integration tests

**Modified:**
- `pkg/types/query.go` — add `Partial bool` to `QueryResult`
- `internal/query/server.go` — forward `result.Partial` in gRPC response
- `test/integration/phase5_node_test.go` — add `QueryService` registration + `queryClient()` helper to `testNode`
- `cmd/coordinator/main.go` — wire `FanOutExecutor` and `FanOutQueryServer`
- `docs/planning/BACKLOG.md` — update Phase 6 checklist

---

## Task 1: Add `Partial` to `types.QueryResult`

**Files:**
- Modify: `pkg/types/query.go`
- Modify: `internal/query/server.go`

`types.QueryResult` currently has no `Partial` field. The fan-out executor needs to propagate this through to callers. The local query server should also set it (always `false` for single-node results) to keep the API surface consistent.

- [ ] **Step 1: Add `Partial` field to `QueryResult`**

Replace the struct in `pkg/types/query.go`:

```go
// QueryResult holds the output of a log query.
type QueryResult struct {
	Entries []*LogEntry
	Total   int32 // lower-bound candidate count before limit/offset (see Architecture Notes Decision 7)
	TookMs  int64
	Partial bool // true if one or more source nodes did not respond within the deadline
}
```

- [ ] **Step 2: Forward `result.Partial` in the local query server**

In `internal/query/server.go`, update the `QueryResponse` construction:

```go
return &logengine.QueryResponse{
    Entries: pbEntries,
    Total:   result.Total,
    Partial: result.Partial,
    TookMs:  time.Since(start).Milliseconds(),
}, nil
```

- [ ] **Step 3: Verify existing tests still pass**

```bash
make test
```

Expected: all existing tests pass; `result.Partial` is `false` for all local-executor results.

---

## Task 2: Write merge tests (test-first)

**Files:**
- Create: `internal/coordinator/merge_test.go`

Write the tests before implementing `MergeResults`. The test file references types (`nodeResult`, `mergeOutput`) and the function (`MergeResults`) that don't exist yet, so the build will fail until Task 3.

- [ ] **Step 1: Create `internal/coordinator/merge_test.go`**

```go
package coordinator

import (
	"errors"
	"testing"

	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

func mkEntry(id string, ts int64) *types.LogEntry {
	return &types.LogEntry{ID: id, Timestamp: ts, Service: "svc", Message: "msg"}
}

func TestMergeResults_Sort(t *testing.T) {
	parts := []nodeResult{
		{nodeID: "n1", entries: []*types.LogEntry{mkEntry("a", 300), mkEntry("b", 100)}},
		{nodeID: "n2", entries: []*types.LogEntry{mkEntry("c", 200)}},
	}
	out := MergeResults(parts, 0, 0)
	if out.partial {
		t.Error("expected partial=false, no errors in parts")
	}
	if len(out.entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(out.entries))
	}
	// Sorted timestamp desc: a(300), c(200), b(100)
	wantOrder := []string{"a", "c", "b"}
	for i, e := range out.entries {
		if e.ID != wantOrder[i] {
			t.Errorf("position %d: want %q, got %q", i, wantOrder[i], e.ID)
		}
	}
	if out.total != 3 {
		t.Errorf("expected total=3, got %d", out.total)
	}
}

func TestMergeResults_TieBreaker(t *testing.T) {
	// Same timestamp — sort by ID ascending
	parts := []nodeResult{
		{nodeID: "n1", entries: []*types.LogEntry{mkEntry("z", 100), mkEntry("a", 100)}},
	}
	out := MergeResults(parts, 0, 0)
	if out.entries[0].ID != "a" || out.entries[1].ID != "z" {
		t.Errorf("tie-breaker: want [a z], got [%s %s]", out.entries[0].ID, out.entries[1].ID)
	}
}

func TestMergeResults_Dedup(t *testing.T) {
	e := mkEntry("dup-id", 100)
	parts := []nodeResult{
		{nodeID: "n1", entries: []*types.LogEntry{e}},
		{nodeID: "n2", entries: []*types.LogEntry{e}},
	}
	out := MergeResults(parts, 0, 0)
	if len(out.entries) != 1 {
		t.Fatalf("expected 1 entry after dedup, got %d", len(out.entries))
	}
	if out.total != 1 {
		t.Errorf("expected total=1, got %d", out.total)
	}
}

func TestMergeResults_Pagination(t *testing.T) {
	parts := []nodeResult{
		{nodeID: "n1", entries: []*types.LogEntry{
			mkEntry("e1", 500),
			mkEntry("e2", 400),
			mkEntry("e3", 300),
			mkEntry("e4", 200),
			mkEntry("e5", 100),
		}},
	}
	// offset=2, limit=2 → global sorted [e1,e2,e3,e4,e5] → skip 2 → [e3,e4,e5] → take 2 → [e3,e4]
	out := MergeResults(parts, 2, 2)
	if out.total != 5 {
		t.Errorf("expected total=5 (before pagination), got %d", out.total)
	}
	if len(out.entries) != 2 {
		t.Fatalf("expected 2 entries (limit=2), got %d", len(out.entries))
	}
	if out.entries[0].ID != "e3" || out.entries[1].ID != "e4" {
		t.Errorf("wrong pagination result: got [%s %s], want [e3 e4]",
			out.entries[0].ID, out.entries[1].ID)
	}
}

func TestMergeResults_Partial(t *testing.T) {
	parts := []nodeResult{
		{nodeID: "n1", entries: []*types.LogEntry{mkEntry("x", 100)}},
		{nodeID: "n2", err: errors.New("context deadline exceeded")},
	}
	out := MergeResults(parts, 0, 0)
	if !out.partial {
		t.Error("expected partial=true when a node has an error")
	}
	if len(out.entries) != 1 {
		t.Fatalf("expected 1 entry from healthy node, got %d", len(out.entries))
	}
	if out.entries[0].ID != "x" {
		t.Errorf("expected entry x, got %s", out.entries[0].ID)
	}
}

func TestMergeResults_AllFailed(t *testing.T) {
	parts := []nodeResult{
		{nodeID: "n1", err: errors.New("timeout")},
		{nodeID: "n2", err: errors.New("timeout")},
	}
	out := MergeResults(parts, 0, 0)
	if !out.partial {
		t.Error("expected partial=true when all nodes fail")
	}
	if len(out.entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(out.entries))
	}
	if out.total != 0 {
		t.Errorf("expected total=0, got %d", out.total)
	}
}
```

- [ ] **Step 2: Verify the build fails (types don't exist yet)**

```bash
go build ./internal/coordinator/...
```

Expected: compile error — `nodeResult`, `mergeOutput`, `MergeResults` undefined.

---

## Task 3: Implement `MergeResults`

**Files:**
- Create: `internal/coordinator/merge.go`

- [ ] **Step 1: Create `internal/coordinator/merge.go`**

```go
package coordinator

import (
	"sort"

	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// nodeResult holds the query result from one storage node.
type nodeResult struct {
	nodeID  string
	entries []*types.LogEntry
	total   int32
	err     error
}

// mergeOutput is the result of combining results from multiple nodes.
type mergeOutput struct {
	entries []*types.LogEntry
	total   int32
	partial bool
}

// MergeResults combines partial results from multiple nodes into a single
// sorted, deduplicated, paginated result. It is a pure function with no
// external dependencies.
//
// Deduplication is by entry ID (same entry may appear on primary and replica).
// Sort order is timestamp descending, entry ID ascending as tie-breaker.
// total is the deduplicated candidate count before pagination (lower bound — see Decision 7).
// partial is true when any nodeResult has a non-nil err.
// When limit is 0, all entries after the offset are returned.
func MergeResults(parts []nodeResult, offset, limit int32) mergeOutput {
	var partial bool
	seen := make(map[string]struct{})
	var combined []*types.LogEntry

	for _, p := range parts {
		if p.err != nil {
			partial = true
			continue
		}
		for _, e := range p.entries {
			if _, ok := seen[e.ID]; ok {
				continue
			}
			seen[e.ID] = struct{}{}
			combined = append(combined, e)
		}
	}

	sort.Slice(combined, func(i, j int) bool {
		if combined[i].Timestamp != combined[j].Timestamp {
			return combined[i].Timestamp > combined[j].Timestamp
		}
		return combined[i].ID < combined[j].ID
	})

	total := int32(len(combined))

	off := int(offset)
	if off > len(combined) {
		off = len(combined)
	}
	combined = combined[off:]

	if limit > 0 && int(limit) < len(combined) {
		combined = combined[:int(limit)]
	}

	return mergeOutput{
		entries: combined,
		total:   total,
		partial: partial,
	}
}
```

- [ ] **Step 2: Run the merge tests**

```bash
go test ./internal/coordinator/... -run TestMergeResults -v
```

Expected: all 6 `TestMergeResults_*` tests pass.

- [ ] **Step 3: Run full test suite to check for regressions**

```bash
make test
```

Expected: all tests pass.

---

## Task 4: Implement `nodeClientPool`

**Files:**
- Create: `internal/coordinator/node_client.go`

The pool is tested implicitly through `FanOutExecutor` in Task 6. No standalone test is written here.

- [ ] **Step 1: Create `internal/coordinator/node_client.go`**

```go
package coordinator

import (
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
)

// nodeClientPool caches gRPC QueryServiceClient connections keyed by address.
// Connections are created lazily and never evicted; cluster membership is stable
// within a query lifetime.
type nodeClientPool struct {
	mu      sync.Mutex
	clients map[string]logengine.QueryServiceClient
}

func newNodeClientPool() *nodeClientPool {
	return &nodeClientPool{clients: make(map[string]logengine.QueryServiceClient)}
}

func (p *nodeClientPool) get(addr string) (logengine.QueryServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	c := logengine.NewQueryServiceClient(conn)
	p.clients[addr] = c
	return c, nil
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/coordinator/...
```

Expected: builds cleanly.

---

## Task 5: Write `FanOutExecutor` tests (test-first)

**Files:**
- Create: `internal/coordinator/fanout_test.go`

The test starts two in-process gRPC servers backed by real `LocalExecutor` instances, constructs a `staticStateProvider` with both nodes marked healthy, and verifies the executor returns merged, sorted results from both.

- [ ] **Step 1: Create `internal/coordinator/fanout_test.go`**

```go
package coordinator

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

// staticStateProvider implements ClusterStateProvider with a fixed ClusterState.
type staticStateProvider struct {
	state metadata.ClusterState
}

func (s *staticStateProvider) State() metadata.ClusterState { return s.state }

// startInProcessQueryNode starts a gRPC QueryService server backed by a real
// LocalExecutor. Returns the server address, the storage manager, and the index.
func startInProcessQueryNode(t *testing.T) (addr string, mgr *storage.Manager, idx *index.Index) {
	t.Helper()
	dir := t.TempDir()
	var err error
	mgr, err = storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	idx = index.NewIndex()

	executor := query.NewLocalExecutor(idx, mgr)
	querySrv := query.NewQueryServer(executor)

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr = lis.Addr().String()

	grpcSrv := grpc.NewServer()
	logengine.RegisterQueryServiceServer(grpcSrv, querySrv)
	t.Cleanup(func() {
		grpcSrv.GracefulStop()
		mgr.Close() //nolint:errcheck
	})
	go grpcSrv.Serve(lis) //nolint:errcheck
	return addr, mgr, idx
}

// ingestLocalEntry writes one entry directly to a node's storage + index
// without going through a gRPC call. Timestamp must be set explicitly.
func ingestLocalEntry(t *testing.T, mgr *storage.Manager, idx *index.Index, e *logengine.LogEntry) {
	t.Helper()
	srv := ingest.NewLocalServer(mgr, idx)
	if _, err := srv.Ingest(context.Background(), &logengine.IngestRequest{Entry: e}); err != nil {
		t.Fatalf("Ingest %s: %v", e.Id, err)
	}
}

func TestFanOutExecutor_MergesFromTwoNodes(t *testing.T) {
	addr1, mgr1, idx1 := startInProcessQueryNode(t)
	addr2, mgr2, idx2 := startInProcessQueryNode(t)

	ingestLocalEntry(t, mgr1, idx1, &logengine.LogEntry{
		Id: "n1-e1", Service: "svc", Level: "INFO",
		Message: "hello from node1", Timestamp: 200,
	})
	ingestLocalEntry(t, mgr2, idx2, &logengine.LogEntry{
		Id: "n2-e1", Service: "svc", Level: "INFO",
		Message: "hello from node2", Timestamp: 100,
	})

	state := metadata.ClusterState{
		Nodes: map[string]metadata.NodeRecord{
			"node-1": {ID: "node-1", Address: addr1, Status: metadata.NodeHealthy},
			"node-2": {ID: "node-2", Address: addr2, Status: metadata.NodeHealthy},
		},
		Shards: map[int]metadata.ShardRecord{},
	}

	exec := NewFanOutExecutor(&staticStateProvider{state}, 5000, 1000)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := exec.Execute(ctx, &logengine.QueryRequest{Limit: 10})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Partial {
		t.Error("expected Partial=false; both nodes should respond")
	}
	if len(result.Entries) != 2 {
		t.Fatalf("expected 2 entries (one per node), got %d", len(result.Entries))
	}
	// Sorted timestamp desc: n1-e1(200) then n2-e1(100)
	if result.Entries[0].ID != "n1-e1" {
		t.Errorf("expected first entry n1-e1, got %s", result.Entries[0].ID)
	}
	if result.Entries[1].ID != "n2-e1" {
		t.Errorf("expected second entry n2-e1, got %s", result.Entries[1].ID)
	}
	if result.Total != 2 {
		t.Errorf("expected Total=2, got %d", result.Total)
	}
}

func TestFanOutExecutor_SkipsUnhealthyNodes(t *testing.T) {
	addr1, mgr1, idx1 := startInProcessQueryNode(t)

	ingestLocalEntry(t, mgr1, idx1, &logengine.LogEntry{
		Id: "e1", Service: "svc", Level: "INFO", Message: "hello", Timestamp: 100,
	})

	state := metadata.ClusterState{
		Nodes: map[string]metadata.NodeRecord{
			"node-1": {ID: "node-1", Address: addr1, Status: metadata.NodeHealthy},
			// node-2 is unhealthy — should be skipped
			"node-2": {ID: "node-2", Address: "127.0.0.1:9999", Status: metadata.NodeUnhealthy},
		},
		Shards: map[int]metadata.ShardRecord{},
	}

	exec := NewFanOutExecutor(&staticStateProvider{state}, 5000, 1000)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := exec.Execute(ctx, &logengine.QueryRequest{Limit: 10})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Partial {
		t.Error("expected Partial=false; unhealthy nodes are skipped, not failures")
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry from healthy node, got %d", len(result.Entries))
	}
}

func TestFanOutExecutor_PartialOnNodeFailure(t *testing.T) {
	addr1, mgr1, idx1 := startInProcessQueryNode(t)

	ingestLocalEntry(t, mgr1, idx1, &logengine.LogEntry{
		Id: "e1", Service: "svc", Level: "INFO", Message: "hello", Timestamp: 100,
	})

	state := metadata.ClusterState{
		Nodes: map[string]metadata.NodeRecord{
			"node-1": {ID: "node-1", Address: addr1, Status: metadata.NodeHealthy},
			// node-2 has a bad address — dial or query will fail
			"node-2": {ID: "node-2", Address: "127.0.0.1:1", Status: metadata.NodeHealthy},
		},
		Shards: map[int]metadata.ShardRecord{},
	}

	exec := NewFanOutExecutor(&staticStateProvider{state}, 500, 1000)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := exec.Execute(ctx, &logengine.QueryRequest{Limit: 10})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.Partial {
		t.Error("expected Partial=true when one node is unreachable")
	}
	// node-1's entry should still be returned
	found := false
	for _, e := range result.Entries {
		if e.ID == "e1" {
			found = true
		}
	}
	if !found {
		t.Error("expected entry from healthy node to be present in partial result")
	}
}

- [ ] **Step 2: Verify the build fails (FanOutExecutor not defined yet)**

```bash
go build ./internal/coordinator/...
```

Expected: compile error — `NewFanOutExecutor`, `ClusterStateProvider` undefined.

---

## Task 6: Implement `FanOutExecutor`

**Files:**
- Create: `internal/coordinator/fanout.go`

- [ ] **Step 1: Create `internal/coordinator/fanout.go`**

```go
package coordinator

import (
	"context"
	"log"
	"sync"
	"time"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// ClusterStateProvider is satisfied by *metadata.FSM.
type ClusterStateProvider interface {
	State() metadata.ClusterState
}

// FanOutExecutor fans out QueryService requests to all healthy storage nodes
// in parallel and merges the results.
type FanOutExecutor struct {
	state         ClusterStateProvider
	pool          *nodeClientPool
	nodeTimeoutMs int64
	fanOutLimit   int32
}

// NewFanOutExecutor creates a FanOutExecutor.
// nodeTimeoutMs is the per-node query deadline in milliseconds.
// fanOutLimit is the limit sent to each node (overrides the client limit so
// the global merge has enough candidates to apply offset+limit correctly).
func NewFanOutExecutor(state ClusterStateProvider, nodeTimeoutMs int64, fanOutLimit int32) *FanOutExecutor {
	return &FanOutExecutor{
		state:         state,
		pool:          newNodeClientPool(),
		nodeTimeoutMs: nodeTimeoutMs,
		fanOutLimit:   fanOutLimit,
	}
}

// Execute fans out req to all healthy nodes and returns merged results.
// The result's Partial field is true if any node failed to respond.
func (e *FanOutExecutor) Execute(ctx context.Context, req *logengine.QueryRequest) (*types.QueryResult, error) {
	start := time.Now()

	cs := e.state.State()

	type target struct{ id, addr string }
	var targets []target
	for id, n := range cs.Nodes {
		if n.Status == metadata.NodeHealthy && n.Address != "" {
			targets = append(targets, target{id, n.Address})
		}
	}

	ids := make([]string, len(targets))
	for i, t := range targets {
		ids[i] = t.id + "=" + t.addr
	}
	log.Printf("fanout: targeting %d nodes: %v", len(targets), ids)

	// Nodes receive the full fan-out limit so the coordinator has enough
	// candidate entries to correctly apply the client's offset and limit.
	fanReq := &logengine.QueryRequest{
		Keyword:   req.Keyword,
		Service:   req.Service,
		StartTime: req.StartTime,
		EndTime:   req.EndTime,
		Limit:     e.fanOutLimit,
		Offset:    0,
	}

	ch := make(chan nodeResult, len(targets))
	var wg sync.WaitGroup
	timeout := time.Duration(e.nodeTimeoutMs) * time.Millisecond

	for _, t := range targets {
		wg.Add(1)
		t := t
		go func() {
			defer wg.Done()
			nodeCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			client, err := e.pool.get(t.addr)
			if err != nil {
				log.Printf("fanout: node %s error: %v", t.id, err)
				ch <- nodeResult{nodeID: t.id, err: err}
				return
			}

			resp, err := client.Query(nodeCtx, fanReq)
			if err != nil {
				if nodeCtx.Err() != nil {
					log.Printf("fanout: node %s timed out", t.id)
				} else {
					log.Printf("fanout: node %s error: %v", t.id, err)
				}
				ch <- nodeResult{nodeID: t.id, err: err}
				return
			}

			entries := make([]*types.LogEntry, len(resp.Entries))
			for i, pb := range resp.Entries {
				entries[i] = &types.LogEntry{
					ID:         pb.Id,
					Timestamp:  pb.Timestamp,
					ReceivedAt: pb.ReceivedAt,
					Service:    pb.Service,
					Level:      pb.Level,
					Message:    pb.Message,
					Fields:     pb.Fields,
				}
			}
			log.Printf("fanout: node %s responded: %d entries", t.id, len(entries))
			ch <- nodeResult{nodeID: t.id, entries: entries, total: resp.Total}
		}()
	}

	wg.Wait()
	close(ch)

	var parts []nodeResult
	for r := range ch {
		parts = append(parts, r)
	}

	// Apply default limit when client sends 0.
	clientLimit := req.Limit
	if clientLimit == 0 {
		clientLimit = 100
	}

	mergeStart := time.Now()
	out := MergeResults(parts, req.Offset, clientLimit)
	log.Printf("fanout: merge took %dms, total=%d, partial=%v",
		time.Since(mergeStart).Milliseconds(), out.total, out.partial)

	return &types.QueryResult{
		Entries: out.entries,
		Total:   out.total,
		TookMs:  time.Since(start).Milliseconds(),
		Partial: out.partial,
	}, nil
}
```

- [ ] **Step 2: Run fanout executor tests**

```bash
go test ./internal/coordinator/... -run TestFanOutExecutor -v -timeout 30s
```

Expected: all 3 `TestFanOutExecutor_*` tests pass.

- [ ] **Step 3: Run all coordinator tests**

```bash
go test ./internal/coordinator/... -v -timeout 30s
```

Expected: all merge and fanout tests pass.

- [ ] **Step 4: Run full test suite**

```bash
make test
```

Expected: all tests pass.

---

## Task 7: Implement `FanOutQueryServer`

**Files:**
- Create: `internal/coordinator/query_server.go`

- [ ] **Step 1: Create `internal/coordinator/query_server.go`**

```go
package coordinator

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
)

// FanOutQueryServer implements QueryServiceServer using distributed fan-out.
type FanOutQueryServer struct {
	logengine.UnimplementedQueryServiceServer
	executor *FanOutExecutor
}

// NewFanOutQueryServer returns a FanOutQueryServer backed by executor.
func NewFanOutQueryServer(executor *FanOutExecutor) *FanOutQueryServer {
	return &FanOutQueryServer{executor: executor}
}

// Query handles a gRPC Query request by fanning out to all healthy nodes.
func (s *FanOutQueryServer) Query(ctx context.Context, req *logengine.QueryRequest) (*logengine.QueryResponse, error) {
	start := time.Now()

	if req.Limit < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "limit must be non-negative")
	}
	if req.Offset < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "offset must be non-negative")
	}

	result, err := s.executor.Execute(ctx, req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, status.Errorf(codes.Canceled, "query canceled")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Errorf(codes.DeadlineExceeded, "query deadline exceeded")
		}
		return nil, status.Errorf(codes.Internal, "fan-out query failed: %v", err)
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
		Partial: result.Partial,
		TookMs:  time.Since(start).Milliseconds(),
	}, nil
}
```

- [ ] **Step 2: Build the coordinator package**

```bash
go build ./internal/coordinator/...
```

Expected: builds cleanly.

---

## Task 8: Wire into coordinator binary

**Files:**
- Modify: `cmd/coordinator/main.go`

- [ ] **Step 1: Add coordinator import and read new env vars**

In `cmd/coordinator/main.go`, add to the import block:

```go
"github.com/Weilei424/distributed-log-query-engine/internal/coordinator"
```

Add two env var reads directly after the existing `heartbeatTimeout` line (around line 34):

```go
nodeQueryTimeoutMs := int64(envIntOrDefault("NODE_QUERY_TIMEOUT_MS", 5000))
fanOutLimit        := int32(envIntOrDefault("FAN_OUT_LIMIT", 1000))
```

- [ ] **Step 2: Register `QueryService` on the gRPC server**

In `main()`, immediately after:
```go
logengine.RegisterClusterServiceServer(grpcSrv, metadata.NewServer(r, fsm))
```

Add:
```go
fanOutExec := coordinator.NewFanOutExecutor(fsm, nodeQueryTimeoutMs, fanOutLimit)
logengine.RegisterQueryServiceServer(grpcSrv, coordinator.NewFanOutQueryServer(fanOutExec))
```

- [ ] **Step 3: Update the startup log line**

Replace the existing `fmt.Printf("coordinator started: ...")` line with one that includes the new settings:

```go
fmt.Printf("coordinator started: id=%s raft=%s grpc=%s http=%s shards=%d node_query_timeout_ms=%d fan_out_limit=%d\n",
    nodeID, bindAddr, grpcAddr, httpAddr, totalShards, nodeQueryTimeoutMs, fanOutLimit)
```

- [ ] **Step 4: Build the coordinator binary**

```bash
go build ./cmd/coordinator/...
```

Expected: builds cleanly.

- [ ] **Step 5: Run full test suite**

```bash
make test
```

Expected: all tests pass.

---

## Task 9: Add `QueryService` to the test node helper

**Files:**
- Modify: `test/integration/phase5_node_test.go`

The storage node binary (`cmd/node/main.go`) already registers `QueryService`. The test helper `startPhase5Node` is missing this registration, so the coordinator's fan-out has no `QueryService` endpoint to call during integration tests. Adding it here is a correctness fix, not a behavior change.

- [ ] **Step 1: Add `query` import to `phase5_node_test.go`**

The file already imports `ingest`, `storage`, `index`. Add:

```go
"github.com/Weilei424/distributed-log-query-engine/internal/query"
```

- [ ] **Step 2: Register `QueryService` in `startPhase5Node`**

In `startPhase5Node`, immediately after:
```go
logengine.RegisterIngestServiceServer(grpcSrv, srv)
```

Add:
```go
querySrv := query.NewQueryServer(query.NewLocalExecutor(idx, m))
logengine.RegisterQueryServiceServer(grpcSrv, querySrv)
```

- [ ] **Step 3: Add `queryClient` helper to `testNode`**

After the existing `ingestClient` method (around line 49), add:

```go
func (tn *testNode) queryClient(t *testing.T) logengine.QueryServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(tn.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial node %s: %v", tn.nodeID, err)
	}
	t.Cleanup(func() { conn.Close() })
	return logengine.NewQueryServiceClient(conn)
}
```

- [ ] **Step 4: Run existing phase 5 tests to confirm no regressions**

```bash
go test ./test/integration/... -run TestPhase5 -v -timeout 60s
```

Expected: all phase 5 tests pass.

---

## Task 10: Integration tests

**Files:**
- Create: `test/integration/phase6_query_test.go`

The integration tests need a coordinator that also serves `QueryService` (fan-out). A new `startPhase6Coordinator` helper wraps the existing `startTestCoordinator` pattern and adds `FanOutQueryServer` registration. It returns a `testPhase6Coordinator` that embeds `testCoordinator` and exposes a `queryClient` helper.

- [ ] **Step 1: Create `test/integration/phase6_query_test.go`**

```go
package integration_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/coordinator"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
)

// testPhase6Coordinator is a coordinator that also serves QueryService (fan-out).
type testPhase6Coordinator struct {
	testCoordinator
}

func (tc *testPhase6Coordinator) queryClient(t *testing.T) logengine.QueryServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(tc.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial coordinator: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return logengine.NewQueryServiceClient(conn)
}

func startPhase6Coordinator(t *testing.T, totalShards int) *testPhase6Coordinator {
	t.Helper()

	cfg := raft.DefaultConfig()
	cfg.LocalID = "test-coordinator"
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond

	raftAddr, transport := raft.NewInmemTransport("test-coordinator")
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapStore := raft.NewInmemSnapshotStore()

	fsm := metadata.NewFSM(totalShards)
	r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		t.Fatalf("NewRaft: %v", err)
	}
	bootCfg := raft.Configuration{
		Servers: []raft.Server{{ID: "test-coordinator", Address: raftAddr}},
	}
	if f := r.BootstrapCluster(bootCfg); f.Error() != nil {
		t.Fatalf("BootstrapCluster: %v", f.Error())
	}
	waitForLeader(t, r, 5*time.Second)

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	logengine.RegisterClusterServiceServer(grpcSrv, metadata.NewServer(r, fsm))

	// Wire fan-out query service: 5 s per-node timeout, 1000 fan-out limit.
	fanOutExec := coordinator.NewFanOutExecutor(fsm, 5000, 1000)
	logengine.RegisterQueryServiceServer(grpcSrv, coordinator.NewFanOutQueryServer(fanOutExec))

	go grpcSrv.Serve(lis) //nolint:errcheck

	return &testPhase6Coordinator{
		testCoordinator: testCoordinator{
			addr: lis.Addr().String(),
			fsm:  fsm,
			r:    r,
			srv:  grpcSrv,
		},
	}
}

// TestDistributedQuery_AllNodes ingests entries to two storage nodes via the
// orchestrator and verifies the coordinator's QueryService returns merged results
// from both nodes.
func TestDistributedQuery_AllNodes(t *testing.T) {
	const totalShards = 4
	coord := startPhase6Coordinator(t, totalShards)
	defer coord.cleanup()

	nodeA := startPhase5Node(t, "node-a", coord.addr, totalShards)
	defer nodeA.cleanup()
	nodeB := startPhase5Node(t, "node-b", coord.addr, totalShards)
	defer nodeB.cleanup()

	// Wait until both nodes appear healthy in cluster state.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state := coord.fsm.State()
		na, aOK := state.Nodes["node-a"]
		nb, bOK := state.Nodes["node-b"]
		if aOK && bOK && na.Status == metadata.NodeHealthy && nb.Status == metadata.NodeHealthy {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	state := coord.fsm.State()
	if state.Nodes["node-a"].Status != metadata.NodeHealthy {
		t.Fatal("node-a never became healthy")
	}
	if state.Nodes["node-b"].Status != metadata.NodeHealthy {
		t.Fatal("node-b never became healthy")
	}

	// Ingest one entry directly to each node's local storage (bypassing routing
	// to ensure each node holds exactly one entry regardless of shard assignment).
	nodeAIngest := nodeA.ingestClient(t)
	nodeBIngest := nodeB.ingestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := nodeAIngest.Ingest(ctx, &logengine.IngestRequest{
		Entry: &logengine.LogEntry{
			Id: "entry-from-a", Service: "svc-a", Level: "INFO",
			Message: "message on node a",
		},
	}); err != nil {
		t.Fatalf("Ingest to node-a: %v", err)
	}
	if _, err := nodeBIngest.Ingest(ctx, &logengine.IngestRequest{
		Entry: &logengine.LogEntry{
			Id: "entry-from-b", Service: "svc-b", Level: "INFO",
			Message: "message on node b",
		},
	}); err != nil {
		t.Fatalf("Ingest to node-b: %v", err)
	}

	// Wait for replication to settle before querying.
	time.Sleep(200 * time.Millisecond)

	coordQuery := coord.queryClient(t)
	resp, err := coordQuery.Query(ctx, &logengine.QueryRequest{Limit: 100})
	if err != nil {
		t.Fatalf("coordinator Query: %v", err)
	}

	if resp.Partial {
		t.Error("expected Partial=false; both nodes should respond")
	}

	// We expect at least 2 distinct entry IDs (one from each node).
	// Replication may have copied entries, but dedup by ID ensures each appears once.
	ids := make(map[string]bool)
	for _, e := range resp.Entries {
		ids[e.Id] = true
	}
	if !ids["entry-from-a"] {
		t.Error("coordinator response missing entry-from-a")
	}
	if !ids["entry-from-b"] {
		t.Error("coordinator response missing entry-from-b")
	}
}

// TestDistributedQuery_PartialFailure stops one storage node before querying and
// verifies the coordinator returns partial=true along with the surviving node's results.
func TestDistributedQuery_PartialFailure(t *testing.T) {
	const totalShards = 4
	coord := startPhase6Coordinator(t, totalShards)
	defer coord.cleanup()

	nodeA := startPhase5Node(t, "node-a", coord.addr, totalShards)
	nodeB := startPhase5Node(t, "node-b", coord.addr, totalShards)
	defer nodeB.cleanup()

	// Wait until both nodes are healthy.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state := coord.fsm.State()
		na, aOK := state.Nodes["node-a"]
		nb, bOK := state.Nodes["node-b"]
		if aOK && bOK && na.Status == metadata.NodeHealthy && nb.Status == metadata.NodeHealthy {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Ingest one entry to node-b (the node that will survive).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nodeBIngest := nodeB.ingestClient(t)
	if _, err := nodeBIngest.Ingest(ctx, &logengine.IngestRequest{
		Entry: &logengine.LogEntry{
			Id: "survivor-entry", Service: "svc-b", Level: "INFO",
			Message: "this node survived",
		},
	}); err != nil {
		t.Fatalf("Ingest to node-b: %v", err)
	}

	// Stop node-a so the coordinator's fan-out will fail for it.
	nodeA.cleanup()
	time.Sleep(100 * time.Millisecond)

	// Use a short per-node timeout so the test does not wait too long for the dead node.
	fastExec := coordinator.NewFanOutExecutor(coord.fsm, 500, 1000)
	fastSrv := coordinator.NewFanOutQueryServer(fastExec)

	// Call Execute directly (without gRPC overhead) to avoid needing a second listener.
	result, err := fastExec.Execute(ctx, &logengine.QueryRequest{Limit: 100})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	_ = fastSrv // used to confirm it compiles

	if !result.Partial {
		t.Error("expected Partial=true when node-a is unreachable")
	}

	found := false
	for _, e := range result.Entries {
		if e.ID == "survivor-entry" {
			found = true
		}
	}
	if !found {
		t.Error("expected survivor-entry from node-b in partial result")
	}
}
```

- [ ] **Step 2: Run the integration tests**

```bash
go test ./test/integration/... -run TestDistributedQuery -v -timeout 60s
```

Expected: both `TestDistributedQuery_AllNodes` and `TestDistributedQuery_PartialFailure` pass.

- [ ] **Step 3: Run the full test suite**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 4: Run linter**

```bash
make lint
```

Expected: no lint errors.

---

## Task 11: Update `BACKLOG.md`

**Files:**
- Modify: `docs/planning/BACKLOG.md`

- [ ] **Step 1: Replace the Phase 6 checklist with completed items**

Find the `## Phase 6` section and replace the `- [ ]` lines with the completed items:

```markdown
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
```

---

## Commit Message Reference

After all tasks are complete, one commit message per changed file:

```
pkg/types/query.go: add Partial bool to QueryResult
internal/query/server.go: forward result.Partial in gRPC QueryResponse
internal/coordinator/merge.go: add MergeResults, nodeResult, mergeOutput types
internal/coordinator/merge_test.go: unit tests for MergeResults (sort, dedup, pagination, partial)
internal/coordinator/node_client.go: add lazy QueryServiceClient pool
internal/coordinator/fanout.go: add FanOutExecutor and ClusterStateProvider interface
internal/coordinator/fanout_test.go: unit tests for FanOutExecutor with in-process gRPC nodes
internal/coordinator/query_server.go: add FanOutQueryServer thin gRPC adapter
cmd/coordinator/main.go: wire FanOutExecutor and FanOutQueryServer; NODE_QUERY_TIMEOUT_MS, FAN_OUT_LIMIT env vars
test/integration/phase5_node_test.go: register QueryService on test nodes; add queryClient helper
test/integration/phase6_query_test.go: integration tests for distributed query fan-out and partial failure
docs/planning/BACKLOG.md: mark Phase 6 complete
```
