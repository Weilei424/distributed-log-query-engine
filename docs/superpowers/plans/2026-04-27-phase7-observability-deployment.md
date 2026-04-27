# Phase 7: Observability, Deployment, and Reliability — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Prometheus metrics and structured zap logging to all services, provision a zero-click Grafana dashboard, ship real Dockerfiles and Docker Compose with Prometheus+Grafana, build a Helm chart, and add a load test program.

**Architecture:** Observability-first — build `internal/observability/` package, wire metrics and loggers into existing services via minimal constructor changes (functional options for Manager, updated signatures for others), then layer in deployment artifacts. All metric variables are package-level so they work without registration (tests need no changes to their Register calls).

**Tech Stack:** `go.uber.org/zap`, `github.com/prometheus/client_golang`, Docker multi-stage builds, Helm 3, plain Go load test binary.

---

## File Map

**New files:**
- `internal/observability/metrics.go` — all Prometheus metric vars + `Register()`
- `internal/observability/logger.go` — zap factory `NewLogger(component, nodeID)`
- `internal/observability/request_id.go` — context key + `NewRequestID`, `WithRequestID`, `RequestIDFromContext`
- `internal/observability/observability_test.go` — tests for all three
- `deployments/docker-compose/prometheus.yml` — Prometheus scrape config
- `deployments/docker-compose/grafana/provisioning/datasources/prometheus.yml`
- `deployments/docker-compose/grafana/provisioning/dashboards/provider.yml`
- `deployments/docker-compose/grafana/dashboards/logengine.json`
- `deployments/kubernetes/helm/Chart.yaml`
- `deployments/kubernetes/helm/values.yaml`
- `deployments/kubernetes/helm/templates/_helpers.tpl`
- `deployments/kubernetes/helm/templates/namespace.yaml`
- `deployments/kubernetes/helm/templates/coordinator-statefulset.yaml`
- `deployments/kubernetes/helm/templates/coordinator-service.yaml`
- `deployments/kubernetes/helm/templates/node-statefulset.yaml`
- `deployments/kubernetes/helm/templates/node-service.yaml`
- `deployments/kubernetes/helm/templates/prometheus-configmap.yaml`
- `deployments/kubernetes/helm/templates/prometheus-deployment.yaml`
- `deployments/kubernetes/helm/templates/prometheus-service.yaml`
- `test/load/main.go` — standalone load test binary
- `docs/runbooks/failure-demo.md`

**Modified files:**
- `internal/index/index.go` — add `TokenCount() int`
- `internal/index/index_test.go` — add token count test
- `internal/storage/manager.go` — functional options `WithNodeID`, wire metrics
- `internal/ingest/server.go` — add `logger` field + `SetLogger`, wire `IngestRequestsTotal`
- `internal/ingest/orchestrator.go` — wire `IndexTokenCount` after `idx.Add`
- `internal/query/server.go` — updated constructor, wire `QueryDuration`
- `internal/coordinator/fanout.go` — updated constructor, wire fan-out metrics
- `internal/cluster/heartbeat.go` — updated constructor, wire `NodeHealthStatus`
- `internal/replication/replicator.go` — updated constructor, wire `ReplicationLagEntries`
- `cmd/node/main.go` — register metrics, init logger, start `/metrics` HTTP server
- `cmd/coordinator/main.go` — register metrics, init logger, add `/metrics` to mux
- `deployments/docker-compose/Dockerfile.node` — real multi-stage build
- `deployments/docker-compose/Dockerfile.coordinator` — add `go mod download` layer
- `deployments/docker-compose/docker-compose.yml` — add prometheus + grafana services
- `Makefile` — update `run-local`, add `load-test`
- `docs/planning/BACKLOG.md` — mark Phase 7 complete
- `README.md` — document Phase 7

---

## Task 1: Add Dependencies

**Files:** `go.mod`, `go.sum`

- [ ] **Step 1: Add zap and prometheus/client_golang**

```bash
cd /mnt/d/projects/distributed-log-query-engine
go get go.uber.org/zap@latest
go get github.com/prometheus/client_golang@latest
go mod tidy
```

- [ ] **Step 2: Verify build still passes**

```bash
go build ./...
```
Expected: exits 0 with no errors.

---

## Task 2: Build `internal/observability/` Package

**Files:**
- Create: `internal/observability/metrics.go`
- Create: `internal/observability/logger.go`
- Create: `internal/observability/request_id.go`
- Create: `internal/observability/observability_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/observability/observability_test.go`:

```go
package observability_test

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Weilei424/distributed-log-query-engine/internal/observability"
)

func TestRegister_GathersMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	observability.Register(reg)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	if len(mfs) == 0 {
		t.Fatal("expected at least one metric family after Register")
	}
}

func TestRequestID_RoundTrip(t *testing.T) {
	id := observability.NewRequestID()
	ctx := observability.WithRequestID(context.Background(), id)
	if got := observability.RequestIDFromContext(ctx); got != id {
		t.Fatalf("got %q, want %q", got, id)
	}
}

func TestRequestIDFromContext_Missing(t *testing.T) {
	if got := observability.RequestIDFromContext(context.Background()); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestNewLogger_NotNil(t *testing.T) {
	if l := observability.NewLogger("test", "node-1"); l == nil {
		t.Fatal("expected non-nil logger")
	}
}
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
go test ./internal/observability/...
```
Expected: FAIL — package does not compile yet.

- [ ] **Step 3: Write `internal/observability/metrics.go`**

```go
package observability

import "github.com/prometheus/client_golang/prometheus"

var (
	IngestRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "logengine_ingest_requests_total",
		Help: "Total log entries processed by the ingest server.",
	}, []string{"node_id", "status"})

	AppendDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "logengine_append_duration_seconds",
		Help:    "Latency of segment append operations.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0},
	}, []string{"node_id"})

	ActiveSegmentBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logengine_active_segment_bytes",
		Help: "Current size of the active segment file in bytes.",
	}, []string{"node_id"})

	MountedSegmentsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logengine_mounted_segments_total",
		Help: "Number of segment files currently open.",
	}, []string{"node_id"})

	IndexTokenCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logengine_index_token_count",
		Help: "Number of unique tokens in the in-memory inverted index.",
	}, []string{"node_id"})

	QueryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "logengine_query_duration_seconds",
		Help:    "Latency of query execution.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0},
	}, []string{"node_id", "type"})

	FanOutTimeoutsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logengine_fanout_timeouts_total",
		Help: "Number of per-node fan-out timeouts.",
	})

	FanOutPartialTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "logengine_fanout_partial_total",
		Help: "Number of fan-out responses returned with partial=true.",
	})

	NodeHealthStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logengine_node_health_status",
		Help: "Node health: 1 = healthy, 0 = unhealthy.",
	}, []string{"node_id"})

	ReplicationLagEntries = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "logengine_replication_lag_entries",
		Help: "Number of entries pending replication per target address.",
	}, []string{"node_id"})
)

// Register wires all metrics into reg. Call once at process startup.
func Register(reg prometheus.Registerer) {
	reg.MustRegister(
		IngestRequestsTotal,
		AppendDuration,
		ActiveSegmentBytes,
		MountedSegmentsTotal,
		IndexTokenCount,
		QueryDuration,
		FanOutTimeoutsTotal,
		FanOutPartialTotal,
		NodeHealthStatus,
		ReplicationLagEntries,
	)
}
```

- [ ] **Step 4: Write `internal/observability/logger.go`**

```go
package observability

import "go.uber.org/zap"

// NewLogger returns a production zap logger pre-seeded with component and node_id fields.
func NewLogger(component, nodeID string) *zap.Logger {
	l := zap.Must(zap.NewProduction())
	return l.With(zap.String("component", component), zap.String("node_id", nodeID))
}
```

- [ ] **Step 5: Write `internal/observability/request_id.go`**

```go
package observability

import (
	"context"
	"fmt"
	"math/rand"
)

type contextKey int

const requestIDKey contextKey = 0

// NewRequestID returns a random hex request ID.
func NewRequestID() string {
	return fmt.Sprintf("req-%x", rand.Uint64())
}

// WithRequestID attaches id to ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestIDFromContext returns the request ID stored in ctx, or "" if absent.
func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey).(string)
	return v
}
```

- [ ] **Step 6: Run tests to confirm they pass**

```bash
go test ./internal/observability/...
```
Expected: PASS — 4 tests green.

- [ ] **Step 7: Verify build**

```bash
go build ./...
```
Expected: exits 0.

---

## Task 3: Add `Index.TokenCount()` and Wire Storage Manager Metrics

**Files:**
- Modify: `internal/index/index.go`
- Modify: `internal/index/index_test.go`
- Modify: `internal/storage/manager.go`

- [ ] **Step 1: Write failing test for `TokenCount`**

Add to `internal/index/index_test.go`:

```go
func TestIndex_TokenCount(t *testing.T) {
	idx := NewIndex()
	if idx.TokenCount() != 0 {
		t.Fatalf("expected 0 tokens initially, got %d", idx.TokenCount())
	}
	e := &types.LogEntry{Service: "svc", Message: "hello world", Timestamp: 1}
	idx.Add(e, "seg1")
	if idx.TokenCount() < 2 {
		t.Fatalf("expected at least 2 tokens after adding entry with 2 words, got %d", idx.TokenCount())
	}
}
```

- [ ] **Step 2: Run to confirm it fails**

```bash
go test ./internal/index/... -run TestIndex_TokenCount
```
Expected: FAIL — `TokenCount` undefined.

- [ ] **Step 3: Add `TokenCount()` to `internal/index/index.go`**

After the `RebuildFromSegments` function, add:

```go
// TokenCount returns the number of unique tokens in the index.
func (idx *Index) TokenCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.tokenSegments)
}
```

- [ ] **Step 4: Run index tests**

```bash
go test ./internal/index/...
```
Expected: PASS.

- [ ] **Step 5: Wire metrics into `internal/storage/manager.go`**

Add `WithNodeID` functional option and wire metrics. Replace the full file with this updated version (only the struct, `NewManager`, `appendLocked`, and `rotate` functions change — all other functions are unchanged):

**Add to imports:**
```go
"time"

"github.com/Weilei424/distributed-log-query-engine/internal/observability"
```

**Add after the `Manager` struct definition:**
```go
// ManagerOption configures a Manager.
type ManagerOption func(*Manager)

// WithNodeID sets the node ID used in Prometheus metric labels.
func WithNodeID(id string) ManagerOption {
	return func(m *Manager) { m.nodeID = id }
}
```

**Add `nodeID string` field to the `Manager` struct:**
```go
type Manager struct {
	mu              sync.Mutex
	dir             string
	maxSegmentBytes int64
	active          *Segment
	nextSeq         uint64
	paths           []string
	nodeID          string
}
```

**Update `NewManager` to accept opts and set initial gauge — replace just the function body's return statement section:**

The full updated `NewManager`:
```go
func NewManager(dir string, maxSegmentBytes int64, opts ...ManagerOption) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", dir, err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil {
		return nil, fmt.Errorf("glob segments: %w", err)
	}
	sort.Strings(matches)

	nextSeq, err := nextSeqFromMatches(matches)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		dir:             dir,
		maxSegmentBytes: maxSegmentBytes,
		paths:           matches,
		nextSeq:         nextSeq,
	}
	for _, opt := range opts {
		opt(m)
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

	observability.MountedSegmentsTotal.WithLabelValues(m.nodeID).Set(float64(len(m.paths)))
	return m, nil
}
```

**Update `appendLocked` — replace the final `return m.active.Append(data)` line:**
```go
	start := time.Now()
	if err := m.active.Append(data); err != nil {
		return err
	}
	observability.AppendDuration.WithLabelValues(m.nodeID).Observe(time.Since(start).Seconds())
	observability.ActiveSegmentBytes.WithLabelValues(m.nodeID).Set(float64(m.active.Size()))
	return nil
```

**Update `rotate` — add gauge update after `openNewSegment` succeeds:**
```go
func (m *Manager) rotate() error {
	if err := m.active.Close(); err != nil {
		return fmt.Errorf("close active segment before rotation: %w", err)
	}
	if err := m.openNewSegment(); err != nil {
		return err
	}
	observability.MountedSegmentsTotal.WithLabelValues(m.nodeID).Set(float64(len(m.paths)))
	return nil
}
```

- [ ] **Step 6: Run storage tests (no test file changes needed — `NewManager` opts are variadic)**

```bash
go test ./internal/storage/...
```
Expected: PASS — all existing tests still compile and pass because the opts parameter is variadic.

- [ ] **Step 7: Verify build**

```bash
go build ./...
```
Expected: exits 0.

---

## Task 4: Wire Metrics into Ingest (Server + Orchestrator)

**Files:**
- Modify: `internal/ingest/server.go`
- Modify: `internal/ingest/orchestrator.go`

- [ ] **Step 1: Add `logger` field and `SetLogger` to `internal/ingest/server.go`**

Add imports:
```go
"go.uber.org/zap"
"github.com/Weilei424/distributed-log-query-engine/internal/observability"
```

Add `logger *zap.Logger` to the `Server` struct:
```go
type Server struct {
	logengine.UnimplementedIngestServiceServer
	orchestrator *Orchestrator
	nodeID       string
	totalShards  int
	stateReader  cluster.ClusterStateReader
	manager      *storage.Manager
	idx          *index.Index
	logger       *zap.Logger
}
```

Initialize `logger` to `zap.NewNop()` in both constructors:

In `NewServer`:
```go
func NewServer(orchestrator *Orchestrator, nodeID string, totalShards int, manager *storage.Manager, idx *index.Index) *Server {
	return &Server{
		orchestrator: orchestrator,
		nodeID:       nodeID,
		totalShards:  totalShards,
		stateReader:  orchestrator.StateReader(),
		manager:      manager,
		idx:          idx,
		logger:       zap.NewNop(),
	}
}
```

In `NewLocalServer`:
```go
func NewLocalServer(manager *storage.Manager, idx *index.Index) *Server {
	orch := newLocalOrchestrator(manager, idx)
	return &Server{
		orchestrator: orch,
		nodeID:       "local",
		totalShards:  0,
		manager:      manager,
		idx:          idx,
		logger:       zap.NewNop(),
	}
}
```

Add setter after constructors:
```go
// SetLogger replaces the no-op logger with a real one. Call once after construction.
func (s *Server) SetLogger(l *zap.Logger) { s.logger = l }
```

- [ ] **Step 2: Wire `IngestRequestsTotal` into `Server.Ingest`**

Replace the current `Ingest` method:
```go
func (s *Server) Ingest(ctx context.Context, req *logengine.IngestRequest) (*logengine.IngestResponse, error) {
	reqID := observability.NewRequestID()
	ctx = observability.WithRequestID(ctx, reqID)

	resp, err := s.orchestrator.HandleIngest(ctx, req)

	status := "ok"
	if err != nil {
		status = "error"
	}
	observability.IngestRequestsTotal.WithLabelValues(s.nodeID, status).Inc()

	if err == nil {
		s.logger.Info("ingest",
			zap.String("request_id", reqID),
			zap.String("service", req.Entry.GetService()),
		)
	}
	return resp, err
}
```

- [ ] **Step 3: Wire `IndexTokenCount` into `Orchestrator.writeLocal` in `internal/ingest/orchestrator.go`**

Add imports:
```go
"github.com/Weilei424/distributed-log-query-engine/internal/observability"
```

After the line `o.idx.Add(entry, segPath)` in `writeLocal`, add:
```go
observability.IndexTokenCount.WithLabelValues(o.nodeID).Set(float64(o.idx.TokenCount()))
```

- [ ] **Step 4: Also wire `IndexTokenCount` in `Server.ReplicateEntry` after `s.idx.Add`**

In `server.go`'s `ReplicateEntry`, after `s.idx.Add(entry, segPath)`:
```go
observability.IndexTokenCount.WithLabelValues(s.nodeID).Set(float64(s.idx.TokenCount()))
```

- [ ] **Step 5: Run ingest tests**

```bash
go test ./internal/ingest/...
```
Expected: PASS.

- [ ] **Step 6: Verify build**

```bash
go build ./...
```
Expected: exits 0.

---

## Task 5: Wire Metrics into Query Server

**Files:**
- Modify: `internal/query/server.go`
- Modify: `internal/query/server_test.go`
- Modify: `internal/coordinator/fanout_test.go`
- Modify: `test/integration/phase5_node_test.go`

- [ ] **Step 1: Update `QueryServer` constructor**

In `internal/query/server.go`, add imports:
```go
"go.uber.org/zap"
"github.com/Weilei424/distributed-log-query-engine/internal/observability"
```

Update struct:
```go
type QueryServer struct {
	logengine.UnimplementedQueryServiceServer
	executor *LocalExecutor
	nodeID   string
	logger   *zap.Logger
}
```

Update constructor:
```go
func NewQueryServer(executor *LocalExecutor, nodeID string, logger *zap.Logger) *QueryServer {
	return &QueryServer{executor: executor, nodeID: nodeID, logger: logger}
}
```

- [ ] **Step 2: Wire `QueryDuration` into the `Query` handler**

In the `Query` method, the `start := time.Now()` line already exists. After the `result, err := s.executor.Execute(ctx, typesReq)` call, add:
```go
observability.QueryDuration.WithLabelValues(s.nodeID, "local").Observe(time.Since(start).Seconds())
```

Also add a log line after successful execution (before building the response):
```go
if err == nil {
    s.logger.Info("query",
        zap.String("request_id", observability.RequestIDFromContext(ctx)),
        zap.String("keyword", req.Keyword),
        zap.Int("results", len(result.Entries)),
    )
}
```

- [ ] **Step 3: Update the 4 callers that pass `NewQueryServer`**

**`internal/query/server_test.go`** — find both calls to `query.NewQueryServer` and add `"", zap.NewNop()`:

Add import `"go.uber.org/zap"` to the test file.

Change line 28 (helper function):
```go
return query.NewQueryServer(query.NewLocalExecutor(idx, m), "", zap.NewNop())
```

Change line 90:
```go
querySrv := query.NewQueryServer(query.NewLocalExecutor(idx, m), "", zap.NewNop())
```

**`internal/coordinator/fanout_test.go`** — add import `"go.uber.org/zap"`, change line 39:
```go
querySrv := query.NewQueryServer(executor, "", zap.NewNop())
```

**`test/integration/phase5_node_test.go`** — add import `"go.uber.org/zap"`, change line 106:
```go
querySrv := query.NewQueryServer(query.NewLocalExecutor(idx, m), nodeID, zap.NewNop())
```

(`cmd/node/main.go` is updated in Task 9.)

- [ ] **Step 4: Run query tests**

```bash
go test ./internal/query/...
```
Expected: PASS.

- [ ] **Step 5: Verify build**

```bash
go build ./...
```
Expected: exits 0.

---

## Task 6: Wire Metrics into Fan-Out Executor

**Files:**
- Modify: `internal/coordinator/fanout.go`
- Modify: `internal/coordinator/fanout_test.go`
- Modify: `test/integration/phase6_query_test.go`

- [ ] **Step 1: Update `FanOutExecutor` constructor and struct**

In `internal/coordinator/fanout.go`, replace `log` import with `zap` and add observability:
```go
import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
	"github.com/Weilei424/distributed-log-query-engine/internal/observability"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)
```

Add `logger *zap.Logger` to struct:
```go
type FanOutExecutor struct {
	state         ClusterStateProvider
	pool          *nodeClientPool
	nodeTimeoutMs int64
	fanOutLimit   int32
	logger        *zap.Logger
}
```

Update constructor:
```go
func NewFanOutExecutor(state ClusterStateProvider, nodeTimeoutMs int64, fanOutLimit int32, logger *zap.Logger) *FanOutExecutor {
	return &FanOutExecutor{
		state:         state,
		pool:          newNodeClientPool(),
		nodeTimeoutMs: nodeTimeoutMs,
		fanOutLimit:   fanOutLimit,
		logger:        logger,
	}
}
```

- [ ] **Step 2: Replace `log.Printf` calls with zap and add metrics in `Execute`**

In `Execute`, replace all `log.Printf(...)` with `e.logger.Info(...)` or `e.logger.Warn(...)`. Then add:

After `start := time.Now()` and before `return`, add observation:
```go
defer func() {
    observability.QueryDuration.WithLabelValues("coordinator", "fanout").Observe(time.Since(start).Seconds())
}()
```

In the goroutine, when `nodeCtx.Err() != nil` (timeout):
```go
observability.FanOutTimeoutsTotal.Inc()
e.logger.Warn("fanout node timeout", zap.String("node_id", t.id))
```

When a node errors but didn't time out:
```go
e.logger.Warn("fanout node error", zap.String("node_id", t.id), zap.Error(err))
```

After `out := MergeResults(...)`, if `out.partial`:
```go
if out.partial {
    observability.FanOutPartialTotal.Inc()
}
```

Replace the final log line:
```go
e.logger.Info("fanout complete",
    zap.Int64("merge_ms", time.Since(mergeStart).Milliseconds()),
    zap.Int32("total", out.total),
    zap.Bool("partial", out.partial),
)
```

- [ ] **Step 3: Update the 5 caller sites**

**`internal/coordinator/fanout_test.go`** — add import `"go.uber.org/zap"`, update 3 calls:
```go
exec := NewFanOutExecutor(&staticStateProvider{state}, 5000, 1000, zap.NewNop())
// (repeat for all 3 occurrences at lines 88, 131, 164)
exec := NewFanOutExecutor(&staticStateProvider{state}, 500, 1000, zap.NewNop())
```

**`test/integration/phase6_query_test.go`** — add import `"go.uber.org/zap"`, update line 70 and 201:
```go
fanOutExec := coordinator.NewFanOutExecutor(fsm, 5000, 1000, zap.NewNop())
fastExec := coordinator.NewFanOutExecutor(fsm, 500, 1000, zap.NewNop())
```

(`cmd/coordinator/main.go` is updated in Task 9.)

- [ ] **Step 4: Run coordinator tests**

```bash
go test ./internal/coordinator/...
```
Expected: PASS.

- [ ] **Step 5: Verify build**

```bash
go build ./...
```
Expected: exits 0.

---

## Task 7: Wire Metrics into HeartbeatSender and Replicator

**Files:**
- Modify: `internal/cluster/heartbeat.go`
- Modify: `internal/cluster/heartbeat_test.go`
- Modify: `internal/replication/replicator.go`
- Modify: `internal/replication/replicator_test.go`
- Modify: `test/integration/phase5_node_test.go`
- Modify: `test/integration/phase5_catchup_test.go`

- [ ] **Step 1: Update `HeartbeatSender`**

In `internal/cluster/heartbeat.go`, replace the file:
```go
package cluster

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/Weilei424/distributed-log-query-engine/internal/observability"
)

// Beater abstracts the heartbeat send operation for testability.
type Beater interface {
	SendHeartbeat(ctx context.Context) error
}

// HeartbeatSender sends periodic heartbeats to the coordinator.
type HeartbeatSender struct {
	beater   Beater
	interval time.Duration
	nodeID   string
	logger   *zap.Logger
}

// NewHeartbeatSender creates a HeartbeatSender with the given send interval.
func NewHeartbeatSender(b Beater, interval time.Duration, nodeID string, logger *zap.Logger) *HeartbeatSender {
	return &HeartbeatSender{beater: b, interval: interval, nodeID: nodeID, logger: logger}
}

// Run sends heartbeats at the configured interval until ctx is cancelled.
func (h *HeartbeatSender) Run(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := h.beater.SendHeartbeat(ctx); err != nil {
				h.logger.Warn("heartbeat failed", zap.Error(err))
				observability.NodeHealthStatus.WithLabelValues(h.nodeID).Set(0)
			} else {
				observability.NodeHealthStatus.WithLabelValues(h.nodeID).Set(1)
			}
		}
	}
}
```

- [ ] **Step 2: Update heartbeat test caller**

In `internal/cluster/heartbeat_test.go`, add import `"go.uber.org/zap"` and update line 24:
```go
sender := cluster.NewHeartbeatSender(stub, 20*time.Millisecond, "test-node", zap.NewNop())
```

- [ ] **Step 3: Update `Replicator`**

In `internal/replication/replicator.go`, update imports:
```go
import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/observability"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)
```

Add `nodeID string` and `logger *zap.Logger` to `Replicator` struct:
```go
type Replicator struct {
	totalShards int
	nodeID      string
	logger      *zap.Logger

	mu       sync.Mutex
	channels map[string]chan replicaJob
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}
```

Update constructor:
```go
func NewReplicator(totalShards int, nodeID string, logger *zap.Logger) *Replicator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Replicator{
		totalShards: totalShards,
		nodeID:      nodeID,
		logger:      logger,
		channels:    make(map[string]chan replicaJob),
		ctx:         ctx,
		cancel:      cancel,
	}
}
```

In `Enqueue`, after the non-blocking send, add lag metric:
```go
func (r *Replicator) Enqueue(entry *types.LogEntry, shardID int, addr string) {
	ch := r.getOrCreateChannel(addr)
	select {
	case ch <- replicaJob{entry: entry, shardID: shardID}:
		observability.ReplicationLagEntries.WithLabelValues(r.nodeID).Set(float64(len(ch)))
	default:
		r.logger.Warn("replication channel full, dropping entry",
			zap.String("addr", addr),
			zap.String("entry_id", entry.ID),
		)
	}
}
```

In `drain`, replace `log.Printf("replicator: connect...")` with:
```go
r.logger.Error("replicator connect failed", zap.String("addr", addr), zap.Error(err))
```

In `send`, replace `log.Printf` with:
```go
r.logger.Warn("ReplicateEntry failed", zap.String("entry_id", job.entry.ID), zap.Error(err))
```

- [ ] **Step 4: Update replicator test callers**

In `internal/replication/replicator_test.go`, add import `"go.uber.org/zap"` and update all 3 calls:
```go
r := replication.NewReplicator(4, "test-node", zap.NewNop())
```

In `test/integration/phase5_catchup_test.go`, add import `"go.uber.org/zap"` and update line 107:
```go
repl2 := replication.NewReplicator(totalShards, "node-b", zap.NewNop())
```

In `test/integration/phase5_node_test.go`, add import `"go.uber.org/zap"` and update line 100:
```go
repl := replication.NewReplicator(totalShards, nodeID, zap.NewNop())
```

- [ ] **Step 5: Run all tests**

```bash
go test ./...
```
Expected: PASS — all packages green.

---

## Task 8: Wire Observability into Node and Coordinator Binaries

**Files:**
- Modify: `cmd/node/main.go`
- Modify: `cmd/coordinator/main.go`

- [ ] **Step 1: Update `cmd/node/main.go`**

Add imports:
```go
"net/http"

"github.com/prometheus/client_golang/prometheus"
"github.com/prometheus/client_golang/prometheus/promhttp"
"go.uber.org/zap"

"github.com/Weilei424/distributed-log-query-engine/internal/observability"
```

At the top of `main()`, before any other setup:
```go
observability.Register(prometheus.DefaultRegisterer)
nodeLogger := observability.NewLogger("node", nodeID)
metricsAddr := envOrDefault("METRICS_ADDR", ":9090")
```

After `metricsAddr` is declared, start the metrics HTTP server:
```go
metricsMux := http.NewServeMux()
metricsMux.Handle("/metrics", promhttp.Handler())
metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsMux}
go func() {
    if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        nodeLogger.Warn("metrics server error", zap.Error(err))
    }
}()
```

Update `storage.NewManager` call to pass nodeID:
```go
manager, err := storage.NewManager(dataDir, maxSegBytes, storage.WithNodeID(nodeID))
```

Update `replication.NewReplicator`:
```go
repl := replication.NewReplicator(totalShards, nodeID, nodeLogger)
```

Update `cluster.NewHeartbeatSender`:
```go
sender := cluster.NewHeartbeatSender(clusterClient, heartbeatInterval, nodeID, nodeLogger)
```

Call `SetLogger` on the ingest server (after it is constructed — applies to all three construction paths):
```go
ingestSrv.SetLogger(nodeLogger)
```

Update `query.NewQueryServer`:
```go
querySrv = query.NewQueryServer(query.NewLocalExecutor(idx, manager), nodeID, nodeLogger)
```

Add metrics server shutdown in the cleanup block (after `grpcSrv.GracefulStop()`):
```go
shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
defer shutCancel()
if err := metricsSrv.Shutdown(shutCtx); err != nil {
    nodeLogger.Warn("metrics server shutdown error", zap.Error(err))
}
```

Replace `fmt.Printf` and `log.Fatalf` calls with zap where they're in the main startup path:
```go
// Replace: fmt.Printf("node started: ...")
nodeLogger.Info("node started", zap.String("addr", grpcAddr), zap.String("data", dataDir))
```

- [ ] **Step 2: Update `cmd/coordinator/main.go`**

Add imports:
```go
"github.com/prometheus/client_golang/prometheus"
"github.com/prometheus/client_golang/prometheus/promhttp"
"go.uber.org/zap"

"github.com/Weilei424/distributed-log-query-engine/internal/observability"
```

At the top of `main()`:
```go
observability.Register(prometheus.DefaultRegisterer)
coordLogger := observability.NewLogger("coordinator", nodeID)
```

Add `/metrics` to the existing mux (after `/status` handler):
```go
mux.Handle("/metrics", promhttp.Handler())
```

Update `coordinator.NewFanOutExecutor`:
```go
fanOutExec := coordinator.NewFanOutExecutor(fsm, nodeQueryTimeoutMs, fanOutLimit, coordLogger)
```

Replace the `fmt.Printf("coordinator started...")` line:
```go
coordLogger.Info("coordinator started",
    zap.String("raft_addr", bindAddr),
    zap.String("grpc_addr", grpcAddr),
    zap.String("http_addr", httpAddr),
    zap.Int("shards", totalShards),
)
```

- [ ] **Step 3: Run full test suite**

```bash
go test ./...
```
Expected: PASS.

- [ ] **Step 4: Build both binaries**

```bash
go build ./cmd/node && go build ./cmd/coordinator
```
Expected: exits 0.

---

## Task 9: Real Dockerfiles + Docker Compose + Prometheus + Grafana

**Files:**
- Modify: `deployments/docker-compose/Dockerfile.node`
- Modify: `deployments/docker-compose/Dockerfile.coordinator`
- Create: `deployments/docker-compose/prometheus.yml`
- Create: `deployments/docker-compose/grafana/provisioning/datasources/prometheus.yml`
- Create: `deployments/docker-compose/grafana/provisioning/dashboards/provider.yml`
- Create: `deployments/docker-compose/grafana/dashboards/logengine.json`
- Modify: `deployments/docker-compose/docker-compose.yml`

- [ ] **Step 1: Write real `Dockerfile.node`**

```dockerfile
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /node ./cmd/node

FROM alpine:3.19
COPY --from=builder /node /node
ENTRYPOINT ["/node"]
```

- [ ] **Step 2: Update `Dockerfile.coordinator` to add `go mod download` layer**

```dockerfile
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /coordinator ./cmd/coordinator

FROM alpine:3.19
COPY --from=builder /coordinator /coordinator
ENTRYPOINT ["/coordinator"]
```

- [ ] **Step 3: Write `deployments/docker-compose/prometheus.yml`**

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: coordinators
    static_configs:
      - targets:
          - coordinator-1:8080
          - coordinator-2:8080
          - coordinator-3:8080

  - job_name: nodes
    static_configs:
      - targets:
          - node-1:9090
          - node-2:9090
          - node-3:9090
```

- [ ] **Step 4: Write Grafana datasource provisioning**

`deployments/docker-compose/grafana/provisioning/datasources/prometheus.yml`:
```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    uid: prometheus
    url: http://prometheus:9090
    access: proxy
    isDefault: true
```

- [ ] **Step 5: Write Grafana dashboard provider**

`deployments/docker-compose/grafana/provisioning/dashboards/provider.yml`:
```yaml
apiVersion: 1
providers:
  - name: default
    type: file
    options:
      path: /etc/grafana/dashboards
```

- [ ] **Step 6: Write Grafana dashboard JSON**

`deployments/docker-compose/grafana/dashboards/logengine.json`:

```json
{
  "title": "Log Engine",
  "uid": "logengine-v1",
  "schemaVersion": 36,
  "version": 1,
  "refresh": "10s",
  "panels": [
    {
      "id": 1,
      "type": "timeseries",
      "title": "Ingestion Rate (entries/s)",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "rate(logengine_ingest_requests_total{status=\"ok\"}[1m])",
          "legendFormat": "{{node_id}}"
        }
      ]
    },
    {
      "id": 2,
      "type": "timeseries",
      "title": "Ingestion Error Rate (errors/s)",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "rate(logengine_ingest_requests_total{status=\"error\"}[1m])",
          "legendFormat": "{{node_id}}"
        }
      ]
    },
    {
      "id": 3,
      "type": "timeseries",
      "title": "Append Latency p50 / p95 (s)",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "histogram_quantile(0.5, rate(logengine_append_duration_seconds_bucket[1m]))",
          "legendFormat": "p50 {{node_id}}"
        },
        {
          "expr": "histogram_quantile(0.95, rate(logengine_append_duration_seconds_bucket[1m]))",
          "legendFormat": "p95 {{node_id}}"
        }
      ]
    },
    {
      "id": 4,
      "type": "timeseries",
      "title": "Local Query Latency p50 / p95 (s)",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "histogram_quantile(0.5, rate(logengine_query_duration_seconds_bucket{type=\"local\"}[1m]))",
          "legendFormat": "p50 {{node_id}}"
        },
        {
          "expr": "histogram_quantile(0.95, rate(logengine_query_duration_seconds_bucket{type=\"local\"}[1m]))",
          "legendFormat": "p95 {{node_id}}"
        }
      ]
    },
    {
      "id": 5,
      "type": "timeseries",
      "title": "Fan-Out Query Latency p50 / p95 (s)",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 16 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "histogram_quantile(0.5, rate(logengine_query_duration_seconds_bucket{type=\"fanout\"}[1m]))",
          "legendFormat": "p50"
        },
        {
          "expr": "histogram_quantile(0.95, rate(logengine_query_duration_seconds_bucket{type=\"fanout\"}[1m]))",
          "legendFormat": "p95"
        }
      ]
    },
    {
      "id": 6,
      "type": "timeseries",
      "title": "Active Segment Size (bytes)",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 16 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "logengine_active_segment_bytes",
          "legendFormat": "{{node_id}}"
        }
      ]
    },
    {
      "id": 7,
      "type": "stat",
      "title": "Node Health Status",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 24 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "options": {
        "colorMode": "background",
        "reduceOptions": { "calcs": ["lastNotNull"] }
      },
      "fieldConfig": {
        "defaults": {
          "mappings": [
            { "type": "value", "options": { "0": { "text": "Unhealthy", "color": "red" }, "1": { "text": "Healthy", "color": "green" } } }
          ]
        }
      },
      "targets": [
        {
          "expr": "logengine_node_health_status",
          "legendFormat": "{{node_id}}"
        }
      ]
    },
    {
      "id": 8,
      "type": "timeseries",
      "title": "Replication Lag (pending entries)",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 24 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        {
          "expr": "logengine_replication_lag_entries",
          "legendFormat": "{{node_id}}"
        }
      ]
    }
  ]
}
```

- [ ] **Step 7: Update `docker-compose.yml` — add metrics ports to nodes and add prometheus + grafana services**

Add `METRICS_ADDR=:9090` and expose port 9090 to each node service. For `node-1`:
```yaml
  node-1:
    build:
      context: ../..
      dockerfile: deployments/docker-compose/Dockerfile.node
    environment:
      - NODE_ID=node-1
      - GRPC_ADDR=:50051
      - NODE_GRPC_ADDR=node-1:50051
      - DATA_DIR=/data
      - COORDINATOR_ADDRS=coordinator-1:9000,coordinator-2:9000,coordinator-3:9000
      - METRICS_ADDR=:9090
    ports:
      - "50051:50051"
      - "9091:9090"
    volumes:
      - data-node-1:/data
    depends_on:
      - coordinator-1
      - coordinator-2
      - coordinator-3
```

Repeat for `node-2` (port `9092:9090`) and `node-3` (port `9093:9090`).

Append these two services at the end of the `services:` block:
```yaml
  prometheus:
    image: prom/prometheus:v2.51.0
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9095:9090"
    depends_on:
      - node-1
      - node-2
      - node-3

  grafana:
    image: grafana/grafana:10.4.0
    ports:
      - "3000:3000"
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning
      - ./grafana/dashboards:/etc/grafana/dashboards
    environment:
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Admin
    depends_on:
      - prometheus
```

- [ ] **Step 8: Validate docker-compose config**

```bash
docker compose -f deployments/docker-compose/docker-compose.yml config
```
Expected: prints the resolved config with no errors.

---

## Task 10: Helm Chart

**Files:** all under `deployments/kubernetes/helm/`

- [ ] **Step 1: Write `Chart.yaml`**

```yaml
apiVersion: v2
name: logengine
description: Distributed log query engine
version: 0.1.0
appVersion: "0.1.0"
```

- [ ] **Step 2: Write `values.yaml`**

```yaml
coordinator:
  replicas: 3
  image: logengine-coordinator:latest
  grpcPort: 9000
  httpPort: 8080
  totalShards: 16
  storageSize: 500Mi

node:
  replicas: 3
  image: logengine-node:latest
  grpcPort: 50051
  metricsPort: 9090
  storageSize: 1Gi
  maxSegmentBytes: 67108864

prometheus:
  enabled: true
  image: prom/prometheus:v2.51.0
  port: 9090
```

- [ ] **Step 3: Write `templates/_helpers.tpl`**

```
{{- define "logengine.raftPeers" -}}
{{- $replicas := .Values.coordinator.replicas | int -}}
{{- $peers := list -}}
{{- range $i := until $replicas -}}
{{- $name := printf "coordinator-%d" $i -}}
{{- $addr := printf "%s.coordinator.logengine.svc.cluster.local:7000" $name -}}
{{- $peers = append $peers (printf "%s=%s" $name $addr) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end -}}

{{- define "logengine.coordinatorAddrs" -}}
{{- $replicas := .Values.coordinator.replicas | int -}}
{{- $port := .Values.coordinator.grpcPort | int -}}
{{- $addrs := list -}}
{{- range $i := until $replicas -}}
{{- $addr := printf "coordinator-%d.coordinator.logengine.svc.cluster.local:%d" $i $port -}}
{{- $addrs = append $addrs $addr -}}
{{- end -}}
{{- join "," $addrs -}}
{{- end -}}
```

- [ ] **Step 4: Write `templates/namespace.yaml`**

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: logengine
```

- [ ] **Step 5: Write `templates/coordinator-statefulset.yaml`**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: coordinator
  namespace: logengine
spec:
  serviceName: coordinator
  replicas: {{ .Values.coordinator.replicas }}
  selector:
    matchLabels:
      app: coordinator
  template:
    metadata:
      labels:
        app: coordinator
    spec:
      containers:
      - name: coordinator
        image: {{ .Values.coordinator.image }}
        ports:
        - containerPort: {{ .Values.coordinator.grpcPort }}
          name: grpc
        - containerPort: {{ .Values.coordinator.httpPort }}
          name: http
        - containerPort: 7000
          name: raft
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: RAFT_NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: RAFT_BIND_ADDR
          value: "$(POD_NAME).coordinator.logengine.svc.cluster.local:7000"
        - name: RAFT_DATA_DIR
          value: /raft-data
        - name: RAFT_PEERS
          value: {{ include "logengine.raftPeers" . | quote }}
        - name: GRPC_ADDR
          value: ":{{ .Values.coordinator.grpcPort }}"
        - name: HTTP_ADDR
          value: ":{{ .Values.coordinator.httpPort }}"
        - name: TOTAL_SHARDS
          value: "{{ .Values.coordinator.totalShards }}"
        volumeMounts:
        - name: raft-data
          mountPath: /raft-data
  volumeClaimTemplates:
  - metadata:
      name: raft-data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: {{ .Values.coordinator.storageSize }}
```

- [ ] **Step 6: Write `templates/coordinator-service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: coordinator
  namespace: logengine
spec:
  clusterIP: None
  selector:
    app: coordinator
  ports:
  - name: grpc
    port: {{ .Values.coordinator.grpcPort }}
  - name: http
    port: {{ .Values.coordinator.httpPort }}
  - name: raft
    port: 7000
```

- [ ] **Step 7: Write `templates/node-statefulset.yaml`**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: node
  namespace: logengine
spec:
  serviceName: node
  replicas: {{ .Values.node.replicas }}
  selector:
    matchLabels:
      app: node
  template:
    metadata:
      labels:
        app: node
    spec:
      containers:
      - name: node
        image: {{ .Values.node.image }}
        ports:
        - containerPort: {{ .Values.node.grpcPort }}
          name: grpc
        - containerPort: {{ .Values.node.metricsPort }}
          name: metrics
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: GRPC_ADDR
          value: ":{{ .Values.node.grpcPort }}"
        - name: NODE_GRPC_ADDR
          value: "$(POD_NAME).node.logengine.svc.cluster.local:{{ .Values.node.grpcPort }}"
        - name: DATA_DIR
          value: /data
        - name: COORDINATOR_ADDRS
          value: {{ include "logengine.coordinatorAddrs" . | quote }}
        - name: METRICS_ADDR
          value: ":{{ .Values.node.metricsPort }}"
        - name: MAX_SEGMENT_BYTES
          value: "{{ .Values.node.maxSegmentBytes }}"
        volumeMounts:
        - name: data
          mountPath: /data
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: {{ .Values.node.storageSize }}
```

- [ ] **Step 8: Write `templates/node-service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: node
  namespace: logengine
spec:
  clusterIP: None
  selector:
    app: node
  ports:
  - name: grpc
    port: {{ .Values.node.grpcPort }}
  - name: metrics
    port: {{ .Values.node.metricsPort }}
```

- [ ] **Step 9: Write `templates/prometheus-configmap.yaml`**

```yaml
{{- if .Values.prometheus.enabled }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
  namespace: logengine
data:
  prometheus.yml: |
    global:
      scrape_interval: 15s
    scrape_configs:
      - job_name: coordinators
        static_configs:
          - targets:
            {{- range $i := until (.Values.coordinator.replicas | int) }}
              - coordinator-{{ $i }}.coordinator.logengine.svc.cluster.local:{{ $.Values.coordinator.httpPort }}
            {{- end }}
      - job_name: nodes
        static_configs:
          - targets:
            {{- range $i := until (.Values.node.replicas | int) }}
              - node-{{ $i }}.node.logengine.svc.cluster.local:{{ $.Values.node.metricsPort }}
            {{- end }}
{{- end }}
```

- [ ] **Step 10: Write `templates/prometheus-deployment.yaml`**

```yaml
{{- if .Values.prometheus.enabled }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
  namespace: logengine
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prometheus
  template:
    metadata:
      labels:
        app: prometheus
    spec:
      containers:
      - name: prometheus
        image: {{ .Values.prometheus.image }}
        args:
          - "--config.file=/etc/prometheus/prometheus.yml"
        ports:
        - containerPort: {{ .Values.prometheus.port }}
        volumeMounts:
        - name: config
          mountPath: /etc/prometheus
      volumes:
      - name: config
        configMap:
          name: prometheus-config
{{- end }}
```

- [ ] **Step 11: Write `templates/prometheus-service.yaml`**

```yaml
{{- if .Values.prometheus.enabled }}
apiVersion: v1
kind: Service
metadata:
  name: prometheus
  namespace: logengine
spec:
  selector:
    app: prometheus
  ports:
  - port: {{ .Values.prometheus.port }}
    name: http
{{- end }}
```

- [ ] **Step 12: Lint the chart**

```bash
helm lint deployments/kubernetes/helm/
```
Expected: `1 chart(s) linted, 0 chart(s) failed`.

---

## Task 11: Load Test Program

**Files:**
- Create: `test/load/main.go`
- Modify: `Makefile`

- [ ] **Step 1: Write `test/load/main.go`**

```go
// test/load/main.go — standalone load test binary. Run against a live cluster.
// Not a _test.go file; not part of `go test ./...`.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
)

var (
	addr     = flag.String("addr", "localhost:9001", "coordinator gRPC address")
	workers  = flag.Int("workers", 10, "concurrent goroutines per mode")
	duration = flag.Duration("duration", 30*time.Second, "test duration")
	mode     = flag.String("mode", "both", `"ingest", "query", or "both"`)
)

func main() {
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("dial %s: %v", *addr, err))
	}
	defer conn.Close()

	ingestClient := logengine.NewIngestServiceClient(conn)
	queryClient := logengine.NewQueryServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var (
		ingestTotal  atomic.Int64
		ingestErrors atomic.Int64
		queryTotal   atomic.Int64
		queryErrors  atomic.Int64
		queryPartial atomic.Int64
	)
	var latencies []int64
	var latMu sync.Mutex
	var wg sync.WaitGroup

	services := []string{"auth", "billing", "api", "worker", "scheduler"}

	if *mode == "ingest" || *mode == "both" {
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						svc := services[rand.Intn(len(services))]
						_, err := ingestClient.IngestBatch(ctx, &logengine.IngestBatchRequest{
							Entries: []*logengine.LogEntry{{
								Service:   svc,
								Level:     "INFO",
								Message:   fmt.Sprintf("load test message %d", rand.Int63()),
								Timestamp: time.Now().UnixNano(),
							}},
						})
						if err != nil {
							ingestErrors.Add(1)
						} else {
							ingestTotal.Add(1)
						}
					}
				}
			}()
		}
	}

	if *mode == "query" || *mode == "both" {
		keywords := []string{"load", "test", "message", "auth", "billing"}
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						kw := keywords[rand.Intn(len(keywords))]
						start := time.Now()
						resp, err := queryClient.Query(ctx, &logengine.QueryRequest{
							Keyword: kw,
							Limit:   20,
						})
						ms := time.Since(start).Milliseconds()
						if err != nil {
							queryErrors.Add(1)
						} else {
							queryTotal.Add(1)
							latMu.Lock()
							latencies = append(latencies, ms)
							latMu.Unlock()
							if resp.Partial {
								queryPartial.Add(1)
							}
						}
					}
				}
			}()
		}
	}

	wg.Wait()
	secs := duration.Seconds()

	if *mode == "ingest" || *mode == "both" {
		total := ingestTotal.Load()
		fmt.Printf("--- Ingest ---\n")
		fmt.Printf("  total:    %d entries\n", total)
		fmt.Printf("  rate:     %.0f/s\n", float64(total)/secs)
		fmt.Printf("  errors:   %d\n\n", ingestErrors.Load())
	}

	if *mode == "query" || *mode == "both" {
		total := queryTotal.Load()
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		p50, p95 := int64(0), int64(0)
		if n := len(latencies); n > 0 {
			p50 = latencies[n*50/100]
			p95 = latencies[n*95/100]
		}
		var partialPct float64
		if total > 0 {
			partialPct = float64(queryPartial.Load()) / float64(total) * 100
		}
		fmt.Printf("--- Query ---\n")
		fmt.Printf("  total:    %d queries\n", total)
		fmt.Printf("  p50:      %dms\n", p50)
		fmt.Printf("  p95:      %dms\n", p95)
		fmt.Printf("  partial:  %.1f%%\n", partialPct)
		fmt.Printf("  errors:   %d\n", queryErrors.Load())
	}
}
```

- [ ] **Step 2: Verify the load test compiles**

```bash
go build ./test/load/...
```
Expected: exits 0.

- [ ] **Step 3: Add `load-test` target to `Makefile`**

Add to `Makefile`:
```makefile
## load-test: run load test against a live cluster (ADDR, DURATION, MODE are optional)
load-test:
	go run ./test/load \
		-addr=$(or $(ADDR),localhost:9001) \
		-duration=$(or $(DURATION),30s) \
		-mode=$(or $(MODE),both)
```

---

## Task 12: Makefile, Runbook, Backlog, README

**Files:**
- Modify: `Makefile`
- Create: `docs/runbooks/failure-demo.md`
- Modify: `docs/planning/BACKLOG.md`
- Modify: `README.md`

- [ ] **Step 1: Update `make run-local` in `Makefile`**

Replace the existing `run-local` target:
```makefile
## run-local: start the full local cluster (nodes, coordinators, Prometheus, Grafana)
run-local:
	docker compose -f deployments/docker-compose/docker-compose.yml up --build
```

- [ ] **Step 2: Verify `make test` passes**

```bash
make test
```
Expected: exits 0.

- [ ] **Step 3: Verify `make lint` passes**

```bash
make lint
```
Expected: exits 0 with no lint errors.

- [ ] **Step 4: Write `docs/runbooks/failure-demo.md`**

```markdown
# Failure Demo Runbook

Demonstrates observable node failure and recovery using the local Docker Compose stack.

## Prerequisites

- Docker and Docker Compose installed
- `make run-local` has started the full stack

## Steps

### 1. Start the stack

```bash
make run-local
```

Wait ~10 seconds for all nodes to register and begin heartbeating.

### 2. Ingest baseline data

In a second terminal:

```bash
make load-test ADDR=localhost:9001 DURATION=20s MODE=ingest
```

### 3. Open Grafana

Navigate to http://localhost:3000 (no login required).
Open the **Log Engine** dashboard. Confirm:
- `NodeHealthStatus` shows 1 (green) for node-1, node-2, node-3
- Ingestion rate shows activity

### 4. Kill node-1

```bash
docker compose -f deployments/docker-compose/docker-compose.yml stop node-1
```

### 5. Observe degraded state

Within ~15 seconds (heartbeat timeout), the coordinator marks node-1 unhealthy.
On the Grafana dashboard:
- `NodeHealthStatus` for node-1 drops to 0 (red)
- Fan-out partial response count increases

Run a query against the coordinator to confirm `partial=true`:

```bash
make load-test ADDR=localhost:9001 DURATION=5s MODE=query
# partial: should be > 0%
```

### 6. Restart node-1

```bash
docker compose -f deployments/docker-compose/docker-compose.yml start node-1
```

### 7. Observe recovery

Within ~5 seconds, node-1 re-registers and begins heartbeating.
On the Grafana dashboard:
- `NodeHealthStatus` for node-1 returns to 1 (green)
- Partial response count returns to 0%

Run another query to confirm full results:

```bash
make load-test ADDR=localhost:9001 DURATION=5s MODE=query
# partial: should be 0.0%
```
```

- [ ] **Step 5: Update `docs/planning/BACKLOG.md` — mark Phase 7 items complete**

Change all Phase 7 `- [ ]` items to `- [x]`.

- [ ] **Step 6: Update `README.md`**

Add a Phase 7 section describing:
- How to start the full observability stack: `make run-local`
- Grafana URL: http://localhost:3000
- Prometheus URL: http://localhost:9095
- How to run the load test: `make load-test`
- How to run the failure demo: `docs/runbooks/failure-demo.md`

- [ ] **Step 7: Final validation**

```bash
make build && make test && make lint
```
Expected: all three exit 0.

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| Prometheus metrics on all services | Tasks 2–8 |
| Ingestion rate, failure count, append latency | Tasks 2–4 |
| Active segment size, mounted segments, index token count | Tasks 2–4 |
| Local query latency, fan-out latency, timeout count, partial count | Tasks 2, 5–6 |
| Heartbeat health status, replication lag | Tasks 2, 7 |
| Structured logs with stable keys | Tasks 4–8 |
| Request IDs propagated through write and query paths | Tasks 2, 4 |
| Grafana dashboard auto-provisioned | Task 9 |
| Real Dockerfiles | Task 9 |
| Docker Compose with Prometheus + Grafana | Task 9 |
| Kubernetes Helm chart | Task 10 |
| Load test program | Task 11 |
| Failure scenario demo | Task 12 |
| `make run-local` starts full stack | Task 12 |
| `make test` passes | Task 12 |

**No placeholders found.** All steps contain complete code.

**Type consistency:** `observability.Register`, `observability.NewLogger`, `observability.NewRequestID`, `observability.WithRequestID`, `observability.RequestIDFromContext` — all defined in Task 2 and used consistently across Tasks 4–8. `storage.WithNodeID` defined in Task 3, used in Task 8. All constructor changes in Tasks 5–7 match their usage in Task 8.
