# Phase 5: Distributed Ingestion, Partitioning, and Replication — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Distribute log writes across nodes using hash-based shard routing, async primary→replica replication, and lightweight catch-up on node restart.

**Architecture:** Any node can receive a write; an `IngestionOrchestrator` computes the target shard, writes locally if this node is the primary, or forwards to the primary node via gRPC. After a local write the primary enqueues an async `ReplicateEntry` call to the replica node. On restart the replica fetches missing entries from the primary before accepting traffic. The FSM is updated to distribute shards evenly across healthy nodes and assign one replica per shard.

**Tech Stack:** Go, gRPC, Protocol Buffers (buf), FNV-1a hash, HashiCorp Raft (existing).

**Spec:** `docs/superpowers/specs/2026-04-17-phase5-distributed-ingest-replication-design.md`

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Modify | `internal/metadata/state.go` | Add `ReplicaNode string` to `ShardRecord` |
| Modify | `internal/metadata/fsm.go` | Replace greedy assignment with `rebalancePrimary()` + `assignReplicas()` |
| Modify | `internal/metadata/fsm_test.go` | Update broken test + add two new tests |
| Modify | `proto/logengine/v1/cluster.proto` | Add `replica_node` to `ShardInfo` |
| Modify | `proto/logengine/v1/ingest.proto` | Add `ReplicateEntry` + `FetchShardEntries` RPCs |
| Modify | `internal/metadata/server.go` | Include `ReplicaNode` in `GetClusterState` response |
| Create | `internal/ingest/router.go` | `ShardID(service, totalShards) int` (FNV-1a) |
| Create | `internal/ingest/router_test.go` | Determinism, range, distribution, zero-length |
| Modify | `internal/cluster/client.go` | Add `GetClusterState(ctx) (metadata.ClusterState, error)` |
| Create | `internal/cluster/state_cache.go` | `ClusterStateReader` interface + `StateCache` implementation |
| Create | `internal/replication/replicator.go` | Async worker: buffered channel per replica, drain goroutine |
| Create | `internal/replication/replicator_test.go` | Non-blocking enqueue, delivery, full-channel drop |
| Create | `internal/ingest/convert.go` | `ProtoToEntry`, `EntryToProto`, `GenerateID` (exported) |
| Create | `internal/ingest/orchestrator.go` | `IngestionOrchestrator`: route/forward/replicate |
| Modify | `internal/ingest/server.go` | Thin adapter + `ReplicateEntry` + `FetchShardEntries` + `NewLocalServer` |
| Modify | `internal/ingest/server_test.go` | Update `NewServer` → `NewLocalServer` calls |
| Modify | `test/integration/ingest_test.go` | Update `NewServer` → `NewLocalServer` calls |
| Modify | `cmd/node/main.go` | Wire state cache, replicator, orchestrator; add catch-up |
| Create | `test/integration/phase5_routing_test.go` | Write to non-owning node → appears on primary |
| Create | `test/integration/phase5_replication_test.go` | Write to primary → appears on replica |
| Create | `test/integration/phase5_failure_test.go` | Primary stopped → replica still serves logs |
| Create | `test/integration/phase5_catchup_test.go` | Replica restarts → catches up before serving |
| Modify | `docs/planning/BACKLOG.md` | Update Phase 5 checklist |

---

## Task 1: FSM Data Model and Shard Rebalance

**Files:**
- Modify: `internal/metadata/state.go`
- Modify: `internal/metadata/fsm.go`
- Modify: `internal/metadata/fsm_test.go`

**Background:** The existing FSM uses "greedy" assignment: the first node to register claims all unowned shards; subsequent nodes get none as primary. Phase 5 requires multiple nodes to own different primary shards for routing to be meaningful. This task replaces greedy assignment with round-robin rebalance across all healthy nodes. `TestFSM_RegisterNode_SecondNodeGetsNoShards` explicitly tests the old behavior and must be updated.

- [ ] **Step 1: Add `ReplicaNode` to `ShardRecord` in `state.go`**

```go
// internal/metadata/state.go

// ShardRecord holds the ownership mapping for a single shard.
type ShardRecord struct {
    ShardID     int
    PrimaryNode string // node ID; empty if unowned
    ReplicaNode string // node ID; empty if no replica assigned
}
```

Leave `NodeRecord`, `NodeStatus`, and `ClusterState` unchanged.

- [ ] **Step 2: Rewrite `applyRegisterNode`, add `rebalancePrimary` and `assignReplicas` helpers in `fsm.go`**

Replace the body of `applyRegisterNode` and `applyMarkUnhealthy` with the following. Add the two private helpers below them.

```go
// internal/metadata/fsm.go
// Replace applyRegisterNode:

func (f *FSM) applyRegisterNode(p RegisterNodePayload) error {
    existing, ok := f.state.Nodes[p.NodeID]
    if ok && existing.Status == NodeHealthy {
        // Already healthy: refresh address and last seen, no shard change.
        existing.Address = p.Address
        existing.LastSeen = p.NowUnixNs
        f.state.Nodes[p.NodeID] = existing
        return nil
    }

    // New node or rejoining after being marked unhealthy.
    // Register with no shards; rebalancePrimary distributes them.
    f.state.Nodes[p.NodeID] = NodeRecord{
        ID:       p.NodeID,
        Address:  p.Address,
        Status:   NodeHealthy,
        LastSeen: p.NowUnixNs,
    }

    f.rebalancePrimary()
    f.assignReplicas()
    return nil
}

// Replace applyMarkUnhealthy:

func (f *FSM) applyMarkUnhealthy(p MarkUnhealthyPayload) error {
    node, ok := f.state.Nodes[p.NodeID]
    if !ok {
        return fmt.Errorf("node not found: %s", p.NodeID)
    }
    // Release primary ownership.
    for _, shardID := range node.Shards {
        sr := f.state.Shards[shardID]
        sr.PrimaryNode = ""
        f.state.Shards[shardID] = sr
    }
    // Clear replica slots where this node was the replica.
    for id, sr := range f.state.Shards {
        if sr.ReplicaNode == p.NodeID {
            sr.ReplicaNode = ""
            f.state.Shards[id] = sr
        }
    }
    node.Status = NodeUnhealthy
    node.Shards = nil
    f.state.Nodes[p.NodeID] = node

    // Reassign replica slots among remaining healthy nodes.
    f.assignReplicas()
    return nil
}

// Add after applyMarkUnhealthy:

// rebalancePrimary distributes all shards round-robin across healthy nodes.
// Called whenever a node joins. Does NOT migrate physical data — only metadata.
func (f *FSM) rebalancePrimary() {
    var healthyIDs []string
    for id, n := range f.state.Nodes {
        if n.Status == NodeHealthy {
            healthyIDs = append(healthyIDs, id)
        }
    }
    if len(healthyIDs) == 0 {
        return
    }
    sort.Strings(healthyIDs)

    var allShards []int
    for id := range f.state.Shards {
        allShards = append(allShards, id)
    }
    sort.Ints(allShards)

    // Clear existing primary assignments and node shard lists.
    for id, sr := range f.state.Shards {
        sr.PrimaryNode = ""
        f.state.Shards[id] = sr
    }
    for id, n := range f.state.Nodes {
        n.Shards = nil
        f.state.Nodes[id] = n
    }

    // Assign round-robin.
    for i, shardID := range allShards {
        nodeID := healthyIDs[i%len(healthyIDs)]
        sr := f.state.Shards[shardID]
        sr.PrimaryNode = nodeID
        f.state.Shards[shardID] = sr
        n := f.state.Nodes[nodeID]
        n.Shards = append(n.Shards, shardID)
        f.state.Nodes[nodeID] = n
    }

    for id, n := range f.state.Nodes {
        sort.Ints(n.Shards)
        f.state.Nodes[id] = n
    }
}

// assignReplicas assigns the first healthy non-primary node as the replica for each shard.
// Clears all existing replica assignments before reassigning.
func (f *FSM) assignReplicas() {
    var healthyIDs []string
    for id, n := range f.state.Nodes {
        if n.Status == NodeHealthy {
            healthyIDs = append(healthyIDs, id)
        }
    }
    sort.Strings(healthyIDs)

    for id, sr := range f.state.Shards {
        sr.ReplicaNode = ""
        for _, nodeID := range healthyIDs {
            if nodeID != sr.PrimaryNode {
                sr.ReplicaNode = nodeID
                break
            }
        }
        f.state.Shards[id] = sr
    }
}
```

- [ ] **Step 3: Update and add FSM tests in `fsm_test.go`**

Replace `TestFSM_RegisterNode_SecondNodeGetsNoShards` with a test that reflects the new round-robin behavior, and add two new tests for replica assignment.

```go
// internal/metadata/fsm_test.go

// Replace TestFSM_RegisterNode_SecondNodeGetsNoShards with:
func TestFSM_RegisterNode_TwoNodesShareShards(t *testing.T) {
    fsm := metadata.NewFSM(4)
    applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051", NowUnixNs: 1000})
    applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-2", Address: ":50052", NowUnixNs: 1000})

    state := fsm.State()
    n1 := len(state.Nodes["node-1"].Shards)
    n2 := len(state.Nodes["node-2"].Shards)
    if n1 != 2 {
        t.Errorf("node-1 expected 2 shards (round-robin, 4 total / 2 nodes), got %d", n1)
    }
    if n2 != 2 {
        t.Errorf("node-2 expected 2 shards (round-robin, 4 total / 2 nodes), got %d", n2)
    }
    // Every shard must have a primary.
    for shardID, sr := range state.Shards {
        if sr.PrimaryNode == "" {
            t.Errorf("shard %d has no primary after two-node registration", shardID)
        }
    }
}

// Add after TestFSM_RegisterNode_TwoNodesShareShards:
func TestFSM_RegisterNode_SecondNodeAssignedAsReplica(t *testing.T) {
    fsm := metadata.NewFSM(4)
    applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051", NowUnixNs: 1000})
    applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-2", Address: ":50052", NowUnixNs: 1000})

    state := fsm.State()
    for shardID, sr := range state.Shards {
        if sr.ReplicaNode == "" {
            t.Errorf("shard %d has no replica after two-node registration", shardID)
        }
        if sr.ReplicaNode == sr.PrimaryNode {
            t.Errorf("shard %d: replica %q equals primary %q", shardID, sr.ReplicaNode, sr.PrimaryNode)
        }
    }
}

func TestFSM_MarkUnhealthy_ClearsReplicaSlot(t *testing.T) {
    fsm := metadata.NewFSM(4)
    applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051", NowUnixNs: 1000})
    applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-2", Address: ":50052", NowUnixNs: 1000})
    applyCmd(t, fsm, metadata.CmdMarkUnhealthy, metadata.MarkUnhealthyPayload{NodeID: "node-2"})

    state := fsm.State()
    for shardID, sr := range state.Shards {
        if sr.ReplicaNode == "node-2" {
            t.Errorf("shard %d still has node-2 as replica after mark-unhealthy", shardID)
        }
    }
}
```

- [ ] **Step 4: Run unit tests to verify FSM passes**

```bash
cd /mnt/d/projects/distributed-log-query-engine
go test ./internal/metadata/... -v -run TestFSM
```

Expected: all `TestFSM_*` tests pass.

- [ ] **Step 5: Run full test suite to check for regressions**

```bash
make test
```

Expected: all tests pass. If `TestRejoin_NodeAppearsHealthyAfterRestart` fails, verify that after mark-unhealthy + re-register with 1 healthy node, `rebalancePrimary` gives all 4 shards to that node (`len(shards) != 0` check passes).

---

## Task 2: Proto Changes

**Files:**
- Modify: `proto/logengine/v1/cluster.proto`
- Modify: `proto/logengine/v1/ingest.proto`
- Run: `buf generate`

- [ ] **Step 1: Add `replica_node` to `ShardInfo` in `cluster.proto`**

```proto
// proto/logengine/v1/cluster.proto
// Replace ShardInfo message:

message ShardInfo {
  int32  shard_id     = 1;
  string primary_node = 2;
  string replica_node = 3;
}
```

- [ ] **Step 2: Add `ReplicateEntry` and `FetchShardEntries` to `ingest.proto`**

```proto
// proto/logengine/v1/ingest.proto
// Add to IngestService (after IngestBatch):

service IngestService {
  rpc Ingest(IngestRequest) returns (IngestResponse);
  rpc IngestBatch(IngestBatchRequest) returns (IngestBatchResponse);

  // ReplicateEntry is called by the primary to deliver an async replica copy.
  // The receiving node writes directly to local storage — no further routing.
  rpc ReplicateEntry(ReplicateEntryRequest) returns (ReplicateEntryResponse);

  // FetchShardEntries returns entries for a shard after a given received_at timestamp.
  // Called by a replica node during catch-up after restart.
  rpc FetchShardEntries(FetchShardEntriesRequest) returns (FetchShardEntriesResponse);
}

message ReplicateEntryRequest {
  LogEntry entry    = 1;
  int32    shard_id = 2;
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

- [ ] **Step 3: Regenerate Go bindings**

```bash
cd /mnt/d/projects/distributed-log-query-engine
buf generate
```

Expected: `internal/api/gen/logengine/v1/` files updated. `ingest.pb.go` includes `ReplicateEntryRequest`, `ReplicateEntryResponse`, `FetchShardEntriesRequest`, `FetchShardEntriesResponse`. `cluster.pb.go` includes `ShardInfo.ReplicaNode`. `ingest_grpc.pb.go` includes the two new RPC methods.

- [ ] **Step 4: Verify build still compiles**

```bash
go build ./...
```

Expected: exits 0. (New proto RPCs are on `UnimplementedIngestServiceServer` so existing server still compiles.)

---

## Task 3: Metadata Server — Expose ReplicaNode in GetClusterState

**Files:**
- Modify: `internal/metadata/server.go`

- [ ] **Step 1: Add `ReplicaNode` to `ShardInfo` in `GetClusterState`**

In `GetClusterState`, find the loop that builds `shards` and add `ReplicaNode`:

```go
// internal/metadata/server.go
// Replace the shards loop in GetClusterState:

shards := make([]*logengine.ShardInfo, 0, len(state.Shards))
for _, sr := range state.Shards {
    shards = append(shards, &logengine.ShardInfo{
        ShardId:     int32(sr.ShardID),
        PrimaryNode: sr.PrimaryNode,
        ReplicaNode: sr.ReplicaNode,
    })
}
```

- [ ] **Step 2: Build and run metadata tests**

```bash
go build ./internal/metadata/...
go test ./internal/metadata/... -v
```

Expected: all pass.

---

## Task 4: Shard Router

**Files:**
- Create: `internal/ingest/router.go`
- Create: `internal/ingest/router_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/ingest/router_test.go
package ingest_test

import (
    "testing"

    "github.com/Weilei424/distributed-log-query-engine/internal/ingest"
)

func TestShardID_Deterministic(t *testing.T) {
    got1 := ingest.ShardID("payments", 16)
    got2 := ingest.ShardID("payments", 16)
    if got1 != got2 {
        t.Fatalf("ShardID not deterministic: %d != %d", got1, got2)
    }
}

func TestShardID_InRange(t *testing.T) {
    for _, svc := range []string{"auth", "payments", "api", "worker", "cache", "db"} {
        id := ingest.ShardID(svc, 16)
        if id < 0 || id >= 16 {
            t.Fatalf("ShardID(%q, 16) = %d, want [0, 16)", svc, id)
        }
    }
}

func TestShardID_EmptyService(t *testing.T) {
    id := ingest.ShardID("", 16)
    if id < 0 || id >= 16 {
        t.Fatalf("ShardID(\"\", 16) = %d, out of range", id)
    }
}

func TestShardID_Distribution(t *testing.T) {
    services := []string{"auth", "payments", "api", "worker", "cache", "db", "search", "notify"}
    seen := make(map[int]bool)
    for _, svc := range services {
        seen[ingest.ShardID(svc, 16)] = true
    }
    if len(seen) < 4 {
        t.Fatalf("poor distribution: only %d distinct shards for %d services", len(seen), len(services))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/ingest/... -run TestShardID -v
```

Expected: compile error (ShardID undefined).

- [ ] **Step 3: Implement `router.go`**

```go
// internal/ingest/router.go
package ingest

import "hash/fnv"

// ShardID computes the shard ID for a log entry based on its service name.
// Uses FNV-1a hash modulo totalShards. Deterministic across all nodes:
// given the same service and total shard count, every node returns the same ID.
func ShardID(service string, totalShards int) int {
    if totalShards <= 0 {
        return 0
    }
    h := fnv.New32a()
    h.Write([]byte(service))
    return int(h.Sum32()) % totalShards
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/ingest/... -run TestShardID -v
```

Expected: all four `TestShardID_*` tests pass.

---

## Task 5: ClusterClient.GetClusterState and StateCache

**Files:**
- Modify: `internal/cluster/client.go`
- Create: `internal/cluster/state_cache.go`

- [ ] **Step 1: Add import and `GetClusterState` method to `client.go`**

Add `"github.com/Weilei424/distributed-log-query-engine/internal/metadata"` to the import block, then add this method to `ClusterClient`:

```go
// internal/cluster/client.go — add to ClusterClient

// GetClusterState fetches the current cluster state from the coordinator.
// Any coordinator can serve this request (no leader routing needed).
func (c *ClusterClient) GetClusterState(ctx context.Context) (metadata.ClusterState, error) {
    resp, err := c.client.GetClusterState(ctx, &logengine.GetClusterStateRequest{})
    if err != nil {
        return metadata.ClusterState{}, err
    }
    return protoToClusterState(resp), nil
}

// protoToClusterState converts the proto GetClusterStateResponse to internal ClusterState.
func protoToClusterState(resp *logengine.GetClusterStateResponse) metadata.ClusterState {
    nodes := make(map[string]metadata.NodeRecord, len(resp.Nodes))
    for _, n := range resp.Nodes {
        shards := make([]int, len(n.Shards))
        for i, s := range n.Shards {
            shards[i] = int(s)
        }
        nodes[n.Id] = metadata.NodeRecord{
            ID:      n.Id,
            Address: n.Address,
            Status:  metadata.NodeStatus(n.Status),
            Shards:  shards,
        }
    }
    shards := make(map[int]metadata.ShardRecord, len(resp.Shards))
    for _, s := range resp.Shards {
        shards[int(s.ShardId)] = metadata.ShardRecord{
            ShardID:     int(s.ShardId),
            PrimaryNode: s.PrimaryNode,
            ReplicaNode: s.ReplicaNode,
        }
    }
    return metadata.ClusterState{Nodes: nodes, Shards: shards}
}
```

- [ ] **Step 2: Verify build**

```bash
go build ./internal/cluster/...
```

Expected: exits 0.

- [ ] **Step 3: Create `state_cache.go`**

```go
// internal/cluster/state_cache.go
package cluster

import (
    "context"
    "log"
    "sync"
    "time"

    "github.com/Weilei424/distributed-log-query-engine/internal/metadata"
)

// ClusterStateReader provides routing information derived from cluster state.
// The orchestrator uses this to make routing and replication decisions.
type ClusterStateReader interface {
    // ShardOwners returns the primary and replica node IDs for the given shard.
    // Returns empty strings if the shard is unknown or unowned.
    ShardOwners(shardID int) (primaryNodeID, replicaNodeID string)
    // NodeAddress returns the gRPC address of the given node ID.
    // Returns empty string if the node is unknown.
    NodeAddress(nodeID string) string
}

// StateCache polls the coordinator for cluster state and serves routing
// queries from the cached result. Storage nodes use this to make routing
// decisions without blocking on a live coordinator RPC during writes.
type StateCache struct {
    mu       sync.RWMutex
    state    metadata.ClusterState
    client   *ClusterClient
    interval time.Duration
}

// NewStateCache creates a StateCache backed by the given ClusterClient.
func NewStateCache(client *ClusterClient, interval time.Duration) *StateCache {
    return &StateCache{
        client:   client,
        interval: interval,
        state: metadata.ClusterState{
            Nodes:  make(map[string]metadata.NodeRecord),
            Shards: make(map[int]metadata.ShardRecord),
        },
    }
}

// Refresh fetches the current state immediately. Call once before accepting traffic
// to ensure the cache is populated before the first routing decision.
func (c *StateCache) Refresh(ctx context.Context) {
    c.refresh(ctx)
}

// Run starts the background polling loop. Blocks until ctx is cancelled.
func (c *StateCache) Run(ctx context.Context) {
    ticker := time.NewTicker(c.interval)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            c.refresh(ctx)
        }
    }
}

func (c *StateCache) refresh(ctx context.Context) {
    rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()
    state, err := c.client.GetClusterState(rctx)
    if err != nil {
        log.Printf("state_cache: refresh failed: %v (retaining last known state)", err)
        return
    }
    c.mu.Lock()
    c.state = state
    c.mu.Unlock()
}

// ShardOwners implements ClusterStateReader.
func (c *StateCache) ShardOwners(shardID int) (primaryNodeID, replicaNodeID string) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    sr, ok := c.state.Shards[shardID]
    if !ok {
        return "", ""
    }
    return sr.PrimaryNode, sr.ReplicaNode
}

// NodeAddress implements ClusterStateReader.
func (c *StateCache) NodeAddress(nodeID string) string {
    c.mu.RLock()
    defer c.mu.RUnlock()
    n, ok := c.state.Nodes[nodeID]
    if !ok {
        return ""
    }
    return n.Address
}
```

- [ ] **Step 4: Verify build**

```bash
go build ./internal/cluster/...
```

Expected: exits 0.

---

## Task 6: Replicator

**Files:**
- Create: `internal/replication/replicator.go`
- Create: `internal/replication/replicator_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// internal/replication/replicator_test.go
package replication_test

import (
    "context"
    "net"
    "sync/atomic"
    "testing"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
    "github.com/Weilei424/distributed-log-query-engine/internal/replication"
    "github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// fakeIngestServer counts ReplicateEntry calls.
type fakeIngestServer struct {
    logengine.UnimplementedIngestServiceServer
    received atomic.Int32
}

func (f *fakeIngestServer) ReplicateEntry(_ context.Context, req *logengine.ReplicateEntryRequest) (*logengine.ReplicateEntryResponse, error) {
    f.received.Add(1)
    return &logengine.ReplicateEntryResponse{Ok: true}, nil
}

func startFakeReplica(t *testing.T) (addr string, fake *fakeIngestServer) {
    t.Helper()
    fake = &fakeIngestServer{}
    lis, err := net.Listen("tcp", ":0")
    if err != nil {
        t.Fatalf("listen: %v", err)
    }
    srv := grpc.NewServer()
    logengine.RegisterIngestServiceServer(srv, fake)
    go srv.Serve(lis) //nolint:errcheck
    t.Cleanup(srv.GracefulStop)
    return lis.Addr().String(), fake
}

func TestReplicator_DeliverEntry(t *testing.T) {
    addr, fake := startFakeReplica(t)

    r := replication.NewReplicator(4)
    t.Cleanup(r.Stop)

    entry := &types.LogEntry{ID: "e1", Service: "auth", Message: "hello"}
    r.Enqueue(entry, 0, addr)

    deadline := time.Now().Add(2 * time.Second)
    for time.Now().Before(deadline) {
        if fake.received.Load() >= 1 {
            return
        }
        time.Sleep(10 * time.Millisecond)
    }
    t.Fatalf("entry not delivered to replica within 2 seconds")
}

func TestReplicator_EnqueueNonBlocking(t *testing.T) {
    // Use an address that no server listens on — connections will fail.
    // The channel should still accept entries without blocking.
    r := replication.NewReplicator(4)
    t.Cleanup(r.Stop)

    entry := &types.LogEntry{ID: "e1", Service: "auth", Message: "hello"}
    done := make(chan struct{})
    go func() {
        defer close(done)
        // Fill beyond capacity to trigger the drop path.
        for i := 0; i < 300; i++ {
            r.Enqueue(entry, 0, "localhost:19999")
        }
    }()
    select {
    case <-done:
        // All 300 calls returned without blocking — pass.
    case <-time.After(2 * time.Second):
        t.Fatal("Enqueue blocked for more than 2 seconds")
    }
}

func TestReplicator_StopsCleanly(t *testing.T) {
    addr, _ := startFakeReplica(t)
    r := replication.NewReplicator(4)

    entry := &types.LogEntry{ID: "e1", Service: "auth", Message: "hello"}
    r.Enqueue(entry, 0, addr)

    done := make(chan struct{})
    go func() {
        defer close(done)
        r.Stop()
    }()
    select {
    case <-done:
    case <-time.After(3 * time.Second):
        t.Fatal("Stop did not return within 3 seconds")
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./internal/replication/... -v
```

Expected: compile error (replication package has no exported symbols yet).

- [ ] **Step 3: Implement `replicator.go`**

```go
// internal/replication/replicator.go
package replication

import (
    "context"
    "log"
    "sync"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
    "github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

const channelCapacity = 256

type replicaJob struct {
    entry   *types.LogEntry
    shardID int
}

// Replicator asynchronously delivers log entries to replica nodes via ReplicateEntry RPC.
// It maintains one buffered channel and one drain goroutine per target address.
type Replicator struct {
    totalShards int

    mu       sync.Mutex
    channels map[string]chan replicaJob
    wg       sync.WaitGroup
    ctx      context.Context
    cancel   context.CancelFunc
}

// NewReplicator creates a Replicator. Call Stop to shut down gracefully.
func NewReplicator(totalShards int) *Replicator {
    ctx, cancel := context.WithCancel(context.Background())
    return &Replicator{
        totalShards: totalShards,
        channels:    make(map[string]chan replicaJob),
        ctx:         ctx,
        cancel:      cancel,
    }
}

// Enqueue schedules an entry for async replication to addr.
// Non-blocking: if the channel is full the entry is dropped and logged.
func (r *Replicator) Enqueue(entry *types.LogEntry, shardID int, addr string) {
    ch := r.getOrCreateChannel(addr)
    select {
    case ch <- replicaJob{entry: entry, shardID: shardID}:
    default:
        log.Printf("replicator: channel full for %s, dropping entry %s", addr, entry.ID)
    }
}

func (r *Replicator) getOrCreateChannel(addr string) chan replicaJob {
    r.mu.Lock()
    defer r.mu.Unlock()
    if ch, ok := r.channels[addr]; ok {
        return ch
    }
    ch := make(chan replicaJob, channelCapacity)
    r.channels[addr] = ch
    r.wg.Add(1)
    go r.drain(addr, ch)
    return ch
}

func (r *Replicator) drain(addr string, ch chan replicaJob) {
    defer r.wg.Done()
    conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        log.Printf("replicator: connect to %s failed: %v", addr, err)
        return
    }
    defer conn.Close()
    client := logengine.NewIngestServiceClient(conn)

    for {
        select {
        case <-r.ctx.Done():
            // Drain remaining with a short deadline.
            deadline := time.Now().Add(2 * time.Second)
            for {
                select {
                case job := <-ch:
                    ctx, cancel := context.WithDeadline(context.Background(), deadline)
                    r.send(ctx, client, job)
                    cancel()
                default:
                    return
                }
            }
        case job := <-ch:
            r.send(r.ctx, client, job)
        }
    }
}

func (r *Replicator) send(ctx context.Context, client logengine.IngestServiceClient, job replicaJob) {
    pb := entryToProto(job.entry)
    _, err := client.ReplicateEntry(ctx, &logengine.ReplicateEntryRequest{
        Entry:   pb,
        ShardId: int32(job.shardID),
    })
    if err != nil {
        log.Printf("replicator: ReplicateEntry entry %s failed: %v", job.entry.ID, err)
    }
}

// Stop signals all drain goroutines to finish in-flight entries and exit.
func (r *Replicator) Stop() {
    r.cancel()
    r.wg.Wait()
}

func entryToProto(e *types.LogEntry) *logengine.LogEntry {
    return &logengine.LogEntry{
        Id:         e.ID,
        Timestamp:  e.Timestamp,
        ReceivedAt: e.ReceivedAt,
        Service:    e.Service,
        Level:      e.Level,
        Message:    e.Message,
        Fields:     e.Fields,
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./internal/replication/... -v -timeout 30s
```

Expected: all three `TestReplicator_*` tests pass.

---

## Task 7: ingest/convert.go and IngestionOrchestrator

**Files:**
- Create: `internal/ingest/convert.go`
- Create: `internal/ingest/orchestrator.go`

**Note:** `protoToEntry` and `generateID` currently live unexported in `server.go`. This task extracts them to `convert.go` as exported functions so both `orchestrator.go` and `cmd/node/main.go` can use them without duplication. The unexported originals in `server.go` will be removed in Task 8.

- [ ] **Step 1: Create `convert.go`**

```go
// internal/ingest/convert.go
package ingest

import (
    "crypto/rand"
    "encoding/hex"
    "fmt"
    "time"

    logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
    "github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// ProtoToEntry converts a proto LogEntry to the internal types.LogEntry.
func ProtoToEntry(pb *logengine.LogEntry) *types.LogEntry {
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

// EntryToProto converts an internal types.LogEntry to proto LogEntry.
func EntryToProto(e *types.LogEntry) *logengine.LogEntry {
    return &logengine.LogEntry{
        Id:         e.ID,
        Timestamp:  e.Timestamp,
        ReceivedAt: e.ReceivedAt,
        Service:    e.Service,
        Level:      e.Level,
        Message:    e.Message,
        Fields:     e.Fields,
    }
}

// GenerateID returns a random ID for entries that omit one on ingest.
// Format: "auto-<16 hex chars>".
func GenerateID() string {
    b := make([]byte, 8)
    if _, err := rand.Read(b); err != nil {
        return fmt.Sprintf("auto-%d", time.Now().UnixNano())
    }
    return "auto-" + hex.EncodeToString(b)
}
```

- [ ] **Step 2: Create `orchestrator.go`**

```go
// internal/ingest/orchestrator.go
package ingest

import (
    "context"
    "fmt"
    "sync"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/status"

    logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
    "github.com/Weilei424/distributed-log-query-engine/internal/cluster"
    "github.com/Weilei424/distributed-log-query-engine/internal/index"
    "github.com/Weilei424/distributed-log-query-engine/internal/replication"
    "github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

// Orchestrator handles distributed write logic: shard routing, forwarding, and replication.
// It is the single place where distributed write decisions are made.
type Orchestrator struct {
    nodeID      string
    totalShards int            // 0 = local mode (no routing)
    stateReader cluster.ClusterStateReader
    manager     *storage.Manager
    idx         *index.Index
    replicator  *replication.Replicator

    mu      sync.Mutex
    clients map[string]logengine.IngestServiceClient // addr → gRPC client (cached)
}

// NewOrchestrator creates an Orchestrator for cluster-aware routing.
func NewOrchestrator(
    nodeID string,
    totalShards int,
    stateReader cluster.ClusterStateReader,
    manager *storage.Manager,
    idx *index.Index,
    replicator *replication.Replicator,
) *Orchestrator {
    return &Orchestrator{
        nodeID:      nodeID,
        totalShards: totalShards,
        stateReader: stateReader,
        manager:     manager,
        idx:         idx,
        replicator:  replicator,
        clients:     make(map[string]logengine.IngestServiceClient),
    }
}

// newLocalOrchestrator creates an Orchestrator that always writes locally (no routing).
// Used when the node runs without a coordinator.
func newLocalOrchestrator(manager *storage.Manager, idx *index.Index) *Orchestrator {
    return &Orchestrator{
        totalShards: 0,
        manager:     manager,
        idx:         idx,
        clients:     make(map[string]logengine.IngestServiceClient),
    }
}

// HandleIngest routes an ingest request: local write if this node is the primary,
// or forward to the primary node. Validation happens before routing.
func (o *Orchestrator) HandleIngest(ctx context.Context, req *logengine.IngestRequest) (*logengine.IngestResponse, error) {
    if req.Entry == nil {
        return nil, status.Error(codes.InvalidArgument, "entry is required")
    }
    if req.Entry.Service == "" {
        return nil, status.Error(codes.InvalidArgument, "entry.service is required")
    }
    if req.Entry.Message == "" {
        return nil, status.Error(codes.InvalidArgument, "entry.message is required")
    }

    // Local mode: bypass routing entirely.
    if o.totalShards == 0 {
        return o.writeLocal(ctx, req.Entry, "")
    }

    shardID := ShardID(req.Entry.Service, o.totalShards)
    primaryID, replicaID := o.stateReader.ShardOwners(shardID)

    if primaryID == "" {
        return nil, status.Errorf(codes.Unavailable, "no primary for shard %d", shardID)
    }

    if primaryID == o.nodeID {
        return o.writeLocal(ctx, req.Entry, replicaID)
    }
    return o.forward(ctx, req, primaryID)
}

func (o *Orchestrator) writeLocal(ctx context.Context, pb *logengine.LogEntry, replicaNodeID string) (*logengine.IngestResponse, error) {
    entry := ProtoToEntry(pb)
    entry.ReceivedAt = time.Now().UnixNano()
    if entry.ID == "" {
        entry.ID = GenerateID()
    }

    segPath, err := o.manager.AppendWithPath(entry)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "append failed: %v", err)
    }
    o.idx.Add(entry, segPath)

    // Enqueue async replication if a replica is known and a replicator is wired.
    if replicaNodeID != "" && o.replicator != nil && o.stateReader != nil {
        replicaAddr := o.stateReader.NodeAddress(replicaNodeID)
        if replicaAddr != "" {
            shardID := 0
            if o.totalShards > 0 {
                shardID = ShardID(entry.Service, o.totalShards)
            }
            o.replicator.Enqueue(entry, shardID, replicaAddr)
        }
    }

    return &logengine.IngestResponse{Id: entry.ID, Ok: true}, nil
}

func (o *Orchestrator) forward(ctx context.Context, req *logengine.IngestRequest, primaryID string) (*logengine.IngestResponse, error) {
    addr := o.stateReader.NodeAddress(primaryID)
    if addr == "" {
        return nil, status.Errorf(codes.Unavailable, "primary node %q address unknown", primaryID)
    }
    client, err := o.getOrCreateClient(addr)
    if err != nil {
        return nil, status.Errorf(codes.Unavailable, "connect to primary %s: %v", addr, err)
    }
    return client.Ingest(ctx, req)
}

func (o *Orchestrator) getOrCreateClient(addr string) (logengine.IngestServiceClient, error) {
    o.mu.Lock()
    defer o.mu.Unlock()
    if c, ok := o.clients[addr]; ok {
        return c, nil
    }
    conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return nil, fmt.Errorf("grpc dial %s: %w", addr, err)
    }
    o.clients[addr] = logengine.NewIngestServiceClient(conn)
    return o.clients[addr], nil
}
```

- [ ] **Step 3: Verify build**

```bash
go build ./internal/ingest/...
```

Expected: exits 0.

---

## Task 8: Ingest Server Refactor

**Files:**
- Modify: `internal/ingest/server.go`
- Modify: `internal/ingest/server_test.go`
- Modify: `test/integration/ingest_test.go`

**Note:** `protoToEntry`, `generateID`, and their logic move to `convert.go` (Task 7). `server.go` becomes a thin gRPC adapter plus the `ReplicateEntry` and `FetchShardEntries` handlers. A new `NewLocalServer` constructor preserves the single-node interface for tests that don't need routing.

- [ ] **Step 1: Rewrite `server.go`**

```go
// internal/ingest/server.go
package ingest

import (
    "context"
    "sort"

    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"

    logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
    "github.com/Weilei424/distributed-log-query-engine/internal/index"
    "github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

// Server implements the gRPC IngestServiceServer interface.
// Client-facing RPCs (Ingest, IngestBatch) delegate to the Orchestrator.
// Internal RPCs (ReplicateEntry, FetchShardEntries) bypass routing and
// operate directly on local storage.
type Server struct {
    logengine.UnimplementedIngestServiceServer
    orchestrator *Orchestrator
    nodeID       string
    totalShards  int
    manager      *storage.Manager
    idx          *index.Index
}

// NewServer creates a Server backed by the given orchestrator.
// Use for cluster-aware nodes.
func NewServer(orchestrator *Orchestrator, nodeID string, totalShards int, manager *storage.Manager, idx *index.Index) *Server {
    return &Server{
        orchestrator: orchestrator,
        nodeID:       nodeID,
        totalShards:  totalShards,
        manager:      manager,
        idx:          idx,
    }
}

// NewLocalServer creates a Server for single-node use without cluster routing.
// All writes go directly to local storage. Used by tests and no-coordinator mode.
func NewLocalServer(manager *storage.Manager, idx *index.Index) *Server {
    orch := newLocalOrchestrator(manager, idx)
    return &Server{
        orchestrator: orch,
        nodeID:       "local",
        totalShards:  0,
        manager:      manager,
        idx:          idx,
    }
}

// Ingest delegates to the orchestrator for routing and local write.
func (s *Server) Ingest(ctx context.Context, req *logengine.IngestRequest) (*logengine.IngestResponse, error) {
    return s.orchestrator.HandleIngest(ctx, req)
}

// IngestBatch writes multiple log entries via the orchestrator.
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

// ReplicateEntry writes an entry directly to local storage, bypassing routing.
// Called by the primary's Replicator to deliver an async copy to this replica.
func (s *Server) ReplicateEntry(ctx context.Context, req *logengine.ReplicateEntryRequest) (*logengine.ReplicateEntryResponse, error) {
    if req.Entry == nil {
        return nil, status.Error(codes.InvalidArgument, "entry is required")
    }
    // Defensive check: the computed shard must match the claimed shard_id.
    if s.totalShards > 0 {
        computed := ShardID(req.Entry.Service, s.totalShards)
        if computed != int(req.ShardId) {
            return nil, status.Errorf(codes.FailedPrecondition,
                "shard mismatch: computed %d for service %q, request claims %d",
                computed, req.Entry.Service, req.ShardId)
        }
    }
    entry := ProtoToEntry(req.Entry)
    segPath, err := s.manager.AppendWithPath(entry)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "replicate append failed: %v", err)
    }
    s.idx.Add(entry, segPath)
    return &logengine.ReplicateEntryResponse{Ok: true}, nil
}

// FetchShardEntries returns entries for a shard with received_at > since_unix_ns.
// Called by a replica node during catch-up on restart.
func (s *Server) FetchShardEntries(ctx context.Context, req *logengine.FetchShardEntriesRequest) (*logengine.FetchShardEntriesResponse, error) {
    all, err := s.manager.ReadSegments(s.manager.SegmentPaths())
    if err != nil {
        return nil, status.Errorf(codes.Internal, "read segments: %v", err)
    }

    var result []*logengine.LogEntry
    for _, e := range all {
        if s.totalShards > 0 && ShardID(e.Service, s.totalShards) != int(req.ShardId) {
            continue
        }
        if e.ReceivedAt <= req.SinceUnixNs {
            continue
        }
        result = append(result, EntryToProto(e))
    }

    sort.Slice(result, func(i, j int) bool {
        return result[i].ReceivedAt < result[j].ReceivedAt
    })

    return &logengine.FetchShardEntriesResponse{Entries: result}, nil
}
```

- [ ] **Step 2: Update `server_test.go` — replace `NewServer` with `NewLocalServer`**

Open `internal/ingest/server_test.go`. Find every call to `ingest.NewServer(` or `NewServer(` and replace with `ingest.NewLocalServer(` / `NewLocalServer(`. Remove the `index.Index` parameter if it was separate (it is still the second argument to `NewLocalServer`).

The new call signature is:
```go
// Before:
srv := ingest.NewServer(m, index.NewIndex())
// After:
srv := ingest.NewLocalServer(m, index.NewIndex())
```

- [ ] **Step 3: Update `test/integration/ingest_test.go` — same replacement**

```go
// Before:
srv := ingest.NewServer(m, index.NewIndex())
// After:
srv := ingest.NewLocalServer(m, index.NewIndex())
```

- [ ] **Step 4: Build and run all ingest tests**

```bash
go test ./internal/ingest/... -v
go test ./test/integration/... -run TestIngest -v
```

Expected: all pass.

- [ ] **Step 5: Run full suite**

```bash
make test
```

Expected: all pass.

---

## Task 9: cmd/node — Wire Orchestrator, State Cache, Replicator, and Catch-up

**Files:**
- Modify: `cmd/node/main.go`

- [ ] **Step 1: Add `TOTAL_SHARDS` env var and rewrite `main.go`**

Replace the full content of `cmd/node/main.go` with the following:

```go
// cmd/node/main.go
package main

import (
    "context"
    "fmt"
    "log"
    "net"
    "os"
    "os/signal"
    "strconv"
    "syscall"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
    "github.com/Weilei424/distributed-log-query-engine/internal/cluster"
    "github.com/Weilei424/distributed-log-query-engine/internal/index"
    "github.com/Weilei424/distributed-log-query-engine/internal/ingest"
    "github.com/Weilei424/distributed-log-query-engine/internal/query"
    "github.com/Weilei424/distributed-log-query-engine/internal/replication"
    "github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func main() {
    nodeID := envOrDefault("NODE_ID", "node-local")
    dataDir := envOrDefault("DATA_DIR", "./data")
    grpcAddr := envOrDefault("GRPC_ADDR", ":50051")
    advertisedAddr := envOrDefault("NODE_GRPC_ADDR", grpcAddr)
    maxSegBytes := envInt64OrDefault("MAX_SEGMENT_BYTES", 64*1024*1024)
    coordinatorAddrs := envOrDefault("COORDINATOR_ADDRS", "")
    heartbeatInterval := time.Duration(envIntOrDefault("HEARTBEAT_INTERVAL_SECONDS", 5)) * time.Second
    totalShards := envIntOrDefault("TOTAL_SHARDS", 4)

    manager, err := storage.NewManager(dataDir, maxSegBytes)
    if err != nil {
        log.Fatalf("storage.NewManager: %v", err)
    }

    idx := index.NewIndex()
    if err := idx.RebuildFromSegments(manager.SegmentPaths(), storage.ReadSegment); err != nil {
        log.Fatalf("index rebuild: %v", err)
    }

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    var ingestSrv *ingest.Server
    var querySrv *query.QueryServer

    if coordinatorAddrs != "" {
        addrs := cluster.ParseAddrs(coordinatorAddrs)
        clusterClient, err := cluster.NewClusterClient(addrs, nodeID)
        if err != nil {
            log.Printf("cluster client init: %v (starting in local mode)", err)
            ingestSrv = ingest.NewLocalServer(manager, idx)
        } else {
            defer clusterClient.Close()

            // Register with the coordinator.
            regCtx, regCancel := context.WithTimeout(ctx, 30*time.Second)
            shards, err := clusterClient.Register(regCtx, advertisedAddr)
            regCancel()
            if err != nil {
                log.Printf("cluster register: %v (starting in degraded mode; will retry)", err)
                // Retry in background.
                go func() {
                    for {
                        select {
                        case <-ctx.Done():
                            return
                        case <-time.After(heartbeatInterval):
                        }
                        regCtx, regCancel := context.WithTimeout(ctx, 30*time.Second)
                        shards, err := clusterClient.Register(regCtx, advertisedAddr)
                        regCancel()
                        if err != nil {
                            log.Printf("cluster register retry: %v", err)
                            continue
                        }
                        fmt.Printf("registered with coordinator (retry): shards=%v\n", shards)
                        sender := cluster.NewHeartbeatSender(clusterClient, heartbeatInterval)
                        sender.Run(ctx)
                        return
                    }
                }()
                ingestSrv = ingest.NewLocalServer(manager, idx)
            } else {
                fmt.Printf("registered with coordinator: shards=%v\n", shards)

                // Start state cache (initial refresh before accepting traffic).
                stateCache := cluster.NewStateCache(clusterClient, 5*time.Second)
                stateCache.Refresh(ctx)
                go stateCache.Run(ctx)

                // Run catch-up for shards this node owns as replica.
                runCatchUp(ctx, nodeID, totalShards, clusterClient, stateCache, manager, idx)

                // Build orchestrator.
                repl := replication.NewReplicator(totalShards)
                defer repl.Stop()
                orch := ingest.NewOrchestrator(nodeID, totalShards, stateCache, manager, idx, repl)
                ingestSrv = ingest.NewServer(orch, nodeID, totalShards, manager, idx)

                // Start heartbeat.
                sender := cluster.NewHeartbeatSender(clusterClient, heartbeatInterval)
                go sender.Run(ctx)
            }
        }
    } else {
        ingestSrv = ingest.NewLocalServer(manager, idx)
    }

    querySrv = query.NewQueryServer(query.NewLocalExecutor(idx, manager))

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
    cancel()
    grpcSrv.GracefulStop()
    if err := manager.Close(); err != nil {
        log.Printf("manager close: %v", err)
    }
    fmt.Println("node stopped")
}

// runCatchUp fetches entries from the primary for each shard this node replicates.
// Runs synchronously before the server starts accepting traffic.
// Skips silently if the primary is unreachable.
func runCatchUp(ctx context.Context, nodeID string, totalShards int, clusterClient *cluster.ClusterClient, stateReader *cluster.StateCache, manager *storage.Manager, idx *index.Index) {
    state, err := clusterClient.GetClusterState(ctx)
    if err != nil {
        log.Printf("catch-up: get cluster state failed: %v (skipping)", err)
        return
    }

    for shardID, sr := range state.Shards {
        if sr.ReplicaNode != nodeID {
            continue
        }
        primaryAddr := ""
        if n, ok := state.Nodes[sr.PrimaryNode]; ok {
            primaryAddr = n.Address
        }
        if primaryAddr == "" {
            log.Printf("catch-up: shard %d primary address unknown, skipping", shardID)
            continue
        }

        sinceNs := latestReceivedAtForShard(shardID, totalShards, manager)

        conn, err := grpc.NewClient(primaryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err != nil {
            log.Printf("catch-up: dial primary %s for shard %d: %v (skipping)", primaryAddr, shardID, err)
            continue
        }

        fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
        resp, err := logengine.NewIngestServiceClient(conn).FetchShardEntries(fetchCtx, &logengine.FetchShardEntriesRequest{
            ShardId:     int32(shardID),
            SinceUnixNs: sinceNs,
        })
        cancel()
        conn.Close()

        if err != nil {
            log.Printf("catch-up: FetchShardEntries shard %d from %s: %v (skipping)", shardID, primaryAddr, err)
            continue
        }

        for _, pb := range resp.Entries {
            e := ingest.ProtoToEntry(pb)
            segPath, err := manager.AppendWithPath(e)
            if err != nil {
                log.Printf("catch-up: append entry %s: %v", e.ID, err)
                continue
            }
            idx.Add(e, segPath)
        }
        log.Printf("catch-up: shard %d caught up %d entries from %s", shardID, len(resp.Entries), primaryAddr)
    }
}

// latestReceivedAtForShard returns the largest received_at nanosecond timestamp
// among all local entries that belong to the given shard. Returns 0 if none.
func latestReceivedAtForShard(shardID, totalShards int, manager *storage.Manager) int64 {
    entries, err := manager.ReadSegments(manager.SegmentPaths())
    if err != nil {
        return 0
    }
    var latest int64
    for _, e := range entries {
        if totalShards > 0 && ingest.ShardID(e.Service, totalShards) != shardID {
            continue
        }
        if e.ReceivedAt > latest {
            latest = e.ReceivedAt
        }
    }
    return latest
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

func envIntOrDefault(key string, def int) int {
    if v := os.Getenv(key); v != "" {
        if n, err := strconv.Atoi(v); err == nil {
            return n
        }
    }
    return def
}
```

- [ ] **Step 2: Build and lint**

```bash
make build
make lint
```

Expected: both exit 0.

- [ ] **Step 3: Run full test suite**

```bash
make test
```

Expected: all tests pass.

---

## Task 10: Integration Tests

**Files:**
- Create: `test/integration/phase5_node_test.go` — shared `testNode` helper
- Create: `test/integration/phase5_routing_test.go`
- Create: `test/integration/phase5_replication_test.go`
- Create: `test/integration/phase5_failure_test.go`
- Create: `test/integration/phase5_catchup_test.go`

**Note:** All Phase 5 integration tests live in `package integration_test` and use the existing `startTestCoordinator` and `waitForLeader` helpers from `cluster_test.go`.

- [ ] **Step 1: Create `phase5_node_test.go` — shared node helper**

```go
// test/integration/phase5_node_test.go
package integration_test

import (
    "context"
    "net"
    "testing"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
    "github.com/Weilei424/distributed-log-query-engine/internal/cluster"
    "github.com/Weilei424/distributed-log-query-engine/internal/index"
    "github.com/Weilei424/distributed-log-query-engine/internal/ingest"
    "github.com/Weilei424/distributed-log-query-engine/internal/replication"
    "github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

// testNode is a self-contained in-process storage node for Phase 5 integration tests.
type testNode struct {
    addr        string
    nodeID      string
    manager     *storage.Manager
    idx         *index.Index
    replicator  *replication.Replicator
    stateCache  *cluster.StateCache
    clusterClient *cluster.ClusterClient
    grpcSrv     *grpc.Server
    cancel      context.CancelFunc
}

func (tn *testNode) cleanup() {
    tn.grpcSrv.GracefulStop()
    tn.replicator.Stop()
    tn.clusterClient.Close()
    tn.manager.Close()
    tn.cancel()
}

// ingestClient returns a gRPC client connected to this node's ingest endpoint.
func (tn *testNode) ingestClient(t *testing.T) logengine.IngestServiceClient {
    t.Helper()
    conn, err := grpc.NewClient(tn.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        t.Fatalf("dial node %s: %v", tn.nodeID, err)
    }
    t.Cleanup(func() { conn.Close() })
    return logengine.NewIngestServiceClient(conn)
}

// startPhase5Node registers a node with the coordinator and starts its gRPC server.
// totalShards must match the coordinator's FSM totalShards.
func startPhase5Node(t *testing.T, nodeID string, coordAddr string, totalShards int) *testNode {
    t.Helper()

    dir := t.TempDir()
    m, err := storage.NewManager(dir, 64*1024*1024)
    if err != nil {
        t.Fatalf("NewManager %s: %v", nodeID, err)
    }
    idx := index.NewIndex()

    ctx, cancel := context.WithCancel(context.Background())

    // Cluster client for registration, heartbeat, and state polling.
    clusterClient, err := cluster.NewClusterClient([]string{coordAddr}, nodeID)
    if err != nil {
        cancel()
        t.Fatalf("NewClusterClient %s: %v", nodeID, err)
    }

    // Listen on a random port before registering so we have an address.
    lis, err := net.Listen("tcp", ":0")
    if err != nil {
        cancel()
        t.Fatalf("listen %s: %v", nodeID, err)
    }
    addr := lis.Addr().String()

    // Register with coordinator.
    regCtx, regCancel := context.WithTimeout(ctx, 10*time.Second)
    shards, err := clusterClient.Register(regCtx, addr)
    regCancel()
    if err != nil {
        cancel()
        t.Fatalf("Register %s: %v", nodeID, err)
    }
    t.Logf("node %s registered: shards=%v addr=%s", nodeID, shards, addr)

    // State cache — initial refresh so orchestrator has routing data.
    stateCache := cluster.NewStateCache(clusterClient, 100*time.Millisecond)
    stateCache.Refresh(ctx)
    go stateCache.Run(ctx)

    repl := replication.NewReplicator(totalShards)
    orch := ingest.NewOrchestrator(nodeID, totalShards, stateCache, m, idx, repl)
    srv := ingest.NewServer(orch, nodeID, totalShards, m, idx)

    grpcSrv := grpc.NewServer()
    logengine.RegisterIngestServiceServer(grpcSrv, srv)
    go grpcSrv.Serve(lis) //nolint:errcheck

    // Allow state cache to pick up both nodes once all nodes are registered.
    // Callers should sleep after starting all nodes if needed.

    return &testNode{
        addr:          addr,
        nodeID:        nodeID,
        manager:       m,
        idx:           idx,
        replicator:    repl,
        stateCache:    stateCache,
        clusterClient: clusterClient,
        grpcSrv:       grpcSrv,
        cancel:        cancel,
    }
}

// waitForEntry polls entries on a node's manager until it finds one matching predicate or times out.
func waitForEntry(t *testing.T, m *storage.Manager, predicate func(*storage.Manager) bool, timeout time.Duration) {
    t.Helper()
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        if predicate(m) {
            return
        }
        time.Sleep(20 * time.Millisecond)
    }
    t.Fatalf("entry not found within %s", timeout)
}

// entryCountOnNode returns the total number of log entries in a node's storage.
func entryCountOnNode(t *testing.T, m *storage.Manager) int {
    t.Helper()
    entries, err := m.ReadSegments(m.SegmentPaths())
    if err != nil {
        t.Fatalf("ReadSegments: %v", err)
    }
    return len(entries)
}

// entriesWithService returns entries on a node whose service field matches.
func entriesWithService(t *testing.T, m *storage.Manager, service string) int {
    t.Helper()
    all, err := m.ReadSegments(m.SegmentPaths())
    if err != nil {
        t.Fatalf("ReadSegments: %v", err)
    }
    count := 0
    for _, e := range all {
        if e.Service == service {
            count++
        }
    }
    return count
}
```

- [ ] **Step 2: Create `phase5_routing_test.go`**

```go
// test/integration/phase5_routing_test.go
package integration_test

import (
    "context"
    "testing"
    "time"

    logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
    "github.com/Weilei424/distributed-log-query-engine/internal/ingest"
)

// TestPhase5_RoutingForwardsToCorrectPrimary sends a write to a non-owning node
// and verifies the entry appears on the primary node, not the receiving node.
func TestPhase5_RoutingForwardsToCorrectPrimary(t *testing.T) {
    const totalShards = 4
    coord := startTestCoordinator(t, totalShards)
    defer coord.cleanup()

    nodeA := startPhase5Node(t, "node-a", coord.addr, totalShards)
    defer nodeA.cleanup()
    nodeB := startPhase5Node(t, "node-b", coord.addr, totalShards)
    defer nodeB.cleanup()

    // Allow both state caches to refresh after both nodes are registered.
    time.Sleep(300 * time.Millisecond)

    // Determine which service hashes to a shard owned by node-b (not node-a).
    // With 4 shards and 2 nodes: node-a owns even shards, node-b owns odd shards
    // (round-robin: shard 0→a, 1→b, 2→a, 3→b).
    state := coord.fsm.State()
    var serviceThatRoutesToB string
    for _, svc := range []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"} {
        sid := ingest.ShardID(svc, totalShards)
        if sr, ok := state.Shards[sid]; ok && sr.PrimaryNode == "node-b" {
            serviceThatRoutesToB = svc
            break
        }
    }
    if serviceThatRoutesToB == "" {
        t.Skip("could not find a test service that routes to node-b; shard distribution may differ")
    }
    t.Logf("using service %q (shard %d → node-b)", serviceThatRoutesToB, ingest.ShardID(serviceThatRoutesToB, totalShards))

    // Send write to node-a (which does NOT own this service's shard).
    clientA := nodeA.ingestClient(t)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    _, err := clientA.Ingest(ctx, &logengine.IngestRequest{
        Entry: &logengine.LogEntry{
            Service: serviceThatRoutesToB,
            Message: "routed write",
            Level:   "INFO",
        },
    })
    if err != nil {
        t.Fatalf("Ingest via node-a: %v", err)
    }

    // Entry must appear on node-b (the primary).
    waitForEntry(t, nodeB.manager, func(m *storage.Manager) bool {
        return entriesWithService(t, m, serviceThatRoutesToB) >= 1
    }, 3*time.Second)

    // Entry must NOT appear on node-a (the forwarder).
    time.Sleep(100 * time.Millisecond)
    if n := entriesWithService(t, nodeA.manager, serviceThatRoutesToB); n != 0 {
        t.Errorf("entry unexpectedly stored on node-a (forwarding node): count=%d", n)
    }
}
```

- [ ] **Step 3: Create `phase5_replication_test.go`**

```go
// test/integration/phase5_replication_test.go
package integration_test

import (
    "context"
    "testing"
    "time"

    logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
    "github.com/Weilei424/distributed-log-query-engine/internal/ingest"
)

// TestPhase5_AsyncReplicationToReplica ingests to the primary and verifies
// the entry appears on the replica node asynchronously.
func TestPhase5_AsyncReplicationToReplica(t *testing.T) {
    const totalShards = 4
    coord := startTestCoordinator(t, totalShards)
    defer coord.cleanup()

    nodeA := startPhase5Node(t, "node-a", coord.addr, totalShards)
    defer nodeA.cleanup()
    nodeB := startPhase5Node(t, "node-b", coord.addr, totalShards)
    defer nodeB.cleanup()

    time.Sleep(300 * time.Millisecond)

    // Find a service whose shard is owned by node-a as primary.
    state := coord.fsm.State()
    var svcForNodeA string
    for _, svc := range []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"} {
        sid := ingest.ShardID(svc, totalShards)
        if sr, ok := state.Shards[sid]; ok && sr.PrimaryNode == "node-a" {
            svcForNodeA = svc
            break
        }
    }
    if svcForNodeA == "" {
        t.Skip("could not find a test service that routes to node-a")
    }
    t.Logf("using service %q (shard %d → node-a primary)", svcForNodeA, ingest.ShardID(svcForNodeA, totalShards))

    // Ingest directly to node-a (the primary for this service).
    clientA := nodeA.ingestClient(t)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    _, err := clientA.Ingest(ctx, &logengine.IngestRequest{
        Entry: &logengine.LogEntry{
            Service: svcForNodeA,
            Message: "replicated write",
            Level:   "INFO",
        },
    })
    if err != nil {
        t.Fatalf("Ingest to primary: %v", err)
    }

    // Entry must appear on node-a (primary) immediately.
    if n := entriesWithService(t, nodeA.manager, svcForNodeA); n != 1 {
        t.Errorf("primary node-a: expected 1 entry, got %d", n)
    }

    // Entry must appear on node-b (replica) asynchronously within 2 seconds.
    waitForEntry(t, nodeB.manager, func(m *storage.Manager) bool {
        return entriesWithService(t, m, svcForNodeA) >= 1
    }, 2*time.Second)
}
```

- [ ] **Step 4: Create `phase5_failure_test.go`**

```go
// test/integration/phase5_failure_test.go
package integration_test

import (
    "context"
    "testing"
    "time"

    logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
    "github.com/Weilei424/distributed-log-query-engine/internal/ingest"
    "github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

// TestPhase5_PrimaryFailure_ReplicaStillServesLogs stops the primary node
// and verifies the replica still has the data.
func TestPhase5_PrimaryFailure_ReplicaStillServesLogs(t *testing.T) {
    const totalShards = 4
    coord := startTestCoordinator(t, totalShards)
    defer coord.cleanup()

    nodeA := startPhase5Node(t, "node-a", coord.addr, totalShards)
    // nodeA cleanup called explicitly below after stop.
    nodeB := startPhase5Node(t, "node-b", coord.addr, totalShards)
    defer nodeB.cleanup()

    time.Sleep(300 * time.Millisecond)

    // Find a service whose shard is primary on node-a.
    state := coord.fsm.State()
    var svcForNodeA string
    for _, svc := range []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"} {
        sid := ingest.ShardID(svc, totalShards)
        if sr, ok := state.Shards[sid]; ok && sr.PrimaryNode == "node-a" {
            svcForNodeA = svc
            break
        }
    }
    if svcForNodeA == "" {
        t.Skip("could not find a service routed to node-a")
    }

    // Ingest to node-a (primary).
    clientA := nodeA.ingestClient(t)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    _, err := clientA.Ingest(ctx, &logengine.IngestRequest{
        Entry: &logengine.LogEntry{Service: svcForNodeA, Message: "pre-failure write", Level: "INFO"},
    })
    if err != nil {
        t.Fatalf("Ingest to primary: %v", err)
    }

    // Wait for async replication to node-b.
    waitForEntry(t, nodeB.manager, func(m *storage.Manager) bool {
        return entriesWithService(t, m, svcForNodeA) >= 1
    }, 2*time.Second)

    // Stop node-a (primary failure).
    nodeA.cleanup()

    // node-b (replica) must still have the data.
    count := entriesWithService(t, nodeB.manager, svcForNodeA)
    if count == 0 {
        t.Errorf("replica node-b has no entries for service %q after primary stopped", svcForNodeA)
    } else {
        t.Logf("replica node-b serves %d entries for %q after primary failure", count, svcForNodeA)
    }

    // Verify entries readable from node-b's storage manager.
    all, err := nodeB.manager.ReadSegments(nodeB.manager.SegmentPaths())
    if err != nil {
        t.Fatalf("ReadSegments on replica: %v", err)
    }
    found := false
    for _, e := range all {
        if e.Service == svcForNodeA && e.Message == "pre-failure write" {
            found = true
            break
        }
    }
    if !found {
        t.Errorf("specific pre-failure write not found on replica")
    }
}
```

- [ ] **Step 5: Create `phase5_catchup_test.go`**

```go
// test/integration/phase5_catchup_test.go
package integration_test

import (
    "context"
    "net"
    "testing"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
    "github.com/Weilei424/distributed-log-query-engine/internal/cluster"
    "github.com/Weilei424/distributed-log-query-engine/internal/index"
    "github.com/Weilei424/distributed-log-query-engine/internal/ingest"
    "github.com/Weilei424/distributed-log-query-engine/internal/replication"
    "github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func TestPhase5_CatchUp_ReplicaFetchesMissedEntries(t *testing.T) {
    const totalShards = 4
    coord := startTestCoordinator(t, totalShards)
    defer coord.cleanup()

    // Start primary (node-a).
    nodeA := startPhase5Node(t, "node-a", coord.addr, totalShards)
    defer nodeA.cleanup()
    // Start replica (node-b) — just to get shard assignments recorded.
    nodeB := startPhase5Node(t, "node-b", coord.addr, totalShards)

    time.Sleep(300 * time.Millisecond)

    state := coord.fsm.State()
    var svcForNodeA string
    for _, svc := range []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"} {
        sid := ingest.ShardID(svc, totalShards)
        if sr, ok := state.Shards[sid]; ok && sr.PrimaryNode == "node-a" {
            svcForNodeA = svc
            break
        }
    }
    if svcForNodeA == "" {
        nodeB.cleanup()
        t.Skip("could not find a service routed to node-a")
    }

    // Stop replica immediately after shard assignment is confirmed.
    nodeB.cleanup()

    // Ingest 3 entries to primary while replica is down.
    clientA := nodeA.ingestClient(t)
    for i := 0; i < 3; i++ {
        _, err := clientA.Ingest(context.Background(), &logengine.IngestRequest{
            Entry: &logengine.LogEntry{Service: svcForNodeA, Message: "missed-entry", Level: "INFO"},
        })
        if err != nil {
            t.Fatalf("Ingest %d: %v", i, err)
        }
    }

    // Simulate restart: fresh storage dir, re-register, run catch-up.
    dir2 := t.TempDir()
    m2, err := storage.NewManager(dir2, 64*1024*1024)
    if err != nil {
        t.Fatalf("NewManager restart: %v", err)
    }
    defer m2.Close()
    idx2 := index.NewIndex()

    clusterClient2, err := cluster.NewClusterClient([]string{coord.addr}, "node-b")
    if err != nil {
        t.Fatalf("NewClusterClient: %v", err)
    }
    defer clusterClient2.Close()

    lis2, err := net.Listen("tcp", ":0")
    if err != nil {
        t.Fatalf("listen: %v", err)
    }
    addr2 := lis2.Addr().String()

    regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
    _, err = clusterClient2.Register(regCtx, addr2)
    regCancel()
    if err != nil {
        t.Fatalf("Register restart: %v", err)
    }

    // Fetch missing entries from primary.
    shardID := ingest.ShardID(svcForNodeA, totalShards)
    conn, err := grpc.NewClient(nodeA.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        t.Fatalf("dial primary: %v", err)
    }
    defer conn.Close()

    fetchCtx, fetchCancel := context.WithTimeout(context.Background(), 10*time.Second)
    resp, err := logengine.NewIngestServiceClient(conn).FetchShardEntries(fetchCtx, &logengine.FetchShardEntriesRequest{
        ShardId:     int32(shardID),
        SinceUnixNs: 0,
    })
    fetchCancel()
    if err != nil {
        t.Fatalf("FetchShardEntries: %v", err)
    }
    if len(resp.Entries) != 3 {
        t.Errorf("expected 3 entries from catch-up, got %d", len(resp.Entries))
    }

    // Apply caught-up entries to replica storage.
    for _, pb := range resp.Entries {
        e := ingest.ProtoToEntry(pb)
        segPath, err := m2.AppendWithPath(e)
        if err != nil {
            t.Fatalf("catch-up append: %v", err)
        }
        idx2.Add(e, segPath)
    }

    total := entryCountOnNode(t, m2)
    if total != 3 {
        t.Errorf("after catch-up: expected 3 entries on replica, got %d", total)
    }

    // Bring replica back up and verify it can receive ReplicateEntry calls.
    repl2 := replication.NewReplicator(totalShards)
    defer repl2.Stop()
    ctx2, cancel2 := context.WithCancel(context.Background())
    defer cancel2()

    sc2 := cluster.NewStateCache(clusterClient2, 100*time.Millisecond)
    sc2.Refresh(ctx2)
    go sc2.Run(ctx2)

    orch2 := ingest.NewOrchestrator("node-b", totalShards, sc2, m2, idx2, repl2)
    srv2 := ingest.NewServer(orch2, "node-b", totalShards, m2, idx2)
    grpcSrv2 := grpc.NewServer()
    logengine.RegisterIngestServiceServer(grpcSrv2, srv2)
    go grpcSrv2.Serve(lis2) //nolint:errcheck
    defer grpcSrv2.GracefulStop()

    // Send one more ReplicateEntry to confirm the restarted replica can accept replicas.
    replicaConn, err := grpc.NewClient(addr2, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        t.Fatalf("dial restarted replica: %v", err)
    }
    defer replicaConn.Close()

    _, err = logengine.NewIngestServiceClient(replicaConn).ReplicateEntry(context.Background(), &logengine.ReplicateEntryRequest{
        Entry:   &logengine.LogEntry{Id: "post-catchup", Service: svcForNodeA, Message: "post-restart", Level: "INFO"},
        ShardId: int32(shardID),
    })
    if err != nil {
        t.Fatalf("ReplicateEntry on restarted replica: %v", err)
    }

    if total2 := entryCountOnNode(t, m2); total2 != 4 {
        t.Errorf("after ReplicateEntry: expected 4 entries, got %d", total2)
    }
}
```

- [ ] **Step 6: Run Phase 5 integration tests**

```bash
go test ./test/integration/... -run TestPhase5 -v -timeout 60s
```

Expected: all four `TestPhase5_*` tests pass.

- [ ] **Step 7: Run full test suite**

```bash
make test
```

Expected: all tests pass.

---

## Task 11: Update BACKLOG.md

**Files:**
- Modify: `docs/planning/BACKLOG.md`

- [ ] **Step 1: Replace Phase 5 checklist with completed items**

Replace the Phase 5 section in `docs/planning/BACKLOG.md` with:

```markdown
## Phase 5 — Distributed Ingestion, Partitioning, and Replication

**Plan:** `docs/superpowers/plans/2026-04-17-phase5-distributed-ingest-replication.md`
**Spec:** `docs/superpowers/specs/2026-04-17-phase5-distributed-ingest-replication-design.md`

### Status: Complete

- [x] `internal/metadata/state.go` — add `ReplicaNode string` to `ShardRecord`
- [x] `internal/metadata/fsm.go` — replace greedy assignment with `rebalancePrimary()` + `assignReplicas()`; clear replica slots in `applyMarkUnhealthy`
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
- [x] Unit tests: `fsm_test.go` extended (two-node share, replica assignment, mark-unhealthy clears replica)
- [x] Integration test: routing forwards write to correct primary node
- [x] Integration test: async replication delivers copy to replica node
- [x] Integration test: primary failure leaves data available on replica
- [x] Integration test: restarted replica fetches missed entries via `FetchShardEntries`
- [x] `make test` passes
- [x] `make lint` passes
- [x] `make build` passes
```

- [ ] **Step 2: Add Phase 8 stretch goal for segment file transfer catch-up**

Add to the Phase 8 section:

```markdown
- [ ] Segment file transfer catch-up (Option C): transfer full closed segment files from primary to replica on restart, replacing entry-by-entry fetch for nodes down for extended periods
```

- [ ] **Step 3: Verify docs are consistent**

```bash
make build && make test
```

Expected: exits 0. Confirm `BACKLOG.md` reflects the completed state.

---

## Validation

After all tasks complete, verify:

```bash
make build   # exits 0
make test    # all tests pass including TestPhase5_*
make lint    # no lint errors
```
