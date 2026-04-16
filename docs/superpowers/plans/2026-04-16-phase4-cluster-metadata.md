# Phase 4 — Multi-Node Cluster Formation and Metadata Coordination

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the single-node system into a distributed cluster by adding a real Raft-backed coordinator binary that manages node registration, shard ownership, and liveness tracking.

**Architecture:** Three `cmd/coordinator` instances form a HashiCorp Raft cluster. The Raft FSM stores a node registry and a shard ownership map. Storage nodes (`cmd/node`) register with the coordinator cluster on startup and send periodic heartbeats. A liveness checker marks nodes unhealthy and removes their shard ownership after missed heartbeats.

**Tech Stack:** Go, HashiCorp Raft (`github.com/hashicorp/raft`), BoltDB-backed Raft log store (`github.com/hashicorp/raft-boltdb/v2`), gRPC, Protocol Buffers.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `go.mod` | Modify | Add hashicorp/raft and raft-boltdb/v2 |
| `proto/logengine/v1/cluster.proto` | Create | ClusterService RPC definitions |
| `internal/api/gen/logengine/v1/cluster.pb.go` | Generate | Proto-generated types |
| `internal/api/gen/logengine/v1/cluster_grpc.pb.go` | Generate | Proto-generated gRPC stubs |
| `internal/metadata/state.go` | Create | NodeRecord, ShardRecord, ClusterState types |
| `internal/metadata/fsm.go` | Create | Raft FSM: Apply, Snapshot, Restore, command structs |
| `internal/metadata/fsm_test.go` | Create | Unit tests: RegisterNode, MarkUnhealthy, heartbeat, snapshot/restore |
| `internal/metadata/server.go` | Create | gRPC ClusterService implementation |
| `internal/metadata/liveness.go` | Create | Liveness checker goroutine (leader only) |
| `internal/cluster/client.go` | Create | ClusterClient: multi-address registration, leader redirect |
| `internal/cluster/heartbeat.go` | Create | HeartbeatSender goroutine |
| `internal/cluster/heartbeat_test.go` | Create | Unit test: HeartbeatSender stops on context cancel |
| `cmd/coordinator/main.go` | Rewrite | Full coordinator binary: Raft bootstrap, gRPC, HTTP /status |
| `cmd/node/main.go` | Modify | Add cluster registration + heartbeat on startup |
| `test/integration/cluster_test.go` | Create | Integration: nodes register, appear in cluster state |
| `test/integration/rejoin_test.go` | Create | Integration: node restart rejoins with shard assignment |
| `test/integration/liveness_test.go` | Create | Integration: missed heartbeats → node marked unhealthy |
| `deployments/docker-compose/docker-compose.yml` | Modify | 3 coordinators + updated node env vars |
| `docs/planning/BACKLOG.md` | Modify | Mark Phase 4 items complete |

---

## Task 1: Add Raft dependencies

**Files:**
- Modify: `go.mod`

- [ ] **Step 1: Add dependencies**

```bash
cd /mnt/d/projects/distributed-log-query-engine
go get github.com/hashicorp/raft@v1.7.3
go get github.com/hashicorp/raft-boltdb/v2@v2.3.0
go mod tidy
```

Expected: `go.mod` now lists `github.com/hashicorp/raft v1.7.3` and `github.com/hashicorp/raft-boltdb/v2 v2.3.0` in the `require` block.

- [ ] **Step 2: Verify build still passes**

```bash
make build
```

Expected: exits 0.

---

## Task 2: Write cluster.proto and generate Go bindings

**Files:**
- Create: `proto/logengine/v1/cluster.proto`
- Generate: `internal/api/gen/logengine/v1/cluster.pb.go`
- Generate: `internal/api/gen/logengine/v1/cluster_grpc.pb.go`

- [ ] **Step 1: Create the proto file**

Create `proto/logengine/v1/cluster.proto`:

```protobuf
syntax = "proto3";

package logengine.v1;

option go_package = "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1;logengine";

// ClusterService manages node registration and cluster state.
service ClusterService {
  // RegisterNode is called by a storage node on startup to join the cluster.
  // Only the Raft leader processes this request.
  rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);

  // Heartbeat is called periodically by storage nodes to signal liveness.
  // Only the Raft leader processes this request.
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);

  // GetClusterState returns the current node registry and shard map.
  // Any coordinator can serve this request.
  rpc GetClusterState(GetClusterStateRequest) returns (GetClusterStateResponse);
}

message RegisterNodeRequest {
  string node_id = 1;
  string grpc_address = 2;
}

message RegisterNodeResponse {
  repeated int32 assigned_shards = 1;
}

message HeartbeatRequest {
  string node_id = 1;
}

message HeartbeatResponse {
  bool ok = 1;
}

message GetClusterStateRequest {}

message GetClusterStateResponse {
  repeated NodeInfo nodes = 1;
  repeated ShardInfo shards = 2;
}

message NodeInfo {
  string id = 1;
  string address = 2;
  repeated int32 shards = 3;
  string status = 4;
  int64 last_seen_unix_ns = 5;
}

message ShardInfo {
  int32 shard_id = 1;
  string primary_node = 2;
}
```

- [ ] **Step 2: Generate Go bindings**

```bash
cd /mnt/d/projects/distributed-log-query-engine
buf generate
```

Expected: `internal/api/gen/logengine/v1/cluster.pb.go` and `cluster_grpc.pb.go` created with no errors.

- [ ] **Step 3: Verify build**

```bash
make build
```

Expected: exits 0.

---

## Task 3: Write cluster state types

**Files:**
- Create: `internal/metadata/state.go`

- [ ] **Step 1: Create state.go**

Create `internal/metadata/state.go`:

```go
package metadata

// NodeStatus is the health state of a storage node.
type NodeStatus string

const (
	NodeHealthy   NodeStatus = "healthy"
	NodeUnhealthy NodeStatus = "unhealthy"
)

// NodeRecord holds metadata for a registered storage node.
type NodeRecord struct {
	ID       string
	Address  string     // advertised gRPC address
	Shards   []int
	Status   NodeStatus
	LastSeen int64 // unix nanoseconds
}

// ShardRecord holds the ownership mapping for a single shard.
type ShardRecord struct {
	ShardID     int
	PrimaryNode string // node ID; empty if unowned
}

// ClusterState is the full in-memory state managed by the Raft FSM.
type ClusterState struct {
	Nodes  map[string]NodeRecord
	Shards map[int]ShardRecord
}

// clone returns a deep copy of the cluster state.
func (cs ClusterState) clone() ClusterState {
	nodes := make(map[string]NodeRecord, len(cs.Nodes))
	for k, v := range cs.Nodes {
		shards := make([]int, len(v.Shards))
		copy(shards, v.Shards)
		v.Shards = shards
		nodes[k] = v
	}
	shards := make(map[int]ShardRecord, len(cs.Shards))
	for k, v := range cs.Shards {
		shards[k] = v
	}
	return ClusterState{Nodes: nodes, Shards: shards}
}
```

- [ ] **Step 2: Verify build**

```bash
make build
```

Expected: exits 0.

---

## Task 4: Write Raft FSM (TDD)

**Files:**
- Create: `internal/metadata/fsm.go`
- Create: `internal/metadata/fsm_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/metadata/fsm_test.go`:

```go
package metadata_test

import (
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
)

// applyCmd marshals a command and applies it directly to the FSM.
func applyCmd(t *testing.T, fsm *metadata.FSM, cmdType metadata.CommandType, payload interface{}) {
	t.Helper()
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	cmd := metadata.Command{Type: cmdType, Payload: json.RawMessage(payloadBytes)}
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	result := fsm.Apply(&raft.Log{Data: data})
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("FSM.Apply: %v", err)
	}
}

func TestFSM_RegisterNode_NewNode(t *testing.T) {
	fsm := metadata.NewFSM(4)
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{
		NodeID:  "node-1",
		Address: ":50051",
	})

	state := fsm.State()
	node, ok := state.Nodes["node-1"]
	if !ok {
		t.Fatal("node-1 not in state")
	}
	if node.Status != metadata.NodeHealthy {
		t.Errorf("expected healthy, got %s", node.Status)
	}
	if len(node.Shards) != 4 {
		t.Errorf("expected 4 shards, got %d", len(node.Shards))
	}
	for shardID, sr := range state.Shards {
		if sr.PrimaryNode != "node-1" {
			t.Errorf("shard %d: expected primary node-1, got %q", shardID, sr.PrimaryNode)
		}
	}
}

func TestFSM_RegisterNode_SecondNodeGetsNoShards(t *testing.T) {
	fsm := metadata.NewFSM(4)
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051"})
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-2", Address: ":50052"})

	state := fsm.State()
	// node-1 registered first and claimed all 4 shards; node-2 gets none
	if n := len(state.Nodes["node-2"].Shards); n != 0 {
		t.Errorf("node-2 expected 0 shards (greedy assignment), got %d", n)
	}
}

func TestFSM_UpdateHeartbeat(t *testing.T) {
	fsm := metadata.NewFSM(4)
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051"})

	before := fsm.State().Nodes["node-1"].LastSeen

	applyCmd(t, fsm, metadata.CmdUpdateHeartbeat, metadata.UpdateHeartbeatPayload{NodeID: "node-1"})

	after := fsm.State().Nodes["node-1"].LastSeen
	if after <= before {
		t.Errorf("expected LastSeen to increase: before=%d after=%d", before, after)
	}
}

func TestFSM_MarkUnhealthy_ClearsShards(t *testing.T) {
	fsm := metadata.NewFSM(4)
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051"})
	applyCmd(t, fsm, metadata.CmdMarkUnhealthy, metadata.MarkUnhealthyPayload{NodeID: "node-1"})

	state := fsm.State()
	node := state.Nodes["node-1"]
	if node.Status != metadata.NodeUnhealthy {
		t.Errorf("expected unhealthy, got %s", node.Status)
	}
	if len(node.Shards) != 0 {
		t.Errorf("expected 0 shards after MarkUnhealthy, got %d", len(node.Shards))
	}
	for shardID, sr := range state.Shards {
		if sr.PrimaryNode != "" {
			t.Errorf("shard %d: expected empty primary after MarkUnhealthy, got %q", shardID, sr.PrimaryNode)
		}
	}
}

func TestFSM_Rejoin_ClaimsUnownedShards(t *testing.T) {
	fsm := metadata.NewFSM(4)
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051"})
	applyCmd(t, fsm, metadata.CmdMarkUnhealthy, metadata.MarkUnhealthyPayload{NodeID: "node-1"})
	// Re-register after being marked unhealthy
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051"})

	state := fsm.State()
	node := state.Nodes["node-1"]
	if node.Status != metadata.NodeHealthy {
		t.Errorf("expected healthy after rejoin, got %s", node.Status)
	}
	if len(node.Shards) != 4 {
		t.Errorf("expected 4 shards after rejoin, got %d", len(node.Shards))
	}
}

func TestFSM_SnapshotRestore(t *testing.T) {
	fsm := metadata.NewFSM(4)
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051"})

	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	fsm2 := metadata.NewFSM(4)
	if err := fsm2.Restore(io.NopCloser(strings.NewReader(sink.buf.String()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	state := fsm2.State()
	if _, ok := state.Nodes["node-1"]; !ok {
		t.Fatal("node-1 missing after restore")
	}
}

// memSink implements raft.SnapshotSink for testing.
type memSink struct {
	buf strings.Builder
}

func (s *memSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *memSink) Close() error                { return nil }
func (s *memSink) ID() string                  { return "test-sink" }
func (s *memSink) Cancel() error               { return nil }
```

- [ ] **Step 2: Run tests — expect compile failure**

```bash
go test ./internal/metadata/... 2>&1 | head -20
```

Expected: compile error — `metadata.FSM`, `metadata.NewFSM`, etc. undefined.

- [ ] **Step 3: Write fsm.go**

Create `internal/metadata/fsm.go`:

```go
package metadata

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// CommandType identifies the type of a Raft log command.
type CommandType string

const (
	CmdRegisterNode    CommandType = "register_node"
	CmdUpdateHeartbeat CommandType = "update_heartbeat"
	CmdMarkUnhealthy   CommandType = "mark_unhealthy"
)

// Command is the envelope written to the Raft log.
type Command struct {
	Type    CommandType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// RegisterNodePayload is the payload for CmdRegisterNode.
type RegisterNodePayload struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

// UpdateHeartbeatPayload is the payload for CmdUpdateHeartbeat.
type UpdateHeartbeatPayload struct {
	NodeID string `json:"node_id"`
}

// MarkUnhealthyPayload is the payload for CmdMarkUnhealthy.
type MarkUnhealthyPayload struct {
	NodeID string `json:"node_id"`
}

// FSM is the Raft finite state machine managing cluster metadata.
type FSM struct {
	mu          sync.RWMutex
	state       ClusterState
	totalShards int
}

// NewFSM creates an FSM with all shards unowned.
func NewFSM(totalShards int) *FSM {
	shards := make(map[int]ShardRecord, totalShards)
	for i := 0; i < totalShards; i++ {
		shards[i] = ShardRecord{ShardID: i}
	}
	return &FSM{
		state: ClusterState{
			Nodes:  make(map[string]NodeRecord),
			Shards: shards,
		},
		totalShards: totalShards,
	}
}

// Apply implements raft.FSM. It dispatches to the appropriate handler.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CmdRegisterNode:
		var p RegisterNodePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return err
		}
		return f.applyRegisterNode(p)
	case CmdUpdateHeartbeat:
		var p UpdateHeartbeatPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return err
		}
		return f.applyUpdateHeartbeat(p)
	case CmdMarkUnhealthy:
		var p MarkUnhealthyPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return err
		}
		return f.applyMarkUnhealthy(p)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

func (f *FSM) applyRegisterNode(p RegisterNodePayload) error {
	existing, ok := f.state.Nodes[p.NodeID]
	if ok && existing.Status == NodeHealthy {
		// Already healthy: just refresh address and last seen.
		existing.Address = p.Address
		existing.LastSeen = time.Now().UnixNano()
		f.state.Nodes[p.NodeID] = existing
		return nil
	}

	// New node or rejoining after being marked unhealthy: claim all unowned shards.
	var unowned []int
	for id, sr := range f.state.Shards {
		if sr.PrimaryNode == "" {
			unowned = append(unowned, id)
		}
	}
	for _, shardID := range unowned {
		sr := f.state.Shards[shardID]
		sr.PrimaryNode = p.NodeID
		f.state.Shards[shardID] = sr
	}
	f.state.Nodes[p.NodeID] = NodeRecord{
		ID:       p.NodeID,
		Address:  p.Address,
		Shards:   unowned,
		Status:   NodeHealthy,
		LastSeen: time.Now().UnixNano(),
	}
	return nil
}

func (f *FSM) applyUpdateHeartbeat(p UpdateHeartbeatPayload) error {
	node, ok := f.state.Nodes[p.NodeID]
	if !ok {
		return fmt.Errorf("node not found: %s", p.NodeID)
	}
	node.LastSeen = time.Now().UnixNano()
	f.state.Nodes[p.NodeID] = node
	return nil
}

func (f *FSM) applyMarkUnhealthy(p MarkUnhealthyPayload) error {
	node, ok := f.state.Nodes[p.NodeID]
	if !ok {
		return fmt.Errorf("node not found: %s", p.NodeID)
	}
	for _, shardID := range node.Shards {
		sr := f.state.Shards[shardID]
		sr.PrimaryNode = ""
		f.state.Shards[shardID] = sr
	}
	node.Status = NodeUnhealthy
	node.Shards = nil
	f.state.Nodes[p.NodeID] = node
	return nil
}

// State returns a deep copy of the current cluster state.
func (f *FSM) State() ClusterState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.clone()
}

// fsmSnapshot holds a snapshot of cluster state.
type fsmSnapshot struct {
	state ClusterState
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.state)
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// Snapshot implements raft.FSM.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &fsmSnapshot{state: f.state.clone()}, nil
}

// Restore implements raft.FSM.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var state ClusterState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}
	f.mu.Lock()
	f.state = state
	f.mu.Unlock()
	return nil
}
```

- [ ] **Step 4: Run tests — expect pass**

```bash
go test ./internal/metadata/... -v -run TestFSM
```

Expected: all 6 FSM tests pass.

- [ ] **Step 5: Verify build and lint**

```bash
make build && make lint
```

Expected: both exit 0.

---

## Task 5: Write gRPC ClusterService server

**Files:**
- Create: `internal/metadata/server.go`

- [ ] **Step 1: Create server.go**

Create `internal/metadata/server.go`:

```go
package metadata

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
)

// Server implements the gRPC ClusterService.
type Server struct {
	logengine.UnimplementedClusterServiceServer
	raft *raft.Raft
	fsm  *FSM
}

// NewServer creates a ClusterService gRPC server backed by the given Raft instance.
func NewServer(r *raft.Raft, fsm *FSM) *Server {
	return &Server{raft: r, fsm: fsm}
}

// applyCommand applies a command through Raft. Returns FAILED_PRECONDITION if not leader.
func (s *Server) applyCommand(cmdType CommandType, payload interface{}) error {
	if s.raft.State() != raft.Leader {
		return status.Error(codes.FailedPrecondition, "not the raft leader")
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return status.Errorf(codes.Internal, "marshal payload: %v", err)
	}
	cmd := Command{Type: cmdType, Payload: json.RawMessage(payloadBytes)}
	data, err := json.Marshal(cmd)
	if err != nil {
		return status.Errorf(codes.Internal, "marshal command: %v", err)
	}
	f := s.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		return status.Errorf(codes.Internal, "raft apply: %v", err)
	}
	if resp := f.Response(); resp != nil {
		if applyErr, ok := resp.(error); ok {
			return status.Errorf(codes.Internal, "fsm apply: %v", applyErr)
		}
	}
	return nil
}

// RegisterNode records a storage node in the cluster and assigns it unowned shards.
func (s *Server) RegisterNode(_ context.Context, req *logengine.RegisterNodeRequest) (*logengine.RegisterNodeResponse, error) {
	if req.NodeId == "" || req.GrpcAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and grpc_address are required")
	}
	if err := s.applyCommand(CmdRegisterNode, RegisterNodePayload{
		NodeID:  req.NodeId,
		Address: req.GrpcAddress,
	}); err != nil {
		return nil, err
	}
	state := s.fsm.State()
	node, ok := state.Nodes[req.NodeId]
	if !ok {
		return nil, status.Error(codes.Internal, "node not found after registration")
	}
	shards := make([]int32, len(node.Shards))
	for i, sid := range node.Shards {
		shards[i] = int32(sid)
	}
	return &logengine.RegisterNodeResponse{AssignedShards: shards}, nil
}

// Heartbeat records a liveness ping from a storage node.
func (s *Server) Heartbeat(_ context.Context, req *logengine.HeartbeatRequest) (*logengine.HeartbeatResponse, error) {
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if err := s.applyCommand(CmdUpdateHeartbeat, UpdateHeartbeatPayload{NodeID: req.NodeId}); err != nil {
		return nil, err
	}
	return &logengine.HeartbeatResponse{Ok: true}, nil
}

// GetClusterState returns the current node registry and shard map.
// Any coordinator can serve this — it reads from the local FSM.
func (s *Server) GetClusterState(_ context.Context, _ *logengine.GetClusterStateRequest) (*logengine.GetClusterStateResponse, error) {
	state := s.fsm.State()

	nodes := make([]*logengine.NodeInfo, 0, len(state.Nodes))
	for _, n := range state.Nodes {
		shards := make([]int32, len(n.Shards))
		for i, sid := range n.Shards {
			shards[i] = int32(sid)
		}
		nodes = append(nodes, &logengine.NodeInfo{
			Id:             n.ID,
			Address:        n.Address,
			Shards:         shards,
			Status:         string(n.Status),
			LastSeenUnixNs: n.LastSeen,
		})
	}

	shards := make([]*logengine.ShardInfo, 0, len(state.Shards))
	for _, sr := range state.Shards {
		shards = append(shards, &logengine.ShardInfo{
			ShardId:     int32(sr.ShardID),
			PrimaryNode: sr.PrimaryNode,
		})
	}

	return &logengine.GetClusterStateResponse{Nodes: nodes, Shards: shards}, nil
}
```

- [ ] **Step 2: Verify build and lint**

```bash
make build && make lint
```

Expected: both exit 0.

---

## Task 6: Write liveness checker

**Files:**
- Create: `internal/metadata/liveness.go`

- [ ] **Step 1: Create liveness.go**

Create `internal/metadata/liveness.go`:

```go
package metadata

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/hashicorp/raft"
)

// StartLivenessChecker monitors node heartbeats and marks stale nodes unhealthy.
// It only applies Raft commands when this coordinator is the leader.
// Call as a goroutine; it exits when ctx is cancelled.
func StartLivenessChecker(ctx context.Context, r *raft.Raft, fsm *FSM, interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.State() != raft.Leader {
				continue
			}
			checkLiveness(r, fsm, timeout)
		}
	}
}

func checkLiveness(r *raft.Raft, fsm *FSM, timeout time.Duration) {
	state := fsm.State()
	now := time.Now().UnixNano()
	timeoutNs := timeout.Nanoseconds()
	for _, node := range state.Nodes {
		if node.Status == NodeUnhealthy {
			continue
		}
		if now-node.LastSeen > timeoutNs {
			if err := applyMarkUnhealthy(r, node.ID); err != nil {
				log.Printf("liveness: failed to mark %s unhealthy: %v", node.ID, err)
			} else {
				log.Printf("liveness: marked %s unhealthy (last seen %.1fs ago)", node.ID, float64(now-node.LastSeen)/1e9)
			}
		}
	}
}

func applyMarkUnhealthy(r *raft.Raft, nodeID string) error {
	payload, _ := json.Marshal(MarkUnhealthyPayload{NodeID: nodeID})
	cmd := Command{Type: CmdMarkUnhealthy, Payload: json.RawMessage(payload)}
	data, _ := json.Marshal(cmd)
	f := r.Apply(data, 5*time.Second)
	return f.Error()
}
```

- [ ] **Step 2: Verify build and lint**

```bash
make build && make lint
```

Expected: both exit 0.

---

## Task 7: Write cluster client and heartbeat sender (TDD)

**Files:**
- Create: `internal/cluster/client.go`
- Create: `internal/cluster/heartbeat.go`
- Create: `internal/cluster/heartbeat_test.go`

- [ ] **Step 1: Write the heartbeat test first**

Create `internal/cluster/heartbeat_test.go`:

```go
package cluster_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Weilei424/distributed-log-query-engine/internal/cluster"
)

// stubSender counts calls and never errors.
type stubSender struct {
	calls int64
}

func (s *stubSender) SendHeartbeat(ctx context.Context) error {
	atomic.AddInt64(&s.calls, 1)
	return nil
}

func TestHeartbeatSender_StopsOnCancel(t *testing.T) {
	stub := &stubSender{}
	sender := cluster.NewHeartbeatSender(stub, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sender.Run(ctx)
		close(done)
	}()

	time.Sleep(70 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("HeartbeatSender did not stop after context cancel")
	}

	calls := atomic.LoadInt64(&stub.calls)
	if calls < 2 {
		t.Errorf("expected at least 2 heartbeat calls in 70ms, got %d", calls)
	}
}
```

- [ ] **Step 2: Run test — expect compile failure**

```bash
go test ./internal/cluster/... 2>&1 | head -10
```

Expected: compile error — `cluster.NewHeartbeatSender` undefined.

- [ ] **Step 3: Create client.go**

Create `internal/cluster/client.go`:

```go
package cluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
)

// ClusterClient connects a storage node to the coordinator cluster.
// It maintains a list of coordinator addresses and round-robins on non-leader responses.
type ClusterClient struct {
	addrs  []string
	idx    int
	conn   *grpc.ClientConn
	client logengine.ClusterServiceClient
	nodeID string
}

// NewClusterClient creates a ClusterClient targeting the given coordinator addresses.
// addrs must contain at least one address.
func NewClusterClient(addrs []string, nodeID string) (*ClusterClient, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("at least one coordinator address required")
	}
	c := &ClusterClient{addrs: addrs, nodeID: nodeID}
	if err := c.connectTo(addrs[0]); err != nil {
		return nil, err
	}
	return c, nil
}

// ParseAddrs splits a comma-separated address string into a slice.
func ParseAddrs(s string) []string {
	var result []string
	for _, a := range strings.Split(s, ",") {
		a = strings.TrimSpace(a)
		if a != "" {
			result = append(result, a)
		}
	}
	return result
}

func (c *ClusterClient) connectTo(addr string) error {
	if c.conn != nil {
		c.conn.Close()
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	c.conn = conn
	c.client = logengine.NewClusterServiceClient(conn)
	return nil
}

func (c *ClusterClient) advanceAndReconnect() error {
	c.idx = (c.idx + 1) % len(c.addrs)
	return c.connectTo(c.addrs[c.idx])
}

// Register calls RegisterNode on the coordinator cluster.
// It retries across coordinators on FAILED_PRECONDITION (not leader).
func (c *ClusterClient) Register(ctx context.Context, grpcAddr string) ([]int, error) {
	for attempt := 0; attempt < len(c.addrs)*3; attempt++ {
		resp, err := c.client.RegisterNode(ctx, &logengine.RegisterNodeRequest{
			NodeId:      c.nodeID,
			GrpcAddress: grpcAddr,
		})
		if err == nil {
			shards := make([]int, len(resp.AssignedShards))
			for i, s := range resp.AssignedShards {
				shards[i] = int(s)
			}
			return shards, nil
		}
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.FailedPrecondition:
			// This coordinator is not the leader; try the next one.
			if reconErr := c.advanceAndReconnect(); reconErr != nil {
				return nil, reconErr
			}
		case codes.Unavailable:
			// Cluster may be in election; back off and retry same address.
			time.Sleep(500 * time.Millisecond)
		default:
			return nil, err
		}
	}
	return nil, fmt.Errorf("register failed after retries: no reachable leader")
}

// SendHeartbeat sends a heartbeat to the coordinator leader.
func (c *ClusterClient) SendHeartbeat(ctx context.Context) error {
	for attempt := 0; attempt < len(c.addrs)*2; attempt++ {
		_, err := c.client.Heartbeat(ctx, &logengine.HeartbeatRequest{NodeId: c.nodeID})
		if err == nil {
			return nil
		}
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.FailedPrecondition:
			if reconErr := c.advanceAndReconnect(); reconErr != nil {
				return reconErr
			}
		default:
			return err
		}
	}
	return fmt.Errorf("heartbeat failed after retries")
}

// Close closes the underlying gRPC connection.
func (c *ClusterClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
```

- [ ] **Step 4: Create heartbeat.go**

Create `internal/cluster/heartbeat.go`:

```go
package cluster

import (
	"context"
	"log"
	"time"
)

// Beater abstracts the heartbeat send operation for testability.
type Beater interface {
	SendHeartbeat(ctx context.Context) error
}

// HeartbeatSender sends periodic heartbeats to the coordinator.
type HeartbeatSender struct {
	beater   Beater
	interval time.Duration
}

// NewHeartbeatSender creates a HeartbeatSender with the given send interval.
func NewHeartbeatSender(b Beater, interval time.Duration) *HeartbeatSender {
	return &HeartbeatSender{beater: b, interval: interval}
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
				log.Printf("heartbeat: %v", err)
			}
		}
	}
}
```

- [ ] **Step 5: Run tests — expect pass**

```bash
go test ./internal/cluster/... -v
```

Expected: `TestHeartbeatSender_StopsOnCancel` passes.

- [ ] **Step 6: Verify build and lint**

```bash
make build && make lint
```

Expected: both exit 0.

---

## Task 8: Write the coordinator binary

**Files:**
- Rewrite: `cmd/coordinator/main.go`

- [ ] **Step 1: Rewrite cmd/coordinator/main.go**

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
)

func main() {
	nodeID := envOrDefault("RAFT_NODE_ID", "coordinator-local")
	bindAddr := envOrDefault("RAFT_BIND_ADDR", "127.0.0.1:7000")
	dataDir := envOrDefault("RAFT_DATA_DIR", "./raft-data")
	peersStr := envOrDefault("RAFT_PEERS", "")
	grpcAddr := envOrDefault("GRPC_ADDR", ":9000")
	httpAddr := envOrDefault("HTTP_ADDR", ":8080")
	totalShards := envIntOrDefault("TOTAL_SHARDS", 16)
	heartbeatInterval := time.Duration(envIntOrDefault("HEARTBEAT_INTERVAL_SECONDS", 5)) * time.Second
	heartbeatTimeout := time.Duration(envIntOrDefault("HEARTBEAT_TIMEOUT_SECONDS", 15)) * time.Second

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	// Raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		log.Fatalf("raftboltdb: %v", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		log.Fatalf("snapshot store: %v", err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		log.Fatalf("resolve bind addr: %v", err)
	}
	transport, err := raft.NewTCPTransport(bindAddr, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("tcp transport: %v", err)
	}

	fsm := metadata.NewFSM(totalShards)
	r, err := raft.NewRaft(config, fsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		log.Fatalf("raft.NewRaft: %v", err)
	}

	// Bootstrap on first start only
	hasState, err := raft.HasExistingState(boltStore, boltStore, snapshotStore)
	if err != nil {
		log.Fatalf("check existing state: %v", err)
	}
	if !hasState {
		peers := parsePeers(peersStr, nodeID, bindAddr)
		cfg := raft.Configuration{Servers: peers}
		if f := r.BootstrapCluster(cfg); f.Error() != nil {
			log.Fatalf("bootstrap: %v", f.Error())
		}
	}

	// gRPC server
	grpcSrv := grpc.NewServer()
	logengine.RegisterClusterServiceServer(grpcSrv, metadata.NewServer(r, fsm))
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("grpc listen: %v", err)
	}

	// HTTP /status
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		state := fsm.State()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(state); err != nil {
			log.Printf("status encode: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()
	go func() {
		srv := &http.Server{Addr: httpAddr, Handler: mux}
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http serve: %v", err)
		}
	}()
	go metadata.StartLivenessChecker(ctx, r, fsm, heartbeatInterval, heartbeatTimeout)

	fmt.Printf("coordinator started: id=%s raft=%s grpc=%s http=%s shards=%d\n",
		nodeID, bindAddr, grpcAddr, httpAddr, totalShards)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	fmt.Println("shutting down...")
	cancel()
	grpcSrv.GracefulStop()
	if err := r.Shutdown().Error(); err != nil {
		log.Printf("raft shutdown: %v", err)
	}
	fmt.Println("coordinator stopped")
}

func parsePeers(peersStr, selfID, selfAddr string) []raft.Server {
	if peersStr == "" {
		return []raft.Server{{
			ID:      raft.ServerID(selfID),
			Address: raft.ServerAddress(selfAddr),
		}}
	}
	var servers []raft.Server
	for _, pair := range strings.Split(peersStr, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) != 2 {
			continue
		}
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(strings.TrimSpace(parts[0])),
			Address: raft.ServerAddress(strings.TrimSpace(parts[1])),
		})
	}
	return servers
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
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

- [ ] **Step 2: Verify build and lint**

```bash
make build && make lint
```

Expected: both exit 0.

---

## Task 9: Update storage node to register with coordinator

**Files:**
- Modify: `cmd/node/main.go`

- [ ] **Step 1: Update main.go**

Replace the contents of `cmd/node/main.go`:

```go
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

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/cluster"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func main() {
	nodeID := envOrDefault("NODE_ID", "node-local")
	dataDir := envOrDefault("DATA_DIR", "./data")
	grpcAddr := envOrDefault("GRPC_ADDR", ":50051")
	maxSegBytes := envInt64OrDefault("MAX_SEGMENT_BYTES", 64*1024*1024)
	coordinatorAddrs := envOrDefault("COORDINATOR_ADDRS", "")
	heartbeatInterval := time.Duration(envIntOrDefault("HEARTBEAT_INTERVAL_SECONDS", 5)) * time.Second

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register with the coordinator cluster if configured.
	if coordinatorAddrs != "" {
		addrs := cluster.ParseAddrs(coordinatorAddrs)
		clusterClient, err := cluster.NewClusterClient(addrs, nodeID)
		if err != nil {
			log.Printf("cluster client init: %v (continuing without cluster registration)", err)
		} else {
			regCtx, regCancel := context.WithTimeout(ctx, 30*time.Second)
			shards, err := clusterClient.Register(regCtx, grpcAddr)
			regCancel()
			if err != nil {
				log.Printf("cluster register: %v (continuing in degraded mode)", err)
			} else {
				fmt.Printf("registered with coordinator: shards=%v\n", shards)
			}
			sender := cluster.NewHeartbeatSender(clusterClient, heartbeatInterval)
			go sender.Run(ctx)
			defer clusterClient.Close()
		}
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

- [ ] **Step 2: Verify build and lint**

```bash
make build && make lint
```

Expected: both exit 0.

---

## Task 10: Integration test — cluster formation

**Files:**
- Create: `test/integration/cluster_test.go`

- [ ] **Step 1: Create cluster_test.go**

Create `test/integration/cluster_test.go`:

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
	"github.com/Weilei424/distributed-log-query-engine/internal/cluster"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
)

// testCoordinator is a self-contained in-process coordinator for integration tests.
type testCoordinator struct {
	addr string
	fsm  *metadata.FSM
	r    *raft.Raft
	srv  *grpc.Server
}

func (tc *testCoordinator) cleanup() {
	tc.srv.GracefulStop()
	tc.r.Shutdown()
}

func startTestCoordinator(t *testing.T, totalShards int) *testCoordinator {
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
	go grpcSrv.Serve(lis)

	return &testCoordinator{addr: lis.Addr().String(), fsm: fsm, r: r, srv: grpcSrv}
}

func waitForLeader(t *testing.T, r *raft.Raft, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r.State() == raft.Leader {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("coordinator never became leader")
}

func newClusterClient(t *testing.T, addr, nodeID string) *cluster.ClusterClient {
	t.Helper()
	c, err := cluster.NewClusterClient([]string{addr}, nodeID)
	if err != nil {
		t.Fatalf("NewClusterClient: %v", err)
	}
	return c
}

func TestCluster_NodeRegistersAndAppearsInState(t *testing.T) {
	coord := startTestCoordinator(t, 4)
	defer coord.cleanup()

	client := newClusterClient(t, coord.addr, "node-1")
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shards, err := client.Register(ctx, ":50051")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if len(shards) == 0 {
		t.Error("expected at least one shard assigned on first registration")
	}

	state := coord.fsm.State()
	node, ok := state.Nodes["node-1"]
	if !ok {
		t.Fatal("node-1 not found in cluster state")
	}
	if node.Status != metadata.NodeHealthy {
		t.Errorf("expected healthy, got %s", node.Status)
	}
}

func TestCluster_GetClusterState_ReturnsAllNodes(t *testing.T) {
	coord := startTestCoordinator(t, 4)
	defer coord.cleanup()

	for _, id := range []string{"node-1", "node-2", "node-3"} {
		c := newClusterClient(t, coord.addr, id)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := c.Register(ctx, ":50051"); err != nil {
			t.Fatalf("Register %s: %v", id, err)
		}
		cancel()
		c.Close()
	}

	// Query via gRPC GetClusterState
	conn, err := grpc.NewClient(coord.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial coordinator: %v", err)
	}
	defer conn.Close()
	svc := logengine.NewClusterServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := svc.GetClusterState(ctx, &logengine.GetClusterStateRequest{})
	if err != nil {
		t.Fatalf("GetClusterState: %v", err)
	}
	if len(resp.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(resp.Nodes))
	}
}
```

- [ ] **Step 2: Run test**

```bash
go test ./test/integration/... -run TestCluster -v -timeout 30s
```

Expected: both tests pass.

---

## Task 11: Integration test — node rejoin

**Files:**
- Create: `test/integration/rejoin_test.go`

- [ ] **Step 1: Create rejoin_test.go**

Create `test/integration/rejoin_test.go`:

```go
package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/Weilei424/distributed-log-query-engine/internal/cluster"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
)

func TestRejoin_NodeAppearsHealthyAfterRestart(t *testing.T) {
	coord := startTestCoordinator(t, 4)
	defer coord.cleanup()

	// Register node-1 for the first time.
	c := newClusterClient(t, coord.addr, "node-1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := c.Register(ctx, ":50051"); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	cancel()
	c.Close()

	// Simulate liveness check: mark node-1 unhealthy through Raft.
	markUnhealthyViaRaft(t, coord.r, "node-1")

	state := coord.fsm.State()
	if state.Nodes["node-1"].Status != metadata.NodeUnhealthy {
		t.Fatal("expected node-1 to be unhealthy before rejoin")
	}
	for _, sr := range state.Shards {
		if sr.PrimaryNode == "node-1" {
			t.Errorf("shard %d still owned by unhealthy node-1", sr.ShardID)
		}
	}

	// Rejoin: new client simulates the node process restarting.
	c2, err := cluster.NewClusterClient([]string{coord.addr}, "node-1")
	if err != nil {
		t.Fatalf("NewClusterClient rejoin: %v", err)
	}
	defer c2.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	shards, err := c2.Register(ctx2, ":50051")
	if err != nil {
		t.Fatalf("rejoin Register: %v", err)
	}
	if len(shards) == 0 {
		t.Error("expected shard assignment on rejoin")
	}

	state2 := coord.fsm.State()
	if state2.Nodes["node-1"].Status != metadata.NodeHealthy {
		t.Errorf("expected healthy after rejoin, got %s", state2.Nodes["node-1"].Status)
	}
}

// markUnhealthyViaRaft applies a MarkUnhealthy command directly through Raft.
func markUnhealthyViaRaft(t *testing.T, r *raft.Raft, nodeID string) {
	t.Helper()
	payload, _ := json.Marshal(metadata.MarkUnhealthyPayload{NodeID: nodeID})
	cmd := metadata.Command{Type: metadata.CmdMarkUnhealthy, Payload: json.RawMessage(payload)}
	data, _ := json.Marshal(cmd)
	if f := r.Apply(data, 5*time.Second); f.Error() != nil {
		t.Fatalf("markUnhealthyViaRaft: %v", f.Error())
	}
}
```

- [ ] **Step 2: Run test**

```bash
go test ./test/integration/... -run TestRejoin -v -timeout 30s
```

Expected: test passes.

---

## Task 12: Integration test — liveness detection

**Files:**
- Create: `test/integration/liveness_test.go`

- [ ] **Step 1: Create liveness_test.go**

Create `test/integration/liveness_test.go`:

```go
package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
)

func TestLiveness_NodeMarkedUnhealthyAfterMissedHeartbeats(t *testing.T) {
	coord := startTestCoordinator(t, 4)
	defer coord.cleanup()

	// Register node-1.
	c := newClusterClient(t, coord.addr, "node-1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := c.Register(ctx, ":50051"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	cancel()
	c.Close()
	// Do NOT start HeartbeatSender — intentionally let heartbeats lapse.

	// Start liveness checker with a short timeout so the test completes quickly.
	livenessCtx, livenessCancel := context.WithCancel(context.Background())
	defer livenessCancel()
	go metadata.StartLivenessChecker(livenessCtx, coord.r, coord.fsm,
		50*time.Millisecond,  // check interval
		200*time.Millisecond, // unhealthy threshold
	)

	// Wait for the liveness checker to fire and mark node-1 unhealthy.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		state := coord.fsm.State()
		node, ok := state.Nodes["node-1"]
		if ok && node.Status == metadata.NodeUnhealthy {
			// Verify shards were released.
			for _, sr := range state.Shards {
				if sr.PrimaryNode == "node-1" {
					t.Errorf("shard %d still owned by unhealthy node-1", sr.ShardID)
				}
			}
			return // test passed
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("node-1 was not marked unhealthy within timeout")
}
```

- [ ] **Step 2: Run test**

```bash
go test ./test/integration/... -run TestLiveness -v -timeout 30s
```

Expected: test passes.

- [ ] **Step 3: Run all tests**

```bash
make test
```

Expected: all unit and integration tests pass, exits 0.

- [ ] **Step 4: Run lint**

```bash
make lint
```

Expected: exits 0.

---

## Task 13: Update Docker Compose

**Files:**
- Modify: `deployments/docker-compose/docker-compose.yml`

- [ ] **Step 1: Replace docker-compose.yml**

Replace the contents of `deployments/docker-compose/docker-compose.yml`:

```yaml
services:
  coordinator-1:
    build:
      context: ../..
      dockerfile: deployments/docker-compose/Dockerfile.coordinator
    environment:
      - RAFT_NODE_ID=coordinator-1
      - RAFT_BIND_ADDR=coordinator-1:7000
      - RAFT_DATA_DIR=/raft-data
      - RAFT_PEERS=coordinator-1=coordinator-1:7000,coordinator-2=coordinator-2:7000,coordinator-3=coordinator-3:7000
      - GRPC_ADDR=:9000
      - HTTP_ADDR=:8080
      - TOTAL_SHARDS=16
    ports:
      - "9001:9000"
      - "8081:8080"
    volumes:
      - raft-data-1:/raft-data

  coordinator-2:
    build:
      context: ../..
      dockerfile: deployments/docker-compose/Dockerfile.coordinator
    environment:
      - RAFT_NODE_ID=coordinator-2
      - RAFT_BIND_ADDR=coordinator-2:7000
      - RAFT_DATA_DIR=/raft-data
      - RAFT_PEERS=coordinator-1=coordinator-1:7000,coordinator-2=coordinator-2:7000,coordinator-3=coordinator-3:7000
      - GRPC_ADDR=:9000
      - HTTP_ADDR=:8080
      - TOTAL_SHARDS=16
    ports:
      - "9002:9000"
      - "8082:8080"
    volumes:
      - raft-data-2:/raft-data

  coordinator-3:
    build:
      context: ../..
      dockerfile: deployments/docker-compose/Dockerfile.coordinator
    environment:
      - RAFT_NODE_ID=coordinator-3
      - RAFT_BIND_ADDR=coordinator-3:7000
      - RAFT_DATA_DIR=/raft-data
      - RAFT_PEERS=coordinator-1=coordinator-1:7000,coordinator-2=coordinator-2:7000,coordinator-3=coordinator-3:7000
      - GRPC_ADDR=:9000
      - HTTP_ADDR=:8080
      - TOTAL_SHARDS=16
    ports:
      - "9003:9000"
      - "8083:8080"
    volumes:
      - raft-data-3:/raft-data

  node-1:
    build:
      context: ../..
      dockerfile: deployments/docker-compose/Dockerfile.node
    environment:
      - NODE_ID=node-1
      - GRPC_ADDR=:50051
      - DATA_DIR=/data
      - COORDINATOR_ADDRS=coordinator-1:9000,coordinator-2:9000,coordinator-3:9000
    ports:
      - "50051:50051"
    volumes:
      - data-node-1:/data
    depends_on:
      - coordinator-1
      - coordinator-2
      - coordinator-3

  node-2:
    build:
      context: ../..
      dockerfile: deployments/docker-compose/Dockerfile.node
    environment:
      - NODE_ID=node-2
      - GRPC_ADDR=:50051
      - DATA_DIR=/data
      - COORDINATOR_ADDRS=coordinator-1:9000,coordinator-2:9000,coordinator-3:9000
    ports:
      - "50052:50051"
    volumes:
      - data-node-2:/data
    depends_on:
      - coordinator-1
      - coordinator-2
      - coordinator-3

  node-3:
    build:
      context: ../..
      dockerfile: deployments/docker-compose/Dockerfile.node
    environment:
      - NODE_ID=node-3
      - GRPC_ADDR=:50051
      - DATA_DIR=/data
      - COORDINATOR_ADDRS=coordinator-1:9000,coordinator-2:9000,coordinator-3:9000
    ports:
      - "50053:50051"
    volumes:
      - data-node-3:/data
    depends_on:
      - coordinator-1
      - coordinator-2
      - coordinator-3

volumes:
  raft-data-1:
  raft-data-2:
  raft-data-3:
  data-node-1:
  data-node-2:
  data-node-3:
```

- [ ] **Step 2: Validate Docker Compose config**

```bash
cd /mnt/d/projects/distributed-log-query-engine
docker compose -f deployments/docker-compose/docker-compose.yml config > /dev/null
```

Expected: exits 0, no errors.

---

## Task 14: Update BACKLOG.md

**Files:**
- Modify: `docs/planning/BACKLOG.md`

- [ ] **Step 1: Mark Phase 4 items complete**

In `docs/planning/BACKLOG.md`, replace the Phase 4 section:

```markdown
## Phase 4 — Multi-Node Cluster Formation and Metadata Coordination

**Plan:** `docs/superpowers/plans/2026-04-16-phase4-cluster-metadata.md`
**Spec:** `docs/superpowers/specs/2026-04-16-phase4-cluster-metadata-design.md`

### Status: Complete

- [x] `internal/metadata/state.go` — NodeRecord, ShardRecord, ClusterState types
- [x] `internal/metadata/fsm.go` — Raft FSM: Apply, Snapshot, Restore, command types
- [x] `internal/metadata/server.go` — gRPC ClusterService: RegisterNode, Heartbeat, GetClusterState
- [x] `internal/metadata/liveness.go` — liveness checker goroutine (leader only)
- [x] `proto/logengine/v1/cluster.proto` — ClusterService RPC definitions + buf generate
- [x] `internal/cluster/client.go` — ClusterClient with multi-address round-robin on non-leader
- [x] `internal/cluster/heartbeat.go` — HeartbeatSender goroutine
- [x] `cmd/coordinator/main.go` — real coordinator binary: Raft bootstrap, gRPC, HTTP /status
- [x] `cmd/node/main.go` — updated: cluster registration + heartbeat on startup
- [x] `deployments/docker-compose/docker-compose.yml` — 3 coordinators + updated node env vars
- [x] Unit tests: FSM RegisterNode, MarkUnhealthy, UpdateHeartbeat, snapshot/restore
- [x] Unit test: HeartbeatSender stops on context cancel
- [x] Integration test: node registers and appears in cluster state
- [x] Integration test: GetClusterState returns all registered nodes
- [x] Integration test: node restart rejoins cluster with shard assignment
- [x] Integration test: missed heartbeats cause node to be marked unhealthy and shards released
- [x] `make test` passes
- [x] `make lint` passes
- [x] `make build` passes
```

- [ ] **Step 2: Run final validation**

```bash
make test && make lint && make build
```

Expected: all three exit 0.

---

## Validation Summary

After all tasks are complete, these commands must all pass:

```bash
make build    # go build ./... exits 0
make test     # all unit + integration tests pass
make lint     # no lint errors
docker compose -f deployments/docker-compose/docker-compose.yml config > /dev/null
```

To manually verify the HTTP status endpoint with a local single-coordinator:

```bash
# Terminal 1: start coordinator in single-node mode
RAFT_NODE_ID=local RAFT_BIND_ADDR=127.0.0.1:7000 RAFT_DATA_DIR=/tmp/raft-test \
  GRPC_ADDR=:9000 HTTP_ADDR=:8080 TOTAL_SHARDS=4 \
  go run ./cmd/coordinator

# Terminal 2: check status (after coordinator starts)
curl http://localhost:8080/status | jq .

# Terminal 3: start a storage node pointing at coordinator
NODE_ID=node-local GRPC_ADDR=:50051 DATA_DIR=/tmp/node-data \
  COORDINATOR_ADDRS=localhost:9000 \
  go run ./cmd/node

# Terminal 2 again: verify node appears
curl http://localhost:8080/status | jq .
```
