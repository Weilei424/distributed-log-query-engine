package metadata_test

import (
	"encoding/json"
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
		NodeID:    "node-1",
		Address:   ":50051",
		NowUnixNs: 1000,
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
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051", NowUnixNs: 1000})
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-2", Address: ":50052", NowUnixNs: 1000})

	state := fsm.State()
	if n := len(state.Nodes["node-2"].Shards); n != 0 {
		t.Errorf("node-2 expected 0 shards (greedy assignment), got %d", n)
	}
}

func TestFSM_UpdateHeartbeat(t *testing.T) {
	fsm := metadata.NewFSM(4)
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{
		NodeID:    "node-1",
		Address:   ":50051",
		NowUnixNs: 1000,
	})

	applyCmd(t, fsm, metadata.CmdUpdateHeartbeat, metadata.UpdateHeartbeatPayload{
		NodeID:    "node-1",
		NowUnixNs: 2000,
	})

	after := fsm.State().Nodes["node-1"].LastSeen
	if after != 2000 {
		t.Errorf("expected LastSeen=2000, got %d", after)
	}
}

func TestFSM_MarkUnhealthy_ClearsShards(t *testing.T) {
	fsm := metadata.NewFSM(4)
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051", NowUnixNs: 1000})
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
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051", NowUnixNs: 1000})
	applyCmd(t, fsm, metadata.CmdMarkUnhealthy, metadata.MarkUnhealthyPayload{NodeID: "node-1"})
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051", NowUnixNs: 1000})

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
	applyCmd(t, fsm, metadata.CmdRegisterNode, metadata.RegisterNodePayload{NodeID: "node-1", Address: ":50051", NowUnixNs: 1000})

	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	fsm2 := metadata.NewFSM(4)
	if err := fsm2.Restore(noopCloser(strings.NewReader(sink.buf.String()))); err != nil {
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

// nopReadCloser wraps an io.Reader with a no-op Close.
type nopReadCloser struct {
	r interface{ Read([]byte) (int, error) }
}

func (n nopReadCloser) Read(p []byte) (int, error) { return n.r.Read(p) }
func (n nopReadCloser) Close() error               { return nil }

// noopCloser wraps r in an io.ReadCloser with a no-op Close method.
func noopCloser(r interface{ Read([]byte) (int, error) }) nopReadCloser {
	return nopReadCloser{r: r}
}
