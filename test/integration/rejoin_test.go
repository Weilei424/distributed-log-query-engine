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
