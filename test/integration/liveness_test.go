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

	// Register node-1. Do NOT start HeartbeatSender — intentionally let heartbeats lapse.
	c := newClusterClient(t, coord.addr, "node-1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := c.Register(ctx, ":50051"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	cancel()
	c.Close()

	// Start liveness checker with a short timeout so the test completes quickly.
	livenessCtx, livenessCancel := context.WithCancel(context.Background())
	defer livenessCancel()
	go metadata.StartLivenessChecker(livenessCtx, coord.r, coord.fsm,
		50*time.Millisecond,  // check interval
		200*time.Millisecond, // unhealthy threshold
	)

	// Wait for node-1 to be marked unhealthy.
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
	t.Fatal("node-1 was not marked unhealthy within 3 seconds")
}
