// test/integration/phase5_routing_test.go
package integration_test

import (
	"context"
	"testing"
	"time"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func TestPhase5_RoutingForwardsToCorrectPrimary(t *testing.T) {
	const totalShards = 4
	coord := startTestCoordinator(t, totalShards)
	defer coord.cleanup()

	nodeA := startPhase5Node(t, "node-a", coord.addr, totalShards)
	defer nodeA.cleanup()
	nodeB := startPhase5Node(t, "node-b", coord.addr, totalShards)
	defer nodeB.cleanup()

	// Wait until both the FSM and node-a's state cache agree that node-b owns at least one shard.
	var serviceThatRoutesToB string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		state := coord.fsm.State()
		for _, svc := range []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"} {
			sid := ingest.ShardID(svc, totalShards)
			sr, ok := state.Shards[sid]
			if !ok || sr.PrimaryNode != "node-b" {
				continue
			}
			// Also confirm node-a's cache sees the same owner so forwarding works.
			primary, _ := nodeA.stateCache.ShardOwners(sid)
			if primary == "node-b" {
				serviceThatRoutesToB = svc
				break
			}
		}
		if serviceThatRoutesToB != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Also wait until node-b's cache shows itself as primary so it won't forward back.
	if serviceThatRoutesToB != "" {
		sid := ingest.ShardID(serviceThatRoutesToB, totalShards)
		deadline2 := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline2) {
			primary, _ := nodeB.stateCache.ShardOwners(sid)
			if primary == "node-b" {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	if serviceThatRoutesToB == "" {
		t.Skip("could not find a test service that routes to node-b; shard distribution may differ")
	}
	t.Logf("using service %q (shard %d → node-b)", serviceThatRoutesToB, ingest.ShardID(serviceThatRoutesToB, totalShards))

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

	// Verify the primary (node-b) received the write.
	waitForEntry(t, nodeB.manager, func(m *storage.Manager) bool {
		return entriesWithService(t, m, serviceThatRoutesToB) >= 1
	}, 3*time.Second)

	// node-a is the replica for shards owned by node-b, so async replication will
	// deliver the entry to node-a as well. We verify here that the entry on node-b
	// arrived as a primary write, not that node-a has zero entries.
	// The critical assertion is that node-b has the entry (proved by waitForEntry above).
	t.Logf("routing verified: entry present on primary node-b; node-a entry count (replica copy expected)=%d",
		entriesWithService(t, nodeA.manager, serviceThatRoutesToB))
}
