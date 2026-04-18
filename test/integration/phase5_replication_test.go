// test/integration/phase5_replication_test.go
package integration_test

import (
	"context"
	"testing"
	"time"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func TestPhase5_AsyncReplicationToReplica(t *testing.T) {
	const totalShards = 4
	coord := startTestCoordinator(t, totalShards)
	defer coord.cleanup()

	nodeA := startPhase5Node(t, "node-a", coord.addr, totalShards)
	defer nodeA.cleanup()
	nodeB := startPhase5Node(t, "node-b", coord.addr, totalShards)
	defer nodeB.cleanup()

	var state = coord.fsm.State()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(state.Nodes["node-a"].Shards) > 0 && len(state.Nodes["node-b"].Shards) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
		state = coord.fsm.State()
	}
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

	// Wait until nodeA's local state cache also knows about the replica so that
	// the replicator has a valid destination address when we write.
	shardForSvc := ingest.ShardID(svcForNodeA, totalShards)
	cacheDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(cacheDeadline) {
		_, replica := nodeA.stateCache.ShardOwners(shardForSvc)
		if replica != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("using service %q (shard %d → node-a primary)", svcForNodeA, ingest.ShardID(svcForNodeA, totalShards))

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

	if n := entriesWithService(t, nodeA.manager, svcForNodeA); n != 1 {
		t.Errorf("primary node-a: expected 1 entry, got %d", n)
	}

	waitForEntry(t, nodeB.manager, func(m *storage.Manager) bool {
		return entriesWithService(t, m, svcForNodeA) >= 1
	}, 2*time.Second)
}
