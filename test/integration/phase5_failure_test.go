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

func TestPhase5_PrimaryFailure_ReplicaStillServesLogs(t *testing.T) {
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
		t.Skip("could not find a service routed to node-a")
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

	clientA := nodeA.ingestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := clientA.Ingest(ctx, &logengine.IngestRequest{
		Entry: &logengine.LogEntry{Service: svcForNodeA, Message: "pre-failure write", Level: "INFO"},
	})
	if err != nil {
		nodeA.cleanup()
		t.Fatalf("Ingest to primary: %v", err)
	}

	waitForEntry(t, nodeB.manager, func(m *storage.Manager) bool {
		return entriesWithService(t, m, svcForNodeA) >= 1
	}, 2*time.Second)

	nodeA.cleanup()

	count := entriesWithService(t, nodeB.manager, svcForNodeA)
	if count == 0 {
		t.Errorf("replica node-b has no entries for service %q after primary stopped", svcForNodeA)
	}

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
