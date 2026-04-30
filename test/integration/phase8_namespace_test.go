package integration_test

import (
	"context"
	"testing"
	"time"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
)

// TestNamespaceIsolation verifies that namespace-scoped queries return only
// entries belonging to the requested namespace, and that an unfiltered query
// returns entries from all namespaces.
func TestNamespaceIsolation(t *testing.T) {
	const totalShards = 4
	coord := startTestCoordinator(t, totalShards)
	defer coord.cleanup()

	node := startPhase5Node(t, "ns-test-node", coord.addr, totalShards)
	defer node.cleanup()

	// Wait until the node is healthy before ingesting.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state := coord.fsm.State()
		if n, ok := state.Nodes["ns-test-node"]; ok && n.Status == metadata.NodeHealthy {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	ctx := context.Background()
	now := time.Now().UnixNano()

	ingestClient := node.ingestClient(t)

	// Ingest one entry per namespace.
	for _, ns := range []string{"team-alpha", "team-beta"} {
		_, err := ingestClient.Ingest(ctx, &logengine.IngestRequest{
			Entry: &logengine.LogEntry{
				Timestamp: now,
				Namespace: ns,
				Service:   "svc",
				Level:     "INFO",
				Message:   "hello from " + ns,
			},
		})
		if err != nil {
			t.Fatalf("ingest %s: %v", ns, err)
		}
	}

	queryClient := node.queryClient(t)

	// Query for team-alpha only.
	resp, err := queryClient.Query(ctx, &logengine.QueryRequest{
		Namespace: "team-alpha",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("query team-alpha: %v", err)
	}
	if len(resp.Entries) != 1 {
		t.Fatalf("expected 1 entry for team-alpha, got %d", len(resp.Entries))
	}
	if resp.Entries[0].Namespace != "team-alpha" {
		t.Fatalf("wrong namespace in result: got %q, want %q", resp.Entries[0].Namespace, "team-alpha")
	}

	// Query for team-beta only.
	resp2, err := queryClient.Query(ctx, &logengine.QueryRequest{
		Namespace: "team-beta",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("query team-beta: %v", err)
	}
	if len(resp2.Entries) != 1 {
		t.Fatalf("expected 1 entry for team-beta, got %d", len(resp2.Entries))
	}
	if resp2.Entries[0].Namespace != "team-beta" {
		t.Fatalf("wrong namespace in result: got %q, want %q", resp2.Entries[0].Namespace, "team-beta")
	}

	// Query with no namespace filter — should return both entries.
	resp3, err := queryClient.Query(ctx, &logengine.QueryRequest{Limit: 10})
	if err != nil {
		t.Fatalf("query all: %v", err)
	}
	if len(resp3.Entries) != 2 {
		t.Fatalf("expected 2 entries with no namespace filter, got %d", len(resp3.Entries))
	}
}
