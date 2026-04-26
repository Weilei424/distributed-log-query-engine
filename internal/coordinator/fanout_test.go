package coordinator

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

// staticStateProvider implements ClusterStateProvider with a fixed ClusterState.
type staticStateProvider struct {
	state metadata.ClusterState
}

func (s *staticStateProvider) State() metadata.ClusterState { return s.state }

// startInProcessQueryNode starts a gRPC QueryService server backed by a real
// LocalExecutor. Returns the server address, the storage manager, and the index.
func startInProcessQueryNode(t *testing.T) (addr string, mgr *storage.Manager, idx *index.Index) {
	t.Helper()
	dir := t.TempDir()
	var err error
	mgr, err = storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	idx = index.NewIndex()

	executor := query.NewLocalExecutor(idx, mgr)
	querySrv := query.NewQueryServer(executor)

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr = lis.Addr().String()

	grpcSrv := grpc.NewServer()
	logengine.RegisterQueryServiceServer(grpcSrv, querySrv)
	t.Cleanup(func() {
		grpcSrv.GracefulStop()
		mgr.Close() //nolint:errcheck
	})
	go grpcSrv.Serve(lis) //nolint:errcheck
	return addr, mgr, idx
}

// ingestLocalEntry writes one entry directly to a node's storage + index
// without going through a gRPC call. Timestamp must be set explicitly.
func ingestLocalEntry(t *testing.T, mgr *storage.Manager, idx *index.Index, e *logengine.LogEntry) {
	t.Helper()
	srv := ingest.NewLocalServer(mgr, idx)
	if _, err := srv.Ingest(context.Background(), &logengine.IngestRequest{Entry: e}); err != nil {
		t.Fatalf("Ingest %s: %v", e.Id, err)
	}
}

func TestFanOutExecutor_MergesFromTwoNodes(t *testing.T) {
	addr1, mgr1, idx1 := startInProcessQueryNode(t)
	addr2, mgr2, idx2 := startInProcessQueryNode(t)

	ingestLocalEntry(t, mgr1, idx1, &logengine.LogEntry{
		Id: "n1-e1", Service: "svc", Level: "INFO",
		Message: "hello from node1", Timestamp: 200,
	})
	ingestLocalEntry(t, mgr2, idx2, &logengine.LogEntry{
		Id: "n2-e1", Service: "svc", Level: "INFO",
		Message: "hello from node2", Timestamp: 100,
	})

	state := metadata.ClusterState{
		Nodes: map[string]metadata.NodeRecord{
			"node-1": {ID: "node-1", Address: addr1, Status: metadata.NodeHealthy},
			"node-2": {ID: "node-2", Address: addr2, Status: metadata.NodeHealthy},
		},
		Shards: map[int]metadata.ShardRecord{},
	}

	exec := NewFanOutExecutor(&staticStateProvider{state}, 5000, 1000)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := exec.Execute(ctx, &logengine.QueryRequest{Limit: 10})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Partial {
		t.Error("expected Partial=false; both nodes should respond")
	}
	if len(result.Entries) != 2 {
		t.Fatalf("expected 2 entries (one per node), got %d", len(result.Entries))
	}
	// Sorted timestamp desc: n1-e1(200) then n2-e1(100)
	if result.Entries[0].ID != "n1-e1" {
		t.Errorf("expected first entry n1-e1, got %s", result.Entries[0].ID)
	}
	if result.Entries[1].ID != "n2-e1" {
		t.Errorf("expected second entry n2-e1, got %s", result.Entries[1].ID)
	}
	if result.Total != 2 {
		t.Errorf("expected Total=2, got %d", result.Total)
	}
}

func TestFanOutExecutor_SkipsUnhealthyNodes(t *testing.T) {
	addr1, mgr1, idx1 := startInProcessQueryNode(t)

	ingestLocalEntry(t, mgr1, idx1, &logengine.LogEntry{
		Id: "e1", Service: "svc", Level: "INFO", Message: "hello", Timestamp: 100,
	})

	state := metadata.ClusterState{
		Nodes: map[string]metadata.NodeRecord{
			"node-1": {ID: "node-1", Address: addr1, Status: metadata.NodeHealthy},
			// node-2 is unhealthy — should be skipped
			"node-2": {ID: "node-2", Address: "127.0.0.1:9999", Status: metadata.NodeUnhealthy},
		},
		Shards: map[int]metadata.ShardRecord{},
	}

	exec := NewFanOutExecutor(&staticStateProvider{state}, 5000, 1000)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := exec.Execute(ctx, &logengine.QueryRequest{Limit: 10})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Partial {
		t.Error("expected Partial=false; unhealthy nodes are skipped, not failures")
	}
	if len(result.Entries) != 1 {
		t.Fatalf("expected 1 entry from healthy node, got %d", len(result.Entries))
	}
}

func TestFanOutExecutor_PartialOnNodeFailure(t *testing.T) {
	addr1, mgr1, idx1 := startInProcessQueryNode(t)

	ingestLocalEntry(t, mgr1, idx1, &logengine.LogEntry{
		Id: "e1", Service: "svc", Level: "INFO", Message: "hello", Timestamp: 100,
	})

	state := metadata.ClusterState{
		Nodes: map[string]metadata.NodeRecord{
			"node-1": {ID: "node-1", Address: addr1, Status: metadata.NodeHealthy},
			// node-2 has a bad address — dial or query will fail
			"node-2": {ID: "node-2", Address: "127.0.0.1:1", Status: metadata.NodeHealthy},
		},
		Shards: map[int]metadata.ShardRecord{},
	}

	exec := NewFanOutExecutor(&staticStateProvider{state}, 500, 1000)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := exec.Execute(ctx, &logengine.QueryRequest{Limit: 10})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.Partial {
		t.Error("expected Partial=true when one node is unreachable")
	}
	// node-1's entry should still be returned
	found := false
	for _, e := range result.Entries {
		if e.ID == "e1" {
			found = true
		}
	}
	if !found {
		t.Error("expected entry from healthy node to be present in partial result")
	}
}
