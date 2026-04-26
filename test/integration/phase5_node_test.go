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
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
	"github.com/Weilei424/distributed-log-query-engine/internal/replication"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

type testNode struct {
	addr          string
	nodeID        string
	manager       *storage.Manager
	idx           *index.Index
	replicator    *replication.Replicator
	stateCache    *cluster.StateCache
	clusterClient *cluster.ClusterClient
	grpcSrv       *grpc.Server
	cancel        context.CancelFunc
}

func (tn *testNode) cleanup() {
	tn.grpcSrv.GracefulStop()
	tn.replicator.Stop()
	tn.clusterClient.Close()
	tn.manager.Close()
	tn.cancel()
}

func (tn *testNode) ingestClient(t *testing.T) logengine.IngestServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(tn.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial node %s: %v", tn.nodeID, err)
	}
	t.Cleanup(func() { conn.Close() })
	return logengine.NewIngestServiceClient(conn)
}

func (tn *testNode) queryClient(t *testing.T) logengine.QueryServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(tn.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial node %s: %v", tn.nodeID, err)
	}
	t.Cleanup(func() { conn.Close() })
	return logengine.NewQueryServiceClient(conn)
}

func startPhase5Node(t *testing.T, nodeID string, coordAddr string, totalShards int) *testNode {
	t.Helper()

	dir := t.TempDir()
	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager %s: %v", nodeID, err)
	}
	idx := index.NewIndex()

	ctx, cancel := context.WithCancel(context.Background())

	clusterClient, err := cluster.NewClusterClient([]string{coordAddr}, nodeID)
	if err != nil {
		cancel()
		t.Fatalf("NewClusterClient %s: %v", nodeID, err)
	}

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		cancel()
		t.Fatalf("listen %s: %v", nodeID, err)
	}
	addr := lis.Addr().String()

	regCtx, regCancel := context.WithTimeout(ctx, 10*time.Second)
	shards, err := clusterClient.Register(regCtx, addr)
	regCancel()
	if err != nil {
		cancel()
		t.Fatalf("Register %s: %v", nodeID, err)
	}
	t.Logf("node %s registered: shards=%v addr=%s", nodeID, shards, addr)

	stateCache := cluster.NewStateCache(clusterClient, 100*time.Millisecond)
	stateCache.Refresh(ctx)
	go stateCache.Run(ctx)

	repl := replication.NewReplicator(totalShards)
	orch := ingest.NewOrchestrator(nodeID, totalShards, stateCache, m, idx, repl)
	srv := ingest.NewServer(orch, nodeID, totalShards, m, idx)

	grpcSrv := grpc.NewServer()
	logengine.RegisterIngestServiceServer(grpcSrv, srv)
	querySrv := query.NewQueryServer(query.NewLocalExecutor(idx, m))
	logengine.RegisterQueryServiceServer(grpcSrv, querySrv)
	go grpcSrv.Serve(lis) //nolint:errcheck

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

func entryCountOnNode(t *testing.T, m *storage.Manager) int {
	t.Helper()
	entries, err := m.ReadSegments(m.SegmentPaths())
	if err != nil {
		t.Fatalf("ReadSegments: %v", err)
	}
	return len(entries)
}

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
