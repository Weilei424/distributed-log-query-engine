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
	"github.com/Weilei424/distributed-log-query-engine/internal/coordinator"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
)

// testPhase6Coordinator is a coordinator that also serves QueryService (fan-out).
type testPhase6Coordinator struct {
	testCoordinator
}

func (tc *testPhase6Coordinator) queryClient(t *testing.T) logengine.QueryServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(tc.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial coordinator: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return logengine.NewQueryServiceClient(conn)
}

func startPhase6Coordinator(t *testing.T, totalShards int) *testPhase6Coordinator {
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

	// Wire fan-out query service: 5 s per-node timeout, 1000 fan-out limit.
	fanOutExec := coordinator.NewFanOutExecutor(fsm, 5000, 1000)
	logengine.RegisterQueryServiceServer(grpcSrv, coordinator.NewFanOutQueryServer(fanOutExec))

	go grpcSrv.Serve(lis) //nolint:errcheck

	return &testPhase6Coordinator{
		testCoordinator: testCoordinator{
			addr: lis.Addr().String(),
			fsm:  fsm,
			r:    r,
			srv:  grpcSrv,
		},
	}
}

// TestDistributedQuery_AllNodes ingests entries to two storage nodes via the
// orchestrator and verifies the coordinator's QueryService returns merged results
// from both nodes.
func TestDistributedQuery_AllNodes(t *testing.T) {
	const totalShards = 4
	coord := startPhase6Coordinator(t, totalShards)
	defer coord.cleanup()

	nodeA := startPhase5Node(t, "node-a", coord.addr, totalShards)
	defer nodeA.cleanup()
	nodeB := startPhase5Node(t, "node-b", coord.addr, totalShards)
	defer nodeB.cleanup()

	// Wait until both nodes appear healthy in cluster state.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state := coord.fsm.State()
		na, aOK := state.Nodes["node-a"]
		nb, bOK := state.Nodes["node-b"]
		if aOK && bOK && na.Status == metadata.NodeHealthy && nb.Status == metadata.NodeHealthy {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	state := coord.fsm.State()
	if state.Nodes["node-a"].Status != metadata.NodeHealthy {
		t.Fatal("node-a never became healthy")
	}
	if state.Nodes["node-b"].Status != metadata.NodeHealthy {
		t.Fatal("node-b never became healthy")
	}

	// Ingest one entry directly to each node's local storage (bypassing routing
	// to ensure each node holds exactly one entry regardless of shard assignment).
	nodeAIngest := nodeA.ingestClient(t)
	nodeBIngest := nodeB.ingestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := nodeAIngest.Ingest(ctx, &logengine.IngestRequest{
		Entry: &logengine.LogEntry{
			Id: "entry-from-a", Service: "svc-a", Level: "INFO",
			Message: "message on node a",
		},
	}); err != nil {
		t.Fatalf("Ingest to node-a: %v", err)
	}
	if _, err := nodeBIngest.Ingest(ctx, &logengine.IngestRequest{
		Entry: &logengine.LogEntry{
			Id: "entry-from-b", Service: "svc-b", Level: "INFO",
			Message: "message on node b",
		},
	}); err != nil {
		t.Fatalf("Ingest to node-b: %v", err)
	}

	// Wait for replication to settle before querying.
	time.Sleep(200 * time.Millisecond)

	// Sanity-check: node-a should hold at least one entry locally.
	nodeAQuery := nodeA.queryClient(t)
	nodeAResp, err := nodeAQuery.Query(ctx, &logengine.QueryRequest{Limit: 100})
	if err != nil {
		t.Fatalf("node-a direct Query: %v", err)
	}
	if len(nodeAResp.Entries) == 0 {
		t.Error("expected node-a to hold at least one entry after ingest")
	}

	coordQuery := coord.queryClient(t)
	resp, err := coordQuery.Query(ctx, &logengine.QueryRequest{Limit: 100})
	if err != nil {
		t.Fatalf("coordinator Query: %v", err)
	}

	if resp.Partial {
		t.Error("expected Partial=false; both nodes should respond")
	}

	// We expect at least 2 distinct entry IDs (one from each node).
	// Replication may have copied entries, but dedup by ID ensures each appears once.
	ids := make(map[string]bool)
	for _, e := range resp.Entries {
		ids[e.Id] = true
	}
	if !ids["entry-from-a"] {
		t.Error("coordinator response missing entry-from-a")
	}
	if !ids["entry-from-b"] {
		t.Error("coordinator response missing entry-from-b")
	}
}

// TestDistributedQuery_PartialFailure stops one storage node before querying and
// verifies the coordinator returns partial=true along with the surviving node's results.
func TestDistributedQuery_PartialFailure(t *testing.T) {
	const totalShards = 4
	coord := startPhase6Coordinator(t, totalShards)
	defer coord.cleanup()

	nodeA := startPhase5Node(t, "node-a", coord.addr, totalShards)
	nodeB := startPhase5Node(t, "node-b", coord.addr, totalShards)
	defer nodeB.cleanup()

	// Wait until both nodes are healthy.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state := coord.fsm.State()
		na, aOK := state.Nodes["node-a"]
		nb, bOK := state.Nodes["node-b"]
		if aOK && bOK && na.Status == metadata.NodeHealthy && nb.Status == metadata.NodeHealthy {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Ingest one entry to node-b (the node that will survive).
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nodeBIngest := nodeB.ingestClient(t)
	// svc-c hashes to shard 1 (fnv32a("svc-c") % 4 == 1), which node-b owns as
	// primary after two-node round-robin assignment. This ensures the entry is
	// written locally to node-b rather than forwarded to node-a.
	if _, err := nodeBIngest.Ingest(ctx, &logengine.IngestRequest{
		Entry: &logengine.LogEntry{
			Id: "survivor-entry", Service: "svc-c", Level: "INFO",
			Message: "this node survived",
		},
	}); err != nil {
		t.Fatalf("Ingest to node-b: %v", err)
	}

	// Wait a moment for the write to persist before stopping node-a.
	time.Sleep(50 * time.Millisecond)

	// Stop node-a so the coordinator's fan-out will fail for it.
	nodeA.cleanup()
	time.Sleep(100 * time.Millisecond)

	// Use a short per-node timeout so the test does not wait too long for the dead node.
	fastExec := coordinator.NewFanOutExecutor(coord.fsm, 500, 1000)
	fastSrv := coordinator.NewFanOutQueryServer(fastExec)

	// Call Execute directly (without gRPC overhead) to avoid needing a second listener.
	result, err := fastExec.Execute(ctx, &logengine.QueryRequest{Limit: 100})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	_ = fastSrv // used to confirm it compiles

	if !result.Partial {
		t.Error("expected Partial=true when node-a is unreachable")
	}

	found := false
	for _, e := range result.Entries {
		if e.ID == "survivor-entry" {
			found = true
		}
	}
	if !found {
		t.Error("expected survivor-entry from node-b in partial result")
	}
}
