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
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
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

// writeEntryDirect writes a LogEntry directly to a node's local storage and
// index, bypassing routing so the entry is guaranteed to live on that node.
func writeEntryDirect(t *testing.T, node *testNode, id, service, message string) {
	t.Helper()
	e := &types.LogEntry{
		ID:      id,
		Service: service,
		Level:   "INFO",
		Message: message,
	}
	path, err := node.manager.AppendWithPath(e)
	if err != nil {
		t.Fatalf("AppendWithPath on %s: %v", node.nodeID, err)
	}
	node.idx.Add(e, path)
}

// TestDistributedQuery_AllNodes writes one entry directly to each storage node
// (bypassing routing) and verifies the coordinator's QueryService merges results
// from both nodes into a single response.
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

	// Write directly to each node's storage and index so each node definitely
	// holds its own entry — independent of shard routing decisions.
	writeEntryDirect(t, nodeA, "entry-from-a", "svc-a", "message on node a")
	writeEntryDirect(t, nodeB, "entry-from-b", "svc-b", "message on node b")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Verify each node holds its entry locally before querying the coordinator.
	nodeAQuery := nodeA.queryClient(t)
	nodeAResp, err := nodeAQuery.Query(ctx, &logengine.QueryRequest{Limit: 100})
	if err != nil {
		t.Fatalf("node-a direct Query: %v", err)
	}
	nodeAIDs := make(map[string]bool)
	for _, e := range nodeAResp.Entries {
		nodeAIDs[e.Id] = true
	}
	if !nodeAIDs["entry-from-a"] {
		t.Fatal("entry-from-a not visible on node-a before coordinator query")
	}

	nodeBQuery := nodeB.queryClient(t)
	nodeBResp, err := nodeBQuery.Query(ctx, &logengine.QueryRequest{Limit: 100})
	if err != nil {
		t.Fatalf("node-b direct Query: %v", err)
	}
	nodeBIDs := make(map[string]bool)
	for _, e := range nodeBResp.Entries {
		nodeBIDs[e.Id] = true
	}
	if !nodeBIDs["entry-from-b"] {
		t.Fatal("entry-from-b not visible on node-b before coordinator query")
	}

	// Query the coordinator: it must fan out to both nodes and merge.
	coordQuery := coord.queryClient(t)
	resp, err := coordQuery.Query(ctx, &logengine.QueryRequest{Limit: 100})
	if err != nil {
		t.Fatalf("coordinator Query: %v", err)
	}

	if resp.Partial {
		t.Error("expected Partial=false; both nodes should respond")
	}

	// Dedup by ID — replication may have copied entries across nodes.
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

// startFastCoordinator starts a second coordinator gRPC server that uses a
// short per-node timeout (500 ms) so partial-failure tests don't stall.
func startFastCoordinator(t *testing.T, fsm coordinator.ClusterStateProvider) string {
	t.Helper()
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen fast-coord: %v", err)
	}
	fastExec := coordinator.NewFanOutExecutor(fsm, 500, 1000)
	grpcSrv := grpc.NewServer()
	logengine.RegisterQueryServiceServer(grpcSrv, coordinator.NewFanOutQueryServer(fastExec))
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcSrv.GracefulStop)
	return lis.Addr().String()
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

	// Write directly to node-b so its entry is guaranteed to be local.
	writeEntryDirect(t, nodeB, "survivor-entry", "svc-c", "this node survived")

	// Stop node-a so the coordinator's fan-out will fail for it.
	nodeA.cleanup()
	time.Sleep(100 * time.Millisecond)

	// Use a short per-node timeout coordinator so the test does not stall on the dead node.
	fastCoordAddr := startFastCoordinator(t, coord.fsm)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(fastCoordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial fast coordinator: %v", err)
	}
	defer conn.Close()

	resp, err := logengine.NewQueryServiceClient(conn).Query(ctx, &logengine.QueryRequest{Limit: 100})
	if err != nil {
		t.Fatalf("coordinator Query: %v", err)
	}

	if !resp.Partial {
		t.Error("expected Partial=true when node-a is unreachable")
	}

	found := false
	for _, e := range resp.Entries {
		if e.Id == "survivor-entry" {
			found = true
		}
	}
	if !found {
		t.Error("expected survivor-entry from node-b in partial result")
	}
}
