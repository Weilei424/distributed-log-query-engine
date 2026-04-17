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
	"github.com/Weilei424/distributed-log-query-engine/internal/cluster"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
)

// testCoordinator is a self-contained in-process coordinator for integration tests.
type testCoordinator struct {
	addr string
	fsm  *metadata.FSM
	r    *raft.Raft
	srv  *grpc.Server
}

func (tc *testCoordinator) cleanup() {
	tc.srv.GracefulStop()
	tc.r.Shutdown()
}

func startTestCoordinator(t *testing.T, totalShards int) *testCoordinator {
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
	go grpcSrv.Serve(lis) //nolint:errcheck

	return &testCoordinator{addr: lis.Addr().String(), fsm: fsm, r: r, srv: grpcSrv}
}

func waitForLeader(t *testing.T, r *raft.Raft, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r.State() == raft.Leader {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("coordinator never became leader")
}

func newClusterClient(t *testing.T, addr, nodeID string) *cluster.ClusterClient {
	t.Helper()
	c, err := cluster.NewClusterClient([]string{addr}, nodeID)
	if err != nil {
		t.Fatalf("NewClusterClient: %v", err)
	}
	return c
}

func TestCluster_NodeRegistersAndAppearsInState(t *testing.T) {
	coord := startTestCoordinator(t, 4)
	defer coord.cleanup()

	client := newClusterClient(t, coord.addr, "node-1")
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	shards, err := client.Register(ctx, ":50051")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if len(shards) == 0 {
		t.Error("expected at least one shard assigned on first registration")
	}

	state := coord.fsm.State()
	node, ok := state.Nodes["node-1"]
	if !ok {
		t.Fatal("node-1 not found in cluster state")
	}
	if node.Status != metadata.NodeHealthy {
		t.Errorf("expected healthy, got %s", node.Status)
	}
}

func TestCluster_GetClusterState_ReturnsAllNodes(t *testing.T) {
	coord := startTestCoordinator(t, 4)
	defer coord.cleanup()

	for _, id := range []string{"node-1", "node-2", "node-3"} {
		c := newClusterClient(t, coord.addr, id)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := c.Register(ctx, ":50051"); err != nil {
			t.Fatalf("Register %s: %v", id, err)
		}
		cancel()
		c.Close()
	}

	conn, err := grpc.NewClient(coord.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial coordinator: %v", err)
	}
	defer conn.Close()
	svc := logengine.NewClusterServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := svc.GetClusterState(ctx, &logengine.GetClusterStateRequest{})
	if err != nil {
		t.Fatalf("GetClusterState: %v", err)
	}
	if len(resp.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(resp.Nodes))
	}
}
