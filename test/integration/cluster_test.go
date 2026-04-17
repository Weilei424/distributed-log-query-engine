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

// startThreeCoordinatorCluster stands up three in-memory Raft coordinators peered together.
// The returned slice has the leader at index 0 (after waitForLeader). Callers must call cleanup
// on each returned coordinator.
func startThreeCoordinatorCluster(t *testing.T, totalShards int) []*testCoordinator {
	t.Helper()

	ids := []raft.ServerID{"coord-1", "coord-2", "coord-3"}

	// Create all transports first so we can wire them together.
	addrs := make([]raft.ServerAddress, 3)
	transports := make([]*raft.InmemTransport, 3)
	for i, id := range ids {
		addr, tr := raft.NewInmemTransport(raft.ServerAddress(id))
		addrs[i] = addr
		transports[i] = tr
	}

	// Connect every pair bidirectionally.
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}

	bootCfg := raft.Configuration{
		Servers: []raft.Server{
			{ID: ids[0], Address: addrs[0]},
			{ID: ids[1], Address: addrs[1]},
			{ID: ids[2], Address: addrs[2]},
		},
	}

	coords := make([]*testCoordinator, 3)
	for i, id := range ids {
		cfg := raft.DefaultConfig()
		cfg.LocalID = id
		cfg.HeartbeatTimeout = 50 * time.Millisecond
		cfg.ElectionTimeout = 50 * time.Millisecond
		cfg.CommitTimeout = 5 * time.Millisecond
		cfg.LeaderLeaseTimeout = 50 * time.Millisecond

		logStore := raft.NewInmemStore()
		stableStore := raft.NewInmemStore()
		snapStore := raft.NewInmemSnapshotStore()
		fsm := metadata.NewFSM(totalShards)

		r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapStore, transports[i])
		if err != nil {
			t.Fatalf("NewRaft %s: %v", id, err)
		}
		// Bootstrap returns ErrCantBootstrap on non-first nodes after cluster is formed; ignore it.
		r.BootstrapCluster(bootCfg) //nolint:errcheck

		lis, err := net.Listen("tcp", ":0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		grpcSrv := grpc.NewServer()
		logengine.RegisterClusterServiceServer(grpcSrv, metadata.NewServer(r, fsm))
		go grpcSrv.Serve(lis) //nolint:errcheck

		coords[i] = &testCoordinator{addr: lis.Addr().String(), fsm: fsm, r: r, srv: grpcSrv}
	}

	// Wait for one of the three nodes to become leader.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, c := range coords {
			if c.r.State() == raft.Leader {
				return coords
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("three-coordinator cluster never elected a leader")
	return nil
}

// leaderCoord returns the coordinator that is currently the Raft leader.
func leaderCoord(t *testing.T, coords []*testCoordinator) *testCoordinator {
	t.Helper()
	for _, c := range coords {
		if c.r.State() == raft.Leader {
			return c
		}
	}
	t.Fatal("no leader found in coordinator set")
	return nil
}

// followerCoord returns a coordinator that is not the Raft leader.
func followerCoord(t *testing.T, coords []*testCoordinator) *testCoordinator {
	t.Helper()
	for _, c := range coords {
		if c.r.State() != raft.Leader {
			return c
		}
	}
	t.Fatal("no follower found in coordinator set")
	return nil
}

func TestCluster_ThreeCoordinatorRaftFormation(t *testing.T) {
	coords := startThreeCoordinatorCluster(t, 4)
	for _, c := range coords {
		defer c.cleanup()
	}

	// Verify all three Raft nodes are part of the cluster (one leader, two followers).
	leaderCount := 0
	followerCount := 0
	for _, c := range coords {
		switch c.r.State() {
		case raft.Leader:
			leaderCount++
		case raft.Follower:
			followerCount++
		}
	}
	if leaderCount != 1 {
		t.Errorf("expected 1 leader, got %d", leaderCount)
	}
	if followerCount != 2 {
		t.Errorf("expected 2 followers, got %d", followerCount)
	}

	// Register a node via the leader.
	leader := leaderCoord(t, coords)
	c := newClusterClient(t, leader.addr, "node-1")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shards, err := c.Register(ctx, "node-1:50051")
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if len(shards) == 0 {
		t.Error("expected shards assigned on registration")
	}

	// Allow log replication to reach followers.
	time.Sleep(100 * time.Millisecond)

	// GetClusterState via a follower — verifies state replicated across the cluster.
	follower := followerCoord(t, coords)
	conn, err := grpc.NewClient(follower.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial follower: %v", err)
	}
	defer conn.Close()
	svc := logengine.NewClusterServiceClient(conn)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	resp, err := svc.GetClusterState(ctx2, &logengine.GetClusterStateRequest{})
	if err != nil {
		t.Fatalf("GetClusterState on follower: %v", err)
	}
	if len(resp.Nodes) != 1 {
		t.Errorf("expected 1 node in follower state, got %d", len(resp.Nodes))
	}
	if resp.Nodes[0].Id != "node-1" {
		t.Errorf("expected node-1, got %s", resp.Nodes[0].Id)
	}
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
