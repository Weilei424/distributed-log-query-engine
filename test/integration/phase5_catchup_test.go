// test/integration/phase5_catchup_test.go
package integration_test

import (
	"context"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/cluster"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/replication"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func TestPhase5_CatchUp_ReplicaFetchesMissedEntries(t *testing.T) {
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

	// Stop replica before writing so it misses the entries.
	nodeB.cleanup()

	clientA := nodeA.ingestClient(t)
	for i := 0; i < 3; i++ {
		_, err := clientA.Ingest(context.Background(), &logengine.IngestRequest{
			Entry: &logengine.LogEntry{Service: svcForNodeA, Message: "missed-entry", Level: "INFO"},
		})
		if err != nil {
			t.Fatalf("Ingest %d: %v", i, err)
		}
	}

	// Simulate node-b restart with fresh storage.
	dir2 := t.TempDir()
	m2, err := storage.NewManager(dir2, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager restart: %v", err)
	}
	defer m2.Close()
	idx2 := index.NewIndex()

	clusterClient2, err := cluster.NewClusterClient([]string{coord.addr}, "node-b")
	if err != nil {
		t.Fatalf("NewClusterClient: %v", err)
	}
	defer clusterClient2.Close()

	lis2, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr2 := lis2.Addr().String()

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = clusterClient2.Register(regCtx, addr2)
	regCancel()
	if err != nil {
		t.Fatalf("Register restart: %v", err)
	}

	// Exercise the real CatchUp path (same code used in main.go startup).
	catchUpState, err := clusterClient2.GetClusterState(context.Background())
	if err != nil {
		t.Fatalf("GetClusterState for catch-up: %v", err)
	}
	appended := ingest.CatchUp(context.Background(), "node-b", totalShards, catchUpState, m2, idx2)
	if appended != 3 {
		t.Errorf("CatchUp: expected 3 entries appended, got %d", appended)
	}
	if total := entryCountOnNode(t, m2); total != 3 {
		t.Errorf("after catch-up: expected 3 entries on replica, got %d", total)
	}

	// Bring replica back up and verify it can receive ReplicateEntry calls.
	repl2 := replication.NewReplicator(totalShards, "node-b", zap.NewNop())
	defer repl2.Stop()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	sc2 := cluster.NewStateCache(clusterClient2, 100*time.Millisecond)
	sc2.Refresh(ctx2)
	go sc2.Run(ctx2)

	orch2 := ingest.NewOrchestrator("node-b", totalShards, sc2, m2, idx2, repl2)
	srv2 := ingest.NewServer(orch2, "node-b", totalShards, m2, idx2)
	grpcSrv2 := grpc.NewServer()
	logengine.RegisterIngestServiceServer(grpcSrv2, srv2)
	go grpcSrv2.Serve(lis2) //nolint:errcheck
	defer grpcSrv2.GracefulStop()

	replicaConn, err := grpc.NewClient(addr2, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial restarted replica: %v", err)
	}
	defer replicaConn.Close()

	shardID := ingest.ShardID(svcForNodeA, totalShards)
	_, err = logengine.NewIngestServiceClient(replicaConn).ReplicateEntry(context.Background(), &logengine.ReplicateEntryRequest{
		Entry:   &logengine.LogEntry{Id: "post-catchup", Service: svcForNodeA, Message: "post-restart", Level: "INFO"},
		ShardId: int32(shardID),
	})
	if err != nil {
		t.Fatalf("ReplicateEntry on restarted replica: %v", err)
	}

	if total2 := entryCountOnNode(t, m2); total2 != 4 {
		t.Errorf("after ReplicateEntry: expected 4 entries, got %d", total2)
	}
}
