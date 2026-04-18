// internal/replication/replicator_test.go
package replication_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/replication"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// fakeIngestServer counts ReplicateEntry calls.
type fakeIngestServer struct {
	logengine.UnimplementedIngestServiceServer
	received atomic.Int32
}

func (f *fakeIngestServer) ReplicateEntry(_ context.Context, req *logengine.ReplicateEntryRequest) (*logengine.ReplicateEntryResponse, error) {
	f.received.Add(1)
	return &logengine.ReplicateEntryResponse{Ok: true}, nil
}

func startFakeReplica(t *testing.T) (addr string, fake *fakeIngestServer) {
	t.Helper()
	fake = &fakeIngestServer{}
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	logengine.RegisterIngestServiceServer(srv, fake)
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(srv.GracefulStop)
	return lis.Addr().String(), fake
}

func TestReplicator_DeliverEntry(t *testing.T) {
	addr, fake := startFakeReplica(t)

	r := replication.NewReplicator(4)
	t.Cleanup(r.Stop)

	entry := &types.LogEntry{ID: "e1", Service: "auth", Message: "hello"}
	r.Enqueue(entry, 0, addr)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fake.received.Load() >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("entry not delivered to replica within 2 seconds")
}

func TestReplicator_EnqueueNonBlocking(t *testing.T) {
	// Use an address that no server listens on — connections will fail.
	// The channel should still accept entries without blocking.
	r := replication.NewReplicator(4)
	t.Cleanup(r.Stop)

	entry := &types.LogEntry{ID: "e1", Service: "auth", Message: "hello"}
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Fill beyond capacity to trigger the drop path.
		for i := 0; i < 300; i++ {
			r.Enqueue(entry, 0, "localhost:19999")
		}
	}()
	select {
	case <-done:
		// All 300 calls returned without blocking — pass.
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue blocked for more than 2 seconds")
	}
}

func TestReplicator_StopsCleanly(t *testing.T) {
	addr, _ := startFakeReplica(t)
	r := replication.NewReplicator(4)

	entry := &types.LogEntry{ID: "e1", Service: "auth", Message: "hello"}
	r.Enqueue(entry, 0, addr)

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Stop()
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not return within 3 seconds")
	}
}
