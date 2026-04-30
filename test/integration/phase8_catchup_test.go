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
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

// TestTransferSegment_ListAndStream verifies that ListSegments and TransferSegment
// RPCs work correctly on a local single-node setup.
func TestTransferSegment_ListAndStream(t *testing.T) {
	dir := t.TempDir()
	// Tiny segment cap so rotation (and thus closed segments) happen quickly.
	m, err := storage.NewManager(dir, 1)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	idx := index.NewIndex()
	srv := ingest.NewLocalServer(m, idx)
	srv.SetLogger(zap.NewNop())

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	logengine.RegisterIngestServiceServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcSrv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	client := logengine.NewIngestServiceClient(conn)
	ctx := context.Background()
	now := time.Now().UnixNano()

	// Ingest enough entries to cause at least one segment rotation.
	for i := 0; i < 5; i++ {
		_, err := client.Ingest(ctx, &logengine.IngestRequest{Entry: &logengine.LogEntry{
			Timestamp: now + int64(i),
			Service:   "svc",
			Message:   "transfer test entry",
		}})
		if err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	resp, err := client.ListSegments(ctx, &logengine.ListSegmentsRequest{ShardId: 0})
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	t.Logf("ListSegments returned %d segment names", len(resp.SegmentNames))

	if len(resp.SegmentNames) > 0 {
		stream, err := client.TransferSegment(ctx, &logengine.TransferSegmentRequest{
			SegmentName: resp.SegmentNames[0],
			ShardId:     0,
		})
		if err != nil {
			t.Fatalf("TransferSegment: %v", err)
		}
		var totalBytes int
		for {
			chunk, err := stream.Recv()
			if err != nil {
				break
			}
			totalBytes += len(chunk.Chunk)
		}
		if totalBytes == 0 {
			t.Fatal("expected non-empty segment transfer")
		}
		t.Logf("transferred %d bytes", totalBytes)
	}
}
