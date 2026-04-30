package query_test

import (
	"context"
	"net"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func newQueryServer(t *testing.T) *query.QueryServer {
	t.Helper()
	m, err := storage.NewManager(t.TempDir(), 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	idx := index.NewIndex()
	return query.NewQueryServer(query.NewLocalExecutor(idx, m), "", zap.NewNop())
}

func TestQueryServer_NegativeLimit_InvalidArgument(t *testing.T) {
	srv := newQueryServer(t)
	_, err := srv.Query(context.Background(), &logengine.QueryRequest{Limit: -1})
	if err == nil {
		t.Fatal("expected error for negative limit, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T: %v", err, err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected codes.InvalidArgument, got %v", st.Code())
	}
}

func TestQueryServer_NegativeOffset_InvalidArgument(t *testing.T) {
	srv := newQueryServer(t)
	_, err := srv.Query(context.Background(), &logengine.QueryRequest{Offset: -1})
	if err == nil {
		t.Fatal("expected error for negative offset, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T: %v", err, err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected codes.InvalidArgument, got %v", st.Code())
	}
}

func TestQueryServer_CanceledContext_ReturnsCanceled(t *testing.T) {
	srv := newQueryServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := srv.Query(ctx, &logengine.QueryRequest{})
	if err == nil {
		t.Fatal("expected error for canceled context, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T: %v", err, err)
	}
	if st.Code() != codes.Canceled {
		t.Errorf("expected codes.Canceled, got %v", st.Code())
	}
}

// TestQueryServer_GRPCRoundTrip exercises the full gRPC encoding path: ingest via
// IngestService RPC, query via QueryService RPC, verify proto response fields.
func TestQueryServer_GRPCRoundTrip(t *testing.T) {
	dir := t.TempDir()
	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	idx := index.NewIndex()
	ingestSrv := ingest.NewLocalServer(m, idx)
	querySrv := query.NewQueryServer(query.NewLocalExecutor(idx, m), "", zap.NewNop())

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	logengine.RegisterIngestServiceServer(grpcSrv, ingestSrv)
	logengine.RegisterQueryServiceServer(grpcSrv, querySrv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	ctx := context.Background()
	ingestClient := logengine.NewIngestServiceClient(conn)
	queryClient := logengine.NewQueryServiceClient(conn)

	entries := []*logengine.LogEntry{
		{Id: "1", Service: "auth", Level: "INFO", Message: "user login success", Timestamp: 100},
		{Id: "2", Service: "db", Level: "ERROR", Message: "connection timeout", Timestamp: 200},
		{Id: "3", Service: "auth", Level: "WARN", Message: "user login failed", Timestamp: 300},
	}
	for _, e := range entries {
		if _, err := ingestClient.Ingest(ctx, &logengine.IngestRequest{Entry: e}); err != nil {
			t.Fatalf("Ingest %s: %v", e.Id, err)
		}
	}

	resp, err := queryClient.Query(ctx, &logengine.QueryRequest{QueryString: "login"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if resp.Total != 2 {
		t.Errorf("expected Total=2, got %d", resp.Total)
	}
	if len(resp.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(resp.Entries))
	}
	// Results sorted descending by timestamp: entry 3 (ts=300) before entry 1 (ts=100).
	if resp.Entries[0].Id != "3" || resp.Entries[1].Id != "1" {
		t.Errorf("expected IDs [3, 1], got [%s, %s]", resp.Entries[0].Id, resp.Entries[1].Id)
	}
	// Verify proto fields are mapped through correctly.
	if resp.Entries[0].Service != "auth" || resp.Entries[0].Level != "WARN" {
		t.Errorf("unexpected field values: service=%q level=%q", resp.Entries[0].Service, resp.Entries[0].Level)
	}
}
