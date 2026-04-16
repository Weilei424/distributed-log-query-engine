package query_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
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
	return query.NewQueryServer(query.NewLocalExecutor(idx, m))
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
