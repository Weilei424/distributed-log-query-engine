package query

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// QueryServer implements the gRPC QueryServiceServer interface.
type QueryServer struct {
	logengine.UnimplementedQueryServiceServer
	executor *LocalExecutor
}

// NewQueryServer returns a QueryServer backed by the given executor.
func NewQueryServer(executor *LocalExecutor) *QueryServer {
	return &QueryServer{executor: executor}
}

// Query handles a gRPC Query request.
func (s *QueryServer) Query(ctx context.Context, req *logengine.QueryRequest) (*logengine.QueryResponse, error) {
	start := time.Now()

	if req.Limit < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "limit must be non-negative")
	}
	if req.Offset < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "offset must be non-negative")
	}

	typesReq := &types.QueryRequest{
		Keyword:   req.Keyword,
		Service:   req.Service,
		StartTime: req.StartTime,
		EndTime:   req.EndTime,
		Limit:     req.Limit,
		Offset:    req.Offset,
	}

	result, err := s.executor.Execute(ctx, typesReq)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, status.Errorf(codes.Canceled, "query canceled")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Errorf(codes.DeadlineExceeded, "query deadline exceeded")
		}
		return nil, status.Errorf(codes.Internal, "query failed: %v", err)
	}

	pbEntries := make([]*logengine.LogEntry, len(result.Entries))
	for i, e := range result.Entries {
		pbEntries[i] = &logengine.LogEntry{
			Id:         e.ID,
			Timestamp:  e.Timestamp,
			ReceivedAt: e.ReceivedAt,
			Service:    e.Service,
			Level:      e.Level,
			Message:    e.Message,
			Fields:     e.Fields,
		}
	}

	return &logengine.QueryResponse{
		Entries: pbEntries,
		Total:   result.Total,
		Partial: false,
		TookMs:  time.Since(start).Milliseconds(),
	}, nil
}
