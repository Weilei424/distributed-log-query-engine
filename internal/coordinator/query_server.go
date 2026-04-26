package coordinator

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
)

// FanOutQueryServer implements QueryServiceServer using distributed fan-out.
type FanOutQueryServer struct {
	logengine.UnimplementedQueryServiceServer
	executor *FanOutExecutor
}

// NewFanOutQueryServer returns a FanOutQueryServer backed by executor.
func NewFanOutQueryServer(executor *FanOutExecutor) *FanOutQueryServer {
	return &FanOutQueryServer{executor: executor}
}

// Query handles a gRPC Query request by fanning out to all healthy nodes.
func (s *FanOutQueryServer) Query(ctx context.Context, req *logengine.QueryRequest) (*logengine.QueryResponse, error) {
	start := time.Now()

	if req.Limit < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "limit must be non-negative")
	}
	if req.Offset < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "offset must be non-negative")
	}

	result, err := s.executor.Execute(ctx, req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, status.Errorf(codes.Canceled, "query canceled")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Errorf(codes.DeadlineExceeded, "query deadline exceeded")
		}
		return nil, status.Errorf(codes.Internal, "fan-out query failed: %v", err)
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
		Partial: result.Partial,
		TookMs:  time.Since(start).Milliseconds(),
	}, nil
}
