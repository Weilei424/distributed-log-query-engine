package ingest

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// Server implements the gRPC IngestServiceServer interface.
type Server struct {
	logengine.UnimplementedIngestServiceServer
	manager *storage.Manager
	idx     *index.Index
}

// NewServer creates a new ingest Server backed by the given storage manager and index.
func NewServer(manager *storage.Manager, idx *index.Index) *Server {
	return &Server{manager: manager, idx: idx}
}

// Ingest writes a single log entry to the storage layer and updates the index.
func (s *Server) Ingest(ctx context.Context, req *logengine.IngestRequest) (*logengine.IngestResponse, error) {
	if req.Entry == nil {
		return nil, status.Error(codes.InvalidArgument, "entry is required")
	}
	if req.Entry.Service == "" {
		return nil, status.Error(codes.InvalidArgument, "entry.service is required")
	}
	if req.Entry.Message == "" {
		return nil, status.Error(codes.InvalidArgument, "entry.message is required")
	}

	entry := protoToEntry(req.Entry)
	entry.ReceivedAt = time.Now().UnixNano()

	// TODO: propagate ctx to manager.AppendWithPath when storage layer supports cancellation.
	segPath, err := s.manager.AppendWithPath(entry)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "append failed: %v", err)
	}

	s.idx.Add(entry, segPath)

	return &logengine.IngestResponse{Id: req.Entry.Id, Ok: true}, nil
}

// IngestBatch writes multiple log entries to the storage layer.
// Does not short-circuit on individual entry failure.
func (s *Server) IngestBatch(ctx context.Context, req *logengine.IngestBatchRequest) (*logengine.IngestBatchResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	var accepted, rejected int32
	for _, pb := range req.Entries {
		_, err := s.Ingest(ctx, &logengine.IngestRequest{Entry: pb})
		if err != nil {
			st, _ := status.FromError(err)
			if st.Code() == codes.Internal {
				return nil, status.Errorf(codes.Internal, "storage failure during batch ingest: %v", err)
			}
			rejected++
		} else {
			accepted++
		}
	}
	return &logengine.IngestBatchResponse{Accepted: accepted, Rejected: rejected}, nil
}

// protoToEntry converts a proto LogEntry to the internal types.LogEntry.
// Keeps internal/storage free of direct proto API dependencies.
func protoToEntry(pb *logengine.LogEntry) *types.LogEntry {
	return &types.LogEntry{
		ID:         pb.Id,
		Timestamp:  pb.Timestamp,
		ReceivedAt: pb.ReceivedAt,
		Service:    pb.Service,
		Level:      pb.Level,
		Message:    pb.Message,
		Fields:     pb.Fields,
	}
}
