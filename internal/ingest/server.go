// internal/ingest/server.go
package ingest

import (
	"context"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

// Server implements the gRPC IngestServiceServer interface.
// Client-facing RPCs (Ingest, IngestBatch) delegate to the Orchestrator.
// Internal RPCs (ReplicateEntry, FetchShardEntries) bypass routing and
// operate directly on local storage.
type Server struct {
	logengine.UnimplementedIngestServiceServer
	orchestrator *Orchestrator
	nodeID       string
	totalShards  int
	manager      *storage.Manager
	idx          *index.Index
}

// NewServer creates a Server backed by the given orchestrator.
// Use for cluster-aware nodes.
func NewServer(orchestrator *Orchestrator, nodeID string, totalShards int, manager *storage.Manager, idx *index.Index) *Server {
	return &Server{
		orchestrator: orchestrator,
		nodeID:       nodeID,
		totalShards:  totalShards,
		manager:      manager,
		idx:          idx,
	}
}

// NewLocalServer creates a Server for single-node use without cluster routing.
// All writes go directly to local storage. Used by tests and no-coordinator mode.
func NewLocalServer(manager *storage.Manager, idx *index.Index) *Server {
	orch := newLocalOrchestrator(manager, idx)
	return &Server{
		orchestrator: orch,
		nodeID:       "local",
		totalShards:  0,
		manager:      manager,
		idx:          idx,
	}
}

// Ingest delegates to the orchestrator for routing and local write.
func (s *Server) Ingest(ctx context.Context, req *logengine.IngestRequest) (*logengine.IngestResponse, error) {
	return s.orchestrator.HandleIngest(ctx, req)
}

// IngestBatch writes multiple log entries via the orchestrator.
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

// ReplicateEntry writes an entry directly to local storage, bypassing routing.
// Called by the primary's Replicator to deliver an async copy to this replica.
func (s *Server) ReplicateEntry(ctx context.Context, req *logengine.ReplicateEntryRequest) (*logengine.ReplicateEntryResponse, error) {
	if req.Entry == nil {
		return nil, status.Error(codes.InvalidArgument, "entry is required")
	}
	// Defensive check: the computed shard must match the claimed shard_id.
	if s.totalShards > 0 {
		computed := ShardID(req.Entry.Service, s.totalShards)
		if computed != int(req.ShardId) {
			return nil, status.Errorf(codes.FailedPrecondition,
				"shard mismatch: computed %d for service %q, request claims %d",
				computed, req.Entry.Service, req.ShardId)
		}
	}
	entry := ProtoToEntry(req.Entry)
	segPath, err := s.manager.AppendWithPath(entry)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "replicate append failed: %v", err)
	}
	s.idx.Add(entry, segPath)
	return &logengine.ReplicateEntryResponse{Ok: true}, nil
}

// FetchShardEntries returns entries for a shard with received_at > since_unix_ns.
// Called by a replica node during catch-up on restart.
func (s *Server) FetchShardEntries(ctx context.Context, req *logengine.FetchShardEntriesRequest) (*logengine.FetchShardEntriesResponse, error) {
	all, err := s.manager.ReadSegments(s.manager.SegmentPaths())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read segments: %v", err)
	}

	var result []*logengine.LogEntry
	for _, e := range all {
		if s.totalShards > 0 && ShardID(e.Service, s.totalShards) != int(req.ShardId) {
			continue
		}
		if e.ReceivedAt <= req.SinceUnixNs {
			continue
		}
		result = append(result, EntryToProto(e))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReceivedAt < result[j].ReceivedAt
	})

	return &logengine.FetchShardEntriesResponse{Entries: result}, nil
}
