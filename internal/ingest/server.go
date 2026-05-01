// internal/ingest/server.go
package ingest

import (
	"bytes"
	"context"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/cluster"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/observability"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

const transferChunkSize = 64 * 1024 // 64KB

// Server implements the gRPC IngestServiceServer interface.
// Client-facing RPCs (Ingest, IngestBatch) delegate to the Orchestrator.
// Internal RPCs (ReplicateEntry, FetchShardEntries) bypass routing and
// operate directly on local storage.
type Server struct {
	logengine.UnimplementedIngestServiceServer
	orchestrator *Orchestrator
	nodeID       string
	totalShards  int
	stateReader  cluster.ClusterStateReader // nil in local mode
	manager      *storage.Manager
	idx          *index.Index
	logger       *zap.Logger
}

// NewServer creates a Server backed by the given orchestrator.
// Use for cluster-aware nodes.
func NewServer(orchestrator *Orchestrator, nodeID string, totalShards int, manager *storage.Manager, idx *index.Index) *Server {
	return &Server{
		orchestrator: orchestrator,
		nodeID:       nodeID,
		totalShards:  totalShards,
		stateReader:  orchestrator.StateReader(),
		manager:      manager,
		idx:          idx,
		logger:       zap.NewNop(),
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
		logger:       zap.NewNop(),
	}
}

// SetLogger replaces the no-op logger with a real one. Call once after construction.
func (s *Server) SetLogger(l *zap.Logger) { s.logger = l }

// Ingest delegates to the orchestrator for routing and local write.
func (s *Server) Ingest(ctx context.Context, req *logengine.IngestRequest) (*logengine.IngestResponse, error) {
	start := time.Now()

	// Priority: gRPC metadata (forwarded hop) > Go context value (IngestBatch) > new ID.
	reqID := observability.RequestIDFromIncomingContext(ctx)
	if reqID == "" {
		reqID = observability.RequestIDFromContext(ctx)
	}
	if reqID == "" {
		reqID = observability.NewRequestID()
	}
	ctx = observability.WithRequestID(ctx, reqID)

	source := "client"
	if observability.IsForwardedFromIncomingContext(ctx) {
		source = "forwarded"
	}

	resp, err := s.orchestrator.HandleIngest(ctx, req)

	reqStatus := "ok"
	if err != nil {
		reqStatus = "error"
	}
	observability.IngestRequestsTotal.WithLabelValues(s.nodeID, reqStatus, source).Inc()

	if err == nil {
		shardID := -1
		if s.totalShards > 0 && req.Entry != nil {
			shardID = ShardID(req.Entry.GetNamespace(), req.Entry.GetService(), s.totalShards)
		}
		s.logger.Info("ingest",
			zap.String("request_id", reqID),
			zap.String("service", req.Entry.GetService()),
			zap.Int("shard_id", shardID),
			zap.Int("entry_count", 1),
			zap.Int64("duration_ms", time.Since(start).Milliseconds()),
		)
	}
	return resp, err
}

// IngestBatch writes multiple log entries via the orchestrator.
func (s *Server) IngestBatch(ctx context.Context, req *logengine.IngestBatchRequest) (*logengine.IngestBatchResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	start := time.Now()
	batchReqID := observability.NewRequestID()
	ctx = observability.WithRequestID(ctx, batchReqID)
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
	s.logger.Info("ingest_batch",
		zap.String("request_id", batchReqID),
		zap.Int("entry_count", len(req.Entries)),
		zap.Int32("accepted", accepted),
		zap.Int32("rejected", rejected),
		zap.Int64("duration_ms", time.Since(start).Milliseconds()),
	)
	return &logengine.IngestBatchResponse{Accepted: accepted, Rejected: rejected}, nil
}

// ReplicateEntry writes an entry directly to local storage, bypassing routing.
// Called by the primary's Replicator to deliver an async copy to this replica.
func (s *Server) ReplicateEntry(ctx context.Context, req *logengine.ReplicateEntryRequest) (*logengine.ReplicateEntryResponse, error) {
	if req.Entry == nil {
		return nil, status.Error(codes.InvalidArgument, "entry is required")
	}
	if s.totalShards > 0 {
		// Verify the entry actually belongs to the claimed shard.
		computed := ShardID(req.Entry.Namespace, req.Entry.Service, s.totalShards)
		if computed != int(req.ShardId) {
			return nil, status.Errorf(codes.FailedPrecondition,
				"shard mismatch: computed %d for service %q, request claims %d",
				computed, req.Entry.Service, req.ShardId)
		}
		// Verify this node is the designated replica for the shard.
		// Allow writes when the shard is unassigned (state cache may be temporarily stale
		// during rebalancing), but reject when it is explicitly assigned elsewhere.
		if s.stateReader != nil {
			_, replica := s.stateReader.ShardOwners(int(req.ShardId))
			if replica != "" && replica != s.nodeID {
				return nil, status.Errorf(codes.FailedPrecondition,
					"node %s is not the replica for shard %d (replica is %s)",
					s.nodeID, req.ShardId, replica)
			}
		}
	}
	entry := ProtoToEntry(req.Entry)
	segPath, err := s.manager.AppendWithPath(entry)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "replicate append failed: %v", err)
	}
	s.idx.Add(entry, segPath)
	observability.IndexTokenCount.WithLabelValues(s.nodeID).Set(float64(s.idx.TokenCount()))
	return &logengine.ReplicateEntryResponse{Ok: true}, nil
}

// ListSegments returns names of closed segment files that contain entries for the given shard.
func (s *Server) ListSegments(_ context.Context, req *logengine.ListSegmentsRequest) (*logengine.ListSegmentsResponse, error) {
	closed := s.manager.ListClosedSegments()
	names := make([]string, 0, len(closed))
	for _, p := range closed {
		entries, err := s.manager.ReadSegments([]string{p})
		if err != nil {
			continue
		}
		for _, e := range entries {
			if s.totalShards == 0 || ShardID(e.Namespace, e.Service, s.totalShards) == int(req.ShardId) {
				names = append(names, filepath.Base(p))
				break
			}
		}
	}
	return &logengine.ListSegmentsResponse{SegmentNames: names}, nil
}

// TransferSegment streams a shard-filtered view of a closed segment file.
// Only entries belonging to req.ShardId are included, serialized in the same
// length-prefixed protobuf format used by segment files on disk. This ensures
// the replica receives a valid segment file containing only its own shard data,
// even when the source segment holds entries from multiple shards.
func (s *Server) TransferSegment(req *logengine.TransferSegmentRequest, stream logengine.IngestService_TransferSegmentServer) error {
	name := req.SegmentName
	// Reject path traversal attempts and non-.seg names.
	if strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") || !strings.HasSuffix(name, ".seg") {
		return status.Errorf(codes.InvalidArgument, "invalid segment name: %q", name)
	}
	// Reject the active segment — it is still being written and must not be transferred.
	if name == s.manager.ActiveSegmentName() {
		return status.Errorf(codes.FailedPrecondition, "segment %q is the active segment", name)
	}
	path := filepath.Join(s.manager.Dir(), name)
	entries, err := s.manager.ReadSegments([]string{path})
	if err != nil {
		return status.Errorf(codes.NotFound, "segment not found: %s", name)
	}

	// Build a filtered segment in memory containing only entries for the requested shard.
	var buf bytes.Buffer
	for _, e := range entries {
		if s.totalShards > 0 && ShardID(e.Namespace, e.Service, s.totalShards) != int(req.ShardId) {
			continue
		}
		pb := EntryToProto(e)
		data, marshalErr := proto.Marshal(pb)
		if marshalErr != nil {
			return status.Errorf(codes.Internal, "marshal entry: %v", marshalErr)
		}
		if writeErr := storage.WriteRecord(&buf, data); writeErr != nil {
			return status.Errorf(codes.Internal, "write record: %v", writeErr)
		}
	}
	if buf.Len() == 0 {
		return status.Errorf(codes.NotFound, "segment %s contains no entries for shard %d", name, req.ShardId)
	}

	// Stream the filtered segment bytes in chunks.
	payload := buf.Bytes()
	for len(payload) > 0 {
		n := transferChunkSize
		if n > len(payload) {
			n = len(payload)
		}
		if sendErr := stream.Send(&logengine.TransferSegmentResponse{Chunk: payload[:n]}); sendErr != nil {
			return sendErr
		}
		payload = payload[n:]
	}
	return nil
}

// FetchShardEntries returns entries for a shard with received_at >= since_unix_ns.
// Called by a replica node during catch-up on restart. CatchUp deduplicates by ID,
// so returning entries at the boundary is safe and prevents missed entries when
// multiple records share the same timestamp.
func (s *Server) FetchShardEntries(ctx context.Context, req *logengine.FetchShardEntriesRequest) (*logengine.FetchShardEntriesResponse, error) {
	all, err := s.manager.ReadSegments(s.manager.SegmentPaths())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read segments: %v", err)
	}

	var result []*logengine.LogEntry
	for _, e := range all {
		if s.totalShards > 0 && ShardID(e.Namespace, e.Service, s.totalShards) != int(req.ShardId) {
			continue
		}
		if e.ReceivedAt < req.SinceUnixNs {
			continue
		}
		result = append(result, EntryToProto(e))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReceivedAt < result[j].ReceivedAt
	})

	return &logengine.FetchShardEntriesResponse{Entries: result}, nil
}
