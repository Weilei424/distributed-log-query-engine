// internal/ingest/orchestrator.go
package ingest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/cluster"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/observability"
	"github.com/Weilei424/distributed-log-query-engine/internal/replication"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

// Orchestrator handles distributed write logic: shard routing, forwarding, and replication.
// It is the single place where distributed write decisions are made.
type Orchestrator struct {
	nodeID      string
	totalShards int // 0 = local mode (no routing)
	stateReader cluster.ClusterStateReader
	manager     *storage.Manager
	idx         *index.Index
	replicator  *replication.Replicator

	mu      sync.Mutex
	clients map[string]logengine.IngestServiceClient // addr → gRPC client (cached)
}

// NewOrchestrator creates an Orchestrator for cluster-aware routing.
func NewOrchestrator(
	nodeID string,
	totalShards int,
	stateReader cluster.ClusterStateReader,
	manager *storage.Manager,
	idx *index.Index,
	replicator *replication.Replicator,
) *Orchestrator {
	return &Orchestrator{
		nodeID:      nodeID,
		totalShards: totalShards,
		stateReader: stateReader,
		manager:     manager,
		idx:         idx,
		replicator:  replicator,
		clients:     make(map[string]logengine.IngestServiceClient),
	}
}

// StateReader returns the ClusterStateReader used by this orchestrator.
// Returns nil for local-mode orchestrators.
func (o *Orchestrator) StateReader() cluster.ClusterStateReader {
	return o.stateReader
}

// newLocalOrchestrator creates an Orchestrator that always writes locally (no routing).
// Used when the node runs without a coordinator.
func newLocalOrchestrator(manager *storage.Manager, idx *index.Index) *Orchestrator {
	return &Orchestrator{
		totalShards: 0,
		manager:     manager,
		idx:         idx,
		clients:     make(map[string]logengine.IngestServiceClient),
	}
}

// HandleIngest routes an ingest request: local write if this node is the primary,
// or forward to the primary node. Validation happens before routing.
func (o *Orchestrator) HandleIngest(ctx context.Context, req *logengine.IngestRequest) (*logengine.IngestResponse, error) {
	if req.Entry == nil {
		return nil, status.Error(codes.InvalidArgument, "entry is required")
	}
	if req.Entry.Service == "" {
		return nil, status.Error(codes.InvalidArgument, "entry.service is required")
	}
	if req.Entry.Message == "" {
		return nil, status.Error(codes.InvalidArgument, "entry.message is required")
	}

	// Local mode: bypass routing entirely.
	if o.totalShards == 0 {
		return o.writeLocal(ctx, req.Entry, "")
	}

	shardID := ShardID(req.Entry.Service, o.totalShards)
	primaryID, replicaID := o.stateReader.ShardOwners(shardID)

	if primaryID == "" {
		return nil, status.Errorf(codes.Unavailable, "no primary for shard %d", shardID)
	}

	if primaryID == o.nodeID {
		return o.writeLocal(ctx, req.Entry, replicaID)
	}
	return o.forward(ctx, req, primaryID)
}

func (o *Orchestrator) writeLocal(ctx context.Context, pb *logengine.LogEntry, replicaNodeID string) (*logengine.IngestResponse, error) {
	entry := ProtoToEntry(pb)
	entry.ReceivedAt = time.Now().UnixNano()
	if entry.ID == "" {
		entry.ID = GenerateID()
	}

	segPath, err := o.manager.AppendWithPath(entry)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "append failed: %v", err)
	}
	o.idx.Add(entry, segPath)
	observability.IndexTokenCount.WithLabelValues(o.nodeID).Set(float64(o.idx.TokenCount()))

	// Enqueue async replication if a replica is known and a replicator is wired.
	if replicaNodeID != "" && o.replicator != nil && o.stateReader != nil {
		replicaAddr := o.stateReader.NodeAddress(replicaNodeID)
		if replicaAddr != "" {
			shardID := 0
			if o.totalShards > 0 {
				shardID = ShardID(entry.Service, o.totalShards)
			}
			o.replicator.Enqueue(entry, shardID, replicaAddr, observability.RequestIDFromContext(ctx))
		}
	}

	return &logengine.IngestResponse{Id: entry.ID, Ok: true}, nil
}

func (o *Orchestrator) forward(ctx context.Context, req *logengine.IngestRequest, primaryID string) (*logengine.IngestResponse, error) {
	addr := o.stateReader.NodeAddress(primaryID)
	if addr == "" {
		return nil, status.Errorf(codes.Unavailable, "primary node %q address unknown", primaryID)
	}
	client, err := o.getOrCreateClient(addr)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "connect to primary %s: %v", addr, err)
	}
	// Propagate the trace ID and mark as a forwarded hop so the primary
	// logs the same request ID and records the correct metric source.
	if id := observability.RequestIDFromContext(ctx); id != "" {
		ctx = observability.OutgoingContextWithRequestID(ctx, id)
	}
	ctx = observability.OutgoingContextWithForwarded(ctx)
	return client.Ingest(ctx, req)
}

func (o *Orchestrator) getOrCreateClient(addr string) (logengine.IngestServiceClient, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if c, ok := o.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	o.clients[addr] = logengine.NewIngestServiceClient(conn)
	return o.clients[addr], nil
}
