// internal/replication/replicator.go
package replication

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/observability"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

const channelCapacity = 256

type replicaJob struct {
	entry   *types.LogEntry
	shardID int
}

// Replicator asynchronously delivers log entries to replica nodes via ReplicateEntry RPC.
// It maintains one buffered channel and one drain goroutine per target address.
type Replicator struct {
	totalShards int
	nodeID      string
	logger      *zap.Logger

	mu       sync.Mutex
	channels map[string]chan replicaJob
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewReplicator creates a Replicator. Call Stop to shut down gracefully.
func NewReplicator(totalShards int, nodeID string, logger *zap.Logger) *Replicator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Replicator{
		totalShards: totalShards,
		nodeID:      nodeID,
		logger:      logger,
		channels:    make(map[string]chan replicaJob),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Enqueue schedules an entry for async replication to addr.
// Non-blocking: if the channel is full the entry is dropped and logged.
func (r *Replicator) Enqueue(entry *types.LogEntry, shardID int, addr string) {
	ch := r.getOrCreateChannel(addr)
	select {
	case ch <- replicaJob{entry: entry, shardID: shardID}:
		observability.ReplicationLagEntries.WithLabelValues(r.nodeID).Set(float64(len(ch)))
	default:
		r.logger.Warn("replication channel full, dropping entry",
			zap.String("addr", addr),
			zap.String("entry_id", entry.ID),
		)
	}
}

func (r *Replicator) getOrCreateChannel(addr string) chan replicaJob {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ch, ok := r.channels[addr]; ok {
		return ch
	}
	ch := make(chan replicaJob, channelCapacity)
	r.channels[addr] = ch
	r.wg.Add(1)
	go r.drain(addr, ch)
	return ch
}

func (r *Replicator) drain(addr string, ch chan replicaJob) {
	defer r.wg.Done()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		r.logger.Error("replicator connect failed", zap.String("addr", addr), zap.Error(err))
		return
	}
	defer conn.Close()
	client := logengine.NewIngestServiceClient(conn)

	for {
		select {
		case <-r.ctx.Done():
			// Drain remaining with a short deadline.
			deadline := time.Now().Add(2 * time.Second)
			for {
				select {
				case job := <-ch:
					ctx, cancel := context.WithDeadline(context.Background(), deadline)
					r.send(ctx, client, job)
					cancel()
				default:
					return
				}
			}
		case job := <-ch:
			r.send(r.ctx, client, job)
		}
	}
}

func (r *Replicator) send(ctx context.Context, client logengine.IngestServiceClient, job replicaJob) {
	pb := entryToProto(job.entry)
	_, err := client.ReplicateEntry(ctx, &logengine.ReplicateEntryRequest{
		Entry:   pb,
		ShardId: int32(job.shardID),
	})
	if err != nil {
		r.logger.Warn("ReplicateEntry failed", zap.String("entry_id", job.entry.ID), zap.Error(err))
	}
}

// Stop signals all drain goroutines to finish in-flight entries and exit.
func (r *Replicator) Stop() {
	r.cancel()
	r.wg.Wait()
}

func entryToProto(e *types.LogEntry) *logengine.LogEntry {
	return &logengine.LogEntry{
		Id:         e.ID,
		Timestamp:  e.Timestamp,
		ReceivedAt: e.ReceivedAt,
		Service:    e.Service,
		Level:      e.Level,
		Message:    e.Message,
		Fields:     e.Fields,
	}
}
