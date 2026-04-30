package coordinator

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
	"github.com/Weilei424/distributed-log-query-engine/internal/observability"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

const (
	cacheTTL      = 30 * time.Second
	cacheMaxItems = 256
)

// ClusterStateProvider is satisfied by *metadata.FSM.
type ClusterStateProvider interface {
	State() metadata.ClusterState
}

// FanOutExecutor fans out QueryService requests to all healthy storage nodes
// in parallel and merges the results.
type FanOutExecutor struct {
	state         ClusterStateProvider
	pool          *nodeClientPool
	nodeTimeoutMs int64
	fanOutLimit   int32
	logger        *zap.Logger
	cache         *QueryCache
}

// NewFanOutExecutor creates a FanOutExecutor.
// nodeTimeoutMs is the per-node query deadline in milliseconds.
// fanOutLimit is the limit sent to each node (overrides the client limit so
// the global merge has enough candidates to apply offset+limit correctly).
func NewFanOutExecutor(state ClusterStateProvider, nodeTimeoutMs int64, fanOutLimit int32, logger *zap.Logger) *FanOutExecutor {
	return &FanOutExecutor{
		state:         state,
		pool:          newNodeClientPool(),
		nodeTimeoutMs: nodeTimeoutMs,
		fanOutLimit:   fanOutLimit,
		logger:        logger,
		cache:         NewQueryCache(cacheTTL, cacheMaxItems),
	}
}

// Execute fans out req to all healthy nodes and returns merged results.
// The result's Partial field is true if any node failed to respond.
func (e *FanOutExecutor) Execute(ctx context.Context, req *logengine.QueryRequest) (*types.QueryResult, error) {
	start := time.Now()
	fanOutReqID := observability.NewRequestID()
	defer func() {
		observability.QueryDuration.WithLabelValues("coordinator", "fanout").Observe(time.Since(start).Seconds())
	}()

	cacheKey := CacheKey(req.QueryString, req.Namespace, req.Service, req.StartTime, req.EndTime, req.Limit, req.Offset)
	if cached, ok := e.cache.Get(cacheKey); ok {
		e.logger.Info("query cache hit", zap.String("key", cacheKey[:8]))
		return cached, nil
	}

	cs := e.state.State()

	type target struct{ id, addr string }
	var targets []target
	for id, n := range cs.Nodes {
		if n.Status == metadata.NodeHealthy && n.Address != "" {
			targets = append(targets, target{id, n.Address})
		}
	}

	ids := make([]string, len(targets))
	for i, t := range targets {
		ids[i] = t.id + "=" + t.addr
	}
	e.logger.Info("fanout targeting nodes",
		zap.String("request_id", fanOutReqID),
		zap.Int("count", len(targets)),
		zap.Strings("nodes", ids),
	)

	// Resolve the effective client limit before computing the per-node limit
	// so the default (100) is included in the candidate window calculation.
	clientLimit := req.Limit
	if clientLimit == 0 {
		clientLimit = 100
	}

	// Each node must return at least offset+clientLimit entries so the global
	// merge can satisfy the client's full window. fanOutLimit is also a floor.
	nodeLimit := max(e.fanOutLimit, req.Offset+clientLimit)
	fanReq := &logengine.QueryRequest{
		QueryString: req.QueryString,
		Namespace:   req.Namespace,
		Service:     req.Service,
		StartTime:   req.StartTime,
		EndTime:     req.EndTime,
		Limit:       nodeLimit,
		Offset:      0,
	}

	ch := make(chan nodeResult, len(targets))
	var wg sync.WaitGroup
	timeout := time.Duration(e.nodeTimeoutMs) * time.Millisecond

	for _, t := range targets {
		wg.Add(1)
		t := t
		go func() {
			defer wg.Done()
			nodeCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			nodeCtx = observability.OutgoingContextWithRequestID(nodeCtx, fanOutReqID)

			client, err := e.pool.get(t.addr)
			if err != nil {
				e.logger.Warn("fanout node error",
					zap.String("request_id", fanOutReqID),
					zap.String("target_node_id", t.id),
					zap.Error(err),
				)
				ch <- nodeResult{nodeID: t.id, err: err}
				return
			}

			resp, err := client.Query(nodeCtx, fanReq)
			if err != nil {
				if nodeCtx.Err() != nil {
					observability.FanOutTimeoutsTotal.Inc()
					e.logger.Warn("fanout node timeout",
						zap.String("request_id", fanOutReqID),
						zap.String("target_node_id", t.id),
					)
				} else {
					e.logger.Warn("fanout node error",
						zap.String("request_id", fanOutReqID),
						zap.String("target_node_id", t.id),
						zap.Error(err),
					)
				}
				ch <- nodeResult{nodeID: t.id, err: err}
				return
			}

			entries := make([]*types.LogEntry, len(resp.Entries))
			for i, pb := range resp.Entries {
				entries[i] = &types.LogEntry{
					ID:         pb.Id,
					Timestamp:  pb.Timestamp,
					ReceivedAt: pb.ReceivedAt,
					Namespace:  pb.Namespace,
					Service:    pb.Service,
					Level:      pb.Level,
					Message:    pb.Message,
					Fields:     pb.Fields,
				}
			}
			e.logger.Info("fanout node responded",
				zap.String("request_id", fanOutReqID),
				zap.String("target_node_id", t.id),
				zap.Int("entries", len(entries)),
			)
			ch <- nodeResult{nodeID: t.id, entries: entries, total: resp.Total}
		}()
	}

	wg.Wait()
	close(ch)

	// If the parent context was canceled or deadline exceeded, return that
	// error directly — treating all node failures as partial would hide it.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var parts []nodeResult
	for r := range ch {
		parts = append(parts, r)
	}

	mergeStart := time.Now()
	out := MergeResults(parts, req.Offset, clientLimit)
	if out.partial {
		observability.FanOutPartialTotal.Inc()
	}
	e.logger.Info("fanout complete",
		zap.String("request_id", fanOutReqID),
		zap.Int64("merge_ms", time.Since(mergeStart).Milliseconds()),
		zap.Int32("total", out.total),
		zap.Bool("partial", out.partial),
	)

	result := &types.QueryResult{
		Entries: out.entries,
		Total:   out.total,
		TookMs:  time.Since(start).Milliseconds(),
		Partial: out.partial,
	}
	if !result.Partial {
		e.cache.Put(cacheKey, result)
	}
	return result, nil
}
