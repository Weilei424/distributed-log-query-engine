package coordinator

import (
	"context"
	"log"
	"sync"
	"time"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
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
}

// NewFanOutExecutor creates a FanOutExecutor.
// nodeTimeoutMs is the per-node query deadline in milliseconds.
// fanOutLimit is the limit sent to each node (overrides the client limit so
// the global merge has enough candidates to apply offset+limit correctly).
func NewFanOutExecutor(state ClusterStateProvider, nodeTimeoutMs int64, fanOutLimit int32) *FanOutExecutor {
	return &FanOutExecutor{
		state:         state,
		pool:          newNodeClientPool(),
		nodeTimeoutMs: nodeTimeoutMs,
		fanOutLimit:   fanOutLimit,
	}
}

// Execute fans out req to all healthy nodes and returns merged results.
// The result's Partial field is true if any node failed to respond.
func (e *FanOutExecutor) Execute(ctx context.Context, req *logengine.QueryRequest) (*types.QueryResult, error) {
	start := time.Now()

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
	log.Printf("fanout: targeting %d nodes: %v", len(targets), ids)

	// Each node must return at least offset+limit entries so the global merge
	// can satisfy the client's full window. fanOutLimit is also a floor to
	// avoid sending overly small limits when offset+limit is tiny.
	nodeLimit := max(e.fanOutLimit, req.Offset+req.Limit)
	fanReq := &logengine.QueryRequest{
		Keyword:   req.Keyword,
		Service:   req.Service,
		StartTime: req.StartTime,
		EndTime:   req.EndTime,
		Limit:     nodeLimit,
		Offset:    0,
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

			client, err := e.pool.get(t.addr)
			if err != nil {
				log.Printf("fanout: node %s error: %v", t.id, err)
				ch <- nodeResult{nodeID: t.id, err: err}
				return
			}

			resp, err := client.Query(nodeCtx, fanReq)
			if err != nil {
				if nodeCtx.Err() != nil {
					log.Printf("fanout: node %s timed out", t.id)
				} else {
					log.Printf("fanout: node %s error: %v", t.id, err)
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
					Service:    pb.Service,
					Level:      pb.Level,
					Message:    pb.Message,
					Fields:     pb.Fields,
				}
			}
			log.Printf("fanout: node %s responded: %d entries", t.id, len(entries))
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

	// Apply default limit when client sends 0.
	clientLimit := req.Limit
	if clientLimit == 0 {
		clientLimit = 100
	}

	mergeStart := time.Now()
	out := MergeResults(parts, req.Offset, clientLimit)
	log.Printf("fanout: merge took %dms, total=%d, partial=%v",
		time.Since(mergeStart).Milliseconds(), out.total, out.partial)

	return &types.QueryResult{
		Entries: out.entries,
		Total:   out.total,
		TookMs:  time.Since(start).Milliseconds(),
		Partial: out.partial,
	}, nil
}
