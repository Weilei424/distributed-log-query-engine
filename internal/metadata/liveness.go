package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"

	"github.com/Weilei424/distributed-log-query-engine/internal/observability"
)

// StartLivenessChecker monitors node heartbeats and marks stale nodes unhealthy.
// It only applies Raft commands when this coordinator is the leader.
// Call as a goroutine; it exits when ctx is cancelled.
func StartLivenessChecker(ctx context.Context, r *raft.Raft, fsm *FSM, interval, timeout time.Duration, logger *zap.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.State() == raft.Leader {
				checkLiveness(r, fsm, timeout, logger)
			}
			emitNodeHealthMetrics(fsm)
		}
	}
}

func checkLiveness(r *raft.Raft, fsm *FSM, timeout time.Duration, logger *zap.Logger) {
	state := fsm.State()
	now := time.Now().UnixNano()
	timeoutNs := timeout.Nanoseconds()
	for _, node := range state.Nodes {
		if node.Status == NodeUnhealthy {
			continue
		}
		if now-node.LastSeen > timeoutNs {
			if err := applyMarkUnhealthy(r, node.ID); err != nil {
				logger.Error("liveness: failed to mark node unhealthy",
					zap.String("target_node_id", node.ID),
					zap.Error(err),
				)
			} else {
				logger.Info("liveness: marked node unhealthy",
					zap.String("target_node_id", node.ID),
					zap.Float64("last_seen_seconds_ago", float64(now-node.LastSeen)/1e9),
				)
			}
		}
	}
}

// emitNodeHealthMetrics publishes NodeHealthStatus for every node the FSM knows about.
// Runs on every coordinator (not just the leader) so the metrics endpoint reflects
// cluster health even when a node's own metrics endpoint has disappeared.
func emitNodeHealthMetrics(fsm *FSM) {
	for _, node := range fsm.State().Nodes {
		val := float64(0)
		if node.Status == NodeHealthy {
			val = 1
		}
		observability.NodeHealthStatus.WithLabelValues(node.ID).Set(val)
	}
}

func applyMarkUnhealthy(r *raft.Raft, nodeID string) error {
	payload, err := json.Marshal(MarkUnhealthyPayload{NodeID: nodeID})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	cmd := Command{Type: CmdMarkUnhealthy, Payload: json.RawMessage(payload)}
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}
	f := r.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		return err
	}
	if resp := f.Response(); resp != nil {
		if applyErr, ok := resp.(error); ok {
			return applyErr
		}
	}
	return nil
}
