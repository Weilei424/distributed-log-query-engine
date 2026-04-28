package cluster

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/Weilei424/distributed-log-query-engine/internal/observability"
)

// Beater abstracts the heartbeat send operation for testability.
type Beater interface {
	SendHeartbeat(ctx context.Context) error
}

// HeartbeatSender sends periodic heartbeats to the coordinator.
type HeartbeatSender struct {
	beater   Beater
	interval time.Duration
	nodeID   string
	logger   *zap.Logger
}

// NewHeartbeatSender creates a HeartbeatSender with the given send interval.
func NewHeartbeatSender(b Beater, interval time.Duration, nodeID string, logger *zap.Logger) *HeartbeatSender {
	return &HeartbeatSender{beater: b, interval: interval, nodeID: nodeID, logger: logger}
}

// Run sends heartbeats at the configured interval until ctx is cancelled.
func (h *HeartbeatSender) Run(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := h.beater.SendHeartbeat(ctx); err != nil {
				h.logger.Warn("heartbeat failed", zap.Error(err))
				observability.NodeHealthStatus.WithLabelValues(h.nodeID).Set(0)
			} else {
				observability.NodeHealthStatus.WithLabelValues(h.nodeID).Set(1)
			}
		}
	}
}
