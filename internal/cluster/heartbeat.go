package cluster

import (
	"context"
	"log"
	"time"
)

// Beater abstracts the heartbeat send operation for testability.
type Beater interface {
	SendHeartbeat(ctx context.Context) error
}

// HeartbeatSender sends periodic heartbeats to the coordinator.
type HeartbeatSender struct {
	beater   Beater
	interval time.Duration
}

// NewHeartbeatSender creates a HeartbeatSender with the given send interval.
func NewHeartbeatSender(b Beater, interval time.Duration) *HeartbeatSender {
	return &HeartbeatSender{beater: b, interval: interval}
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
				log.Printf("heartbeat: %v", err)
			}
		}
	}
}
