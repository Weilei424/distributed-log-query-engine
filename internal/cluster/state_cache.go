// internal/cluster/state_cache.go
package cluster

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
)

// ClusterStateReader provides routing information derived from cluster state.
// The orchestrator uses this to make routing and replication decisions.
type ClusterStateReader interface {
	// ShardOwners returns the primary and replica node IDs for the given shard.
	// Returns empty strings if the shard is unknown or unowned.
	ShardOwners(shardID int) (primaryNodeID, replicaNodeID string)
	// NodeAddress returns the gRPC address of the given node ID.
	// Returns empty string if the node is unknown.
	NodeAddress(nodeID string) string
}

// StateCache polls the coordinator for cluster state and serves routing
// queries from the cached result. Storage nodes use this to make routing
// decisions without blocking on a live coordinator RPC during writes.
type StateCache struct {
	mu       sync.RWMutex
	state    metadata.ClusterState
	client   *ClusterClient
	interval time.Duration
	logger   *zap.Logger
}

// NewStateCache creates a StateCache backed by the given ClusterClient.
func NewStateCache(client *ClusterClient, interval time.Duration, logger *zap.Logger) *StateCache {
	return &StateCache{
		client:   client,
		interval: interval,
		logger:   logger,
		state: metadata.ClusterState{
			Nodes:  make(map[string]metadata.NodeRecord),
			Shards: make(map[int]metadata.ShardRecord),
		},
	}
}

// Refresh fetches the current state immediately. Call once before accepting traffic
// to ensure the cache is populated before the first routing decision.
func (c *StateCache) Refresh(ctx context.Context) {
	c.refresh(ctx)
}

// Run starts the background polling loop. Blocks until ctx is cancelled.
func (c *StateCache) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.refresh(ctx)
		}
	}
}

func (c *StateCache) refresh(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	state, err := c.client.GetClusterState(rctx)
	if err != nil {
		c.logger.Warn("state_cache: refresh failed, retaining last known state", zap.Error(err))
		return
	}
	c.mu.Lock()
	c.state = state
	c.mu.Unlock()
}

// ShardOwners implements ClusterStateReader.
func (c *StateCache) ShardOwners(shardID int) (primaryNodeID, replicaNodeID string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	sr, ok := c.state.Shards[shardID]
	if !ok {
		return "", ""
	}
	return sr.PrimaryNode, sr.ReplicaNode
}

// NodeAddress implements ClusterStateReader.
func (c *StateCache) NodeAddress(nodeID string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.state.Nodes[nodeID]
	if !ok {
		return ""
	}
	return n.Address
}
