package metadata

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/hashicorp/raft"
)

// CommandType identifies the type of a Raft log command.
type CommandType string

const (
	CmdRegisterNode    CommandType = "register_node"
	CmdUpdateHeartbeat CommandType = "update_heartbeat"
	CmdMarkUnhealthy   CommandType = "mark_unhealthy"
)

// Command is the envelope written to the Raft log.
type Command struct {
	Type    CommandType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// RegisterNodePayload is the payload for CmdRegisterNode.
type RegisterNodePayload struct {
	NodeID    string `json:"node_id"`
	Address   string `json:"address"`
	NowUnixNs int64  `json:"now_unix_ns"`
}

// UpdateHeartbeatPayload is the payload for CmdUpdateHeartbeat.
type UpdateHeartbeatPayload struct {
	NodeID    string `json:"node_id"`
	NowUnixNs int64  `json:"now_unix_ns"`
}

// MarkUnhealthyPayload is the payload for CmdMarkUnhealthy.
type MarkUnhealthyPayload struct {
	NodeID string `json:"node_id"`
}

// FSM is the Raft finite state machine managing cluster metadata.
type FSM struct {
	mu          sync.RWMutex
	state       ClusterState
	totalShards int
}

// NewFSM creates an FSM with all shards unowned.
func NewFSM(totalShards int) *FSM {
	shards := make(map[int]ShardRecord, totalShards)
	for i := 0; i < totalShards; i++ {
		shards[i] = ShardRecord{ShardID: i}
	}
	return &FSM{
		state: ClusterState{
			Nodes:  make(map[string]NodeRecord),
			Shards: shards,
		},
		totalShards: totalShards,
	}
}

// Apply implements raft.FSM. It dispatches to the appropriate handler.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CmdRegisterNode:
		var p RegisterNodePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return err
		}
		return f.applyRegisterNode(p)
	case CmdUpdateHeartbeat:
		var p UpdateHeartbeatPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return err
		}
		return f.applyUpdateHeartbeat(p)
	case CmdMarkUnhealthy:
		var p MarkUnhealthyPayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return err
		}
		return f.applyMarkUnhealthy(p)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

func (f *FSM) applyRegisterNode(p RegisterNodePayload) error {
	existing, ok := f.state.Nodes[p.NodeID]
	if ok && existing.Status == NodeHealthy {
		// Already healthy: refresh address and last seen, no shard change.
		existing.Address = p.Address
		existing.LastSeen = p.NowUnixNs
		f.state.Nodes[p.NodeID] = existing
		return nil
	}

	// New node or rejoining after being marked unhealthy.
	// Register with no shards; rebalancePrimary distributes them.
	f.state.Nodes[p.NodeID] = NodeRecord{
		ID:       p.NodeID,
		Address:  p.Address,
		Status:   NodeHealthy,
		LastSeen: p.NowUnixNs,
	}

	f.rebalancePrimary()
	f.assignReplicas()
	return nil
}

func (f *FSM) applyUpdateHeartbeat(p UpdateHeartbeatPayload) error {
	node, ok := f.state.Nodes[p.NodeID]
	if !ok {
		return fmt.Errorf("node not found: %s", p.NodeID)
	}
	node.LastSeen = p.NowUnixNs
	f.state.Nodes[p.NodeID] = node
	return nil
}

func (f *FSM) applyMarkUnhealthy(p MarkUnhealthyPayload) error {
	node, ok := f.state.Nodes[p.NodeID]
	if !ok {
		return fmt.Errorf("node not found: %s", p.NodeID)
	}
	// Release primary ownership.
	for _, shardID := range node.Shards {
		sr := f.state.Shards[shardID]
		sr.PrimaryNode = ""
		f.state.Shards[shardID] = sr
	}
	// Clear replica slots where this node was the replica.
	for id, sr := range f.state.Shards {
		if sr.ReplicaNode == p.NodeID {
			sr.ReplicaNode = ""
			f.state.Shards[id] = sr
		}
	}
	node.Status = NodeUnhealthy
	node.Shards = nil
	f.state.Nodes[p.NodeID] = node

	// Redistribute primary ownership among remaining healthy nodes.
	// Must be called after the node is set to NodeUnhealthy so the dying
	// node is excluded from the healthy pool during redistribution.
	f.rebalancePrimary()

	// Reassign replica slots among remaining healthy nodes.
	f.assignReplicas()
	return nil
}

// rebalancePrimary distributes all shards round-robin across healthy nodes.
// Called whenever a node joins. Does NOT migrate physical data — only metadata.
func (f *FSM) rebalancePrimary() {
	var healthyIDs []string
	for id, n := range f.state.Nodes {
		if n.Status == NodeHealthy {
			healthyIDs = append(healthyIDs, id)
		}
	}
	if len(healthyIDs) == 0 {
		return
	}
	sort.Strings(healthyIDs)

	var allShards []int
	for id := range f.state.Shards {
		allShards = append(allShards, id)
	}
	sort.Ints(allShards)

	// Clear existing primary assignments and node shard lists.
	for id, sr := range f.state.Shards {
		sr.PrimaryNode = ""
		f.state.Shards[id] = sr
	}
	for id, n := range f.state.Nodes {
		n.Shards = nil
		f.state.Nodes[id] = n
	}

	// Assign round-robin.
	for i, shardID := range allShards {
		nodeID := healthyIDs[i%len(healthyIDs)]
		sr := f.state.Shards[shardID]
		sr.PrimaryNode = nodeID
		f.state.Shards[shardID] = sr
		n := f.state.Nodes[nodeID]
		n.Shards = append(n.Shards, shardID)
		f.state.Nodes[nodeID] = n
	}

	for id, n := range f.state.Nodes {
		sort.Ints(n.Shards)
		f.state.Nodes[id] = n
	}
}

// assignReplicas assigns the first healthy non-primary node as the replica for each shard.
// Clears all existing replica assignments before reassigning.
func (f *FSM) assignReplicas() {
	var healthyIDs []string
	for id, n := range f.state.Nodes {
		if n.Status == NodeHealthy {
			healthyIDs = append(healthyIDs, id)
		}
	}
	sort.Strings(healthyIDs)

	for id, sr := range f.state.Shards {
		sr.ReplicaNode = ""
		for _, nodeID := range healthyIDs {
			if nodeID != sr.PrimaryNode {
				sr.ReplicaNode = nodeID
				break
			}
		}
		f.state.Shards[id] = sr
	}
}

// State returns a deep copy of the current cluster state.
func (f *FSM) State() ClusterState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.clone()
}

// fsmSnapshot holds a snapshot of cluster state.
type fsmSnapshot struct {
	state ClusterState
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.state)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// Snapshot implements raft.FSM.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &fsmSnapshot{state: f.state.clone()}, nil
}

// Restore implements raft.FSM.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var state ClusterState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return err
	}
	f.mu.Lock()
	f.state = state
	f.mu.Unlock()
	return nil
}
