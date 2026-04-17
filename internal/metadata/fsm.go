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
		// Already healthy: just refresh address and last seen.
		existing.Address = p.Address
		existing.LastSeen = p.NowUnixNs
		f.state.Nodes[p.NodeID] = existing
		return nil
	}

	// New node or rejoining after being marked unhealthy: claim all unowned shards.
	var unowned []int
	for id, sr := range f.state.Shards {
		if sr.PrimaryNode == "" {
			unowned = append(unowned, id)
		}
	}
	sort.Ints(unowned) // deterministic shard assignment order
	for _, shardID := range unowned {
		sr := f.state.Shards[shardID]
		sr.PrimaryNode = p.NodeID
		f.state.Shards[shardID] = sr
	}
	f.state.Nodes[p.NodeID] = NodeRecord{
		ID:       p.NodeID,
		Address:  p.Address,
		Shards:   unowned,
		Status:   NodeHealthy,
		LastSeen: p.NowUnixNs,
	}
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
	for _, shardID := range node.Shards {
		sr := f.state.Shards[shardID]
		sr.PrimaryNode = ""
		f.state.Shards[shardID] = sr
	}
	node.Status = NodeUnhealthy
	node.Shards = nil
	f.state.Nodes[p.NodeID] = node
	return nil
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
