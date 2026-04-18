package metadata

// NodeStatus is the health state of a storage node.
type NodeStatus string

const (
	NodeHealthy   NodeStatus = "healthy"
	NodeUnhealthy NodeStatus = "unhealthy"
)

// NodeRecord holds metadata for a registered storage node.
type NodeRecord struct {
	ID       string
	Address  string // advertised gRPC address
	Shards   []int
	Status   NodeStatus
	LastSeen int64 // unix nanoseconds
}

// ShardRecord holds the ownership mapping for a single shard.
type ShardRecord struct {
	ShardID     int
	PrimaryNode string // node ID; empty if unowned
	ReplicaNode string // node ID; empty if no replica assigned
}

// ClusterState is the full in-memory state managed by the Raft FSM.
type ClusterState struct {
	Nodes  map[string]NodeRecord
	Shards map[int]ShardRecord
}

// clone returns a deep copy of the cluster state.
func (cs ClusterState) clone() ClusterState {
	nodes := make(map[string]NodeRecord, len(cs.Nodes))
	for k, v := range cs.Nodes {
		shards := make([]int, len(v.Shards))
		copy(shards, v.Shards)
		v.Shards = shards
		nodes[k] = v
	}
	shards := make(map[int]ShardRecord, len(cs.Shards))
	for k, v := range cs.Shards {
		shards[k] = v
	}
	return ClusterState{Nodes: nodes, Shards: shards}
}
