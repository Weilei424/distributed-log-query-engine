package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
)

// Server implements the gRPC ClusterService.
type Server struct {
	logengine.UnimplementedClusterServiceServer
	raft *raft.Raft
	fsm  *FSM
}

// NewServer creates a ClusterService gRPC server backed by the given Raft instance.
func NewServer(r *raft.Raft, fsm *FSM) *Server {
	return &Server{raft: r, fsm: fsm}
}

// applyCommand applies a command through Raft. Returns FAILED_PRECONDITION if not leader.
func (s *Server) applyCommand(cmdType CommandType, payload interface{}) error {
	if s.raft.State() != raft.Leader {
		return status.Error(codes.FailedPrecondition, "not the raft leader")
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return status.Errorf(codes.Internal, "marshal payload: %v", err)
	}
	cmd := Command{Type: cmdType, Payload: json.RawMessage(payloadBytes)}
	data, err := json.Marshal(cmd)
	if err != nil {
		return status.Errorf(codes.Internal, "marshal command: %v", err)
	}
	f := s.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return status.Error(codes.FailedPrecondition, "lost leadership during apply")
		}
		return status.Errorf(codes.Internal, "raft apply: %v", err)
	}
	if resp := f.Response(); resp != nil {
		if applyErr, ok := resp.(error); ok {
			return status.Errorf(codes.Internal, "fsm apply: %v", applyErr)
		}
	}
	return nil
}

// RegisterNode records a storage node in the cluster and assigns it unowned shards.
func (s *Server) RegisterNode(_ context.Context, req *logengine.RegisterNodeRequest) (*logengine.RegisterNodeResponse, error) {
	if req.NodeId == "" || req.GrpcAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and grpc_address are required")
	}
	if err := s.applyCommand(CmdRegisterNode, RegisterNodePayload{
		NodeID:    req.NodeId,
		Address:   req.GrpcAddress,
		NowUnixNs: time.Now().UnixNano(),
	}); err != nil {
		return nil, err
	}
	state := s.fsm.State()
	node, ok := state.Nodes[req.NodeId]
	if !ok {
		return nil, status.Error(codes.Internal, "node not found after registration")
	}
	shards := make([]int32, len(node.Shards))
	for i, sid := range node.Shards {
		shards[i] = int32(sid)
	}
	return &logengine.RegisterNodeResponse{AssignedShards: shards}, nil
}

// Heartbeat records a liveness ping from a storage node.
func (s *Server) Heartbeat(_ context.Context, req *logengine.HeartbeatRequest) (*logengine.HeartbeatResponse, error) {
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if err := s.applyCommand(CmdUpdateHeartbeat, UpdateHeartbeatPayload{
		NodeID:    req.NodeId,
		NowUnixNs: time.Now().UnixNano(),
	}); err != nil {
		return nil, err
	}
	return &logengine.HeartbeatResponse{Ok: true}, nil
}

// GetClusterState returns the current node registry and shard map.
// Any coordinator can serve this — it reads from the local FSM.
func (s *Server) GetClusterState(_ context.Context, _ *logengine.GetClusterStateRequest) (*logengine.GetClusterStateResponse, error) {
	state := s.fsm.State()

	nodes := make([]*logengine.NodeInfo, 0, len(state.Nodes))
	for _, n := range state.Nodes {
		shards := make([]int32, len(n.Shards))
		for i, sid := range n.Shards {
			shards[i] = int32(sid)
		}
		nodes = append(nodes, &logengine.NodeInfo{
			Id:             n.ID,
			Address:        n.Address,
			Shards:         shards,
			Status:         string(n.Status),
			LastSeenUnixNs: n.LastSeen,
		})
	}

	shards := make([]*logengine.ShardInfo, 0, len(state.Shards))
	for _, sr := range state.Shards {
		shards = append(shards, &logengine.ShardInfo{
			ShardId:     int32(sr.ShardID),
			PrimaryNode: sr.PrimaryNode,
		})
	}

	return &logengine.GetClusterStateResponse{Nodes: nodes, Shards: shards}, nil
}
