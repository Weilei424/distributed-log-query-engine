package cluster

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
)

// ClusterClient connects a storage node to the coordinator cluster.
// It maintains a list of coordinator addresses and round-robins on non-leader responses.
type ClusterClient struct {
	addrs  []string
	idx    int
	conn   *grpc.ClientConn
	client logengine.ClusterServiceClient
	nodeID string
}

// NewClusterClient creates a ClusterClient targeting the given coordinator addresses.
// addrs must contain at least one address.
func NewClusterClient(addrs []string, nodeID string) (*ClusterClient, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("at least one coordinator address required")
	}
	c := &ClusterClient{addrs: addrs, nodeID: nodeID}
	if err := c.connectTo(addrs[0]); err != nil {
		return nil, err
	}
	return c, nil
}

// ParseAddrs splits a comma-separated address string into a slice.
func ParseAddrs(s string) []string {
	var result []string
	for _, a := range strings.Split(s, ",") {
		a = strings.TrimSpace(a)
		if a != "" {
			result = append(result, a)
		}
	}
	return result
}

func (c *ClusterClient) connectTo(addr string) error {
	if c.conn != nil {
		c.conn.Close()
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	c.conn = conn
	c.client = logengine.NewClusterServiceClient(conn)
	return nil
}

func (c *ClusterClient) advanceAndReconnect() error {
	c.idx = (c.idx + 1) % len(c.addrs)
	return c.connectTo(c.addrs[c.idx])
}

// Register calls RegisterNode on the coordinator cluster.
// It retries until ctx expires, rotating across coordinators on both FAILED_PRECONDITION
// (not leader) and UNAVAILABLE (coordinator not ready or down).
func (c *ClusterClient) Register(ctx context.Context, grpcAddr string) ([]int, error) {
	for {
		resp, err := c.client.RegisterNode(ctx, &logengine.RegisterNodeRequest{
			NodeId:      c.nodeID,
			GrpcAddress: grpcAddr,
		})
		if err == nil {
			shards := make([]int, len(resp.AssignedShards))
			for i, s := range resp.AssignedShards {
				shards[i] = int(s)
			}
			return shards, nil
		}
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.FailedPrecondition:
			// This coordinator is not the leader; rotate to next and wait for election.
			if reconErr := c.advanceAndReconnect(); reconErr != nil {
				return nil, reconErr
			}
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
				return nil, fmt.Errorf("register: %w", ctx.Err())
			}
		case codes.Unavailable:
			// Coordinator is unreachable (not ready or down); rotate to next and back off.
			if reconErr := c.advanceAndReconnect(); reconErr != nil {
				return nil, reconErr
			}
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
				return nil, fmt.Errorf("register: %w", ctx.Err())
			}
		default:
			return nil, err
		}
	}
}

// SendHeartbeat sends a heartbeat to the coordinator leader.
// On FAILED_PRECONDITION (not leader) or UNAVAILABLE (leader down / election in progress),
// it advances to the next coordinator address and retries so that a leader failover does not
// leave the node stranded on a dead coordinator indefinitely.
func (c *ClusterClient) SendHeartbeat(ctx context.Context) error {
	for attempt := 0; attempt < len(c.addrs)*2; attempt++ {
		_, err := c.client.Heartbeat(ctx, &logengine.HeartbeatRequest{NodeId: c.nodeID})
		if err == nil {
			return nil
		}
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.Unavailable:
			// Coordinator unreachable or mid-election; back off briefly then try the next address.
			time.Sleep(500 * time.Millisecond)
			if reconErr := c.advanceAndReconnect(); reconErr != nil {
				return reconErr
			}
		case codes.FailedPrecondition:
			// This coordinator is not the leader; try the next address.
			if reconErr := c.advanceAndReconnect(); reconErr != nil {
				return reconErr
			}
		default:
			// Application-level error from the coordinator (e.g. node not found in metadata).
			// Rotating to another coordinator would not help and would mask the real cause.
			return err
		}
	}
	return fmt.Errorf("heartbeat failed after retries: no reachable leader")
}

// GetClusterState fetches the current cluster state from the coordinator.
// Any coordinator can serve this request (no leader routing needed).
func (c *ClusterClient) GetClusterState(ctx context.Context) (metadata.ClusterState, error) {
	resp, err := c.client.GetClusterState(ctx, &logengine.GetClusterStateRequest{})
	if err != nil {
		return metadata.ClusterState{}, err
	}
	return protoToClusterState(resp), nil
}

// protoToClusterState converts the proto GetClusterStateResponse to internal ClusterState.
func protoToClusterState(resp *logengine.GetClusterStateResponse) metadata.ClusterState {
	nodes := make(map[string]metadata.NodeRecord, len(resp.Nodes))
	for _, n := range resp.Nodes {
		shards := make([]int, len(n.Shards))
		for i, s := range n.Shards {
			shards[i] = int(s)
		}
		nodes[n.Id] = metadata.NodeRecord{
			ID:      n.Id,
			Address: n.Address,
			Status:  metadata.NodeStatus(n.Status),
			Shards:  shards,
		}
	}
	shards := make(map[int]metadata.ShardRecord, len(resp.Shards))
	for _, s := range resp.Shards {
		shards[int(s.ShardId)] = metadata.ShardRecord{
			ShardID:     int(s.ShardId),
			PrimaryNode: s.PrimaryNode,
			ReplicaNode: s.ReplicaNode,
		}
	}
	return metadata.ClusterState{Nodes: nodes, Shards: shards}
}

// Close closes the underlying gRPC connection.
func (c *ClusterClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
