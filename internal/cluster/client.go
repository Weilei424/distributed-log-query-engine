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
// It retries across coordinators on FAILED_PRECONDITION (not leader).
func (c *ClusterClient) Register(ctx context.Context, grpcAddr string) ([]int, error) {
	for attempt := 0; attempt < len(c.addrs)*3; attempt++ {
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
			// This coordinator is not the leader; try the next one.
			if reconErr := c.advanceAndReconnect(); reconErr != nil {
				return nil, reconErr
			}
		case codes.Unavailable:
			// Cluster may be in election; back off and retry same address.
			time.Sleep(500 * time.Millisecond)
		default:
			return nil, err
		}
	}
	return nil, fmt.Errorf("register failed after retries: no reachable leader")
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
			// Cluster may be mid-election; back off briefly before trying the next coordinator.
			time.Sleep(500 * time.Millisecond)
			fallthrough
		case codes.FailedPrecondition:
			// Not the leader or coordinator unavailable; try the next address.
			if reconErr := c.advanceAndReconnect(); reconErr != nil {
				return reconErr
			}
		default:
			// For any other error, advance rather than retrying the same coordinator.
			if reconErr := c.advanceAndReconnect(); reconErr != nil {
				return reconErr
			}
		}
	}
	return fmt.Errorf("heartbeat failed after retries: no reachable leader")
}

// Close closes the underlying gRPC connection.
func (c *ClusterClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
