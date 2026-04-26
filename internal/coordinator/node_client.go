package coordinator

import (
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
)

// nodeClientPool caches gRPC QueryServiceClient connections keyed by address.
// Connections are created lazily and never evicted; cluster membership is stable
// within a query lifetime.
type nodeClientPool struct {
	mu      sync.Mutex
	clients map[string]logengine.QueryServiceClient
}

func newNodeClientPool() *nodeClientPool {
	return &nodeClientPool{clients: make(map[string]logengine.QueryServiceClient)}
}

func (p *nodeClientPool) get(addr string) (logengine.QueryServiceClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	c := logengine.NewQueryServiceClient(conn)
	p.clients[addr] = c
	return c, nil
}
