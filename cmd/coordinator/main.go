package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/coordinator"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
)

func main() {
	nodeID := envOrDefault("RAFT_NODE_ID", "coordinator-local")
	bindAddr := envOrDefault("RAFT_BIND_ADDR", "127.0.0.1:7000")
	dataDir := envOrDefault("RAFT_DATA_DIR", "./raft-data")
	peersStr := envOrDefault("RAFT_PEERS", "")
	grpcAddr := envOrDefault("GRPC_ADDR", ":9000")
	httpAddr := envOrDefault("HTTP_ADDR", ":8080")
	totalShards := envIntOrDefault("TOTAL_SHARDS", 16)
	heartbeatInterval := time.Duration(envIntOrDefault("HEARTBEAT_INTERVAL_SECONDS", 5)) * time.Second
	heartbeatTimeout := time.Duration(envIntOrDefault("HEARTBEAT_TIMEOUT_SECONDS", 15)) * time.Second
	nodeQueryTimeoutMs := int64(envIntOrDefault("NODE_QUERY_TIMEOUT_MS", 5000))
	fanOutLimit := int32(envIntOrDefault("FAN_OUT_LIMIT", 1000))

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	// Raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		log.Fatalf("raftboltdb: %v", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		log.Fatalf("snapshot store: %v", err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		log.Fatalf("resolve bind addr: %v", err)
	}
	transport, err := raft.NewTCPTransport(bindAddr, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("tcp transport: %v", err)
	}

	fsm := metadata.NewFSM(totalShards)
	r, err := raft.NewRaft(config, fsm, boltStore, boltStore, snapshotStore, transport)
	if err != nil {
		log.Fatalf("raft.NewRaft: %v", err)
	}

	// Bootstrap on first start only
	hasState, err := raft.HasExistingState(boltStore, boltStore, snapshotStore)
	if err != nil {
		log.Fatalf("check existing state: %v", err)
	}
	if !hasState {
		peers := parsePeers(peersStr, nodeID, bindAddr)
		cfg := raft.Configuration{Servers: peers}
		if f := r.BootstrapCluster(cfg); f.Error() != nil {
			log.Fatalf("bootstrap: %v", f.Error())
		}
	}

	// gRPC server
	grpcSrv := grpc.NewServer()
	logengine.RegisterClusterServiceServer(grpcSrv, metadata.NewServer(r, fsm))
	fanOutExec := coordinator.NewFanOutExecutor(fsm, nodeQueryTimeoutMs, fanOutLimit)
	logengine.RegisterQueryServiceServer(grpcSrv, coordinator.NewFanOutQueryServer(fanOutExec))
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("grpc listen: %v", err)
	}

	// HTTP /status
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		state := fsm.State()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(state); err != nil {
			log.Printf("status encode: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())

	httpSrv := &http.Server{Addr: httpAddr, Handler: mux}

	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http serve: %v", err)
		}
	}()
	go metadata.StartLivenessChecker(ctx, r, fsm, heartbeatInterval, heartbeatTimeout)

	fmt.Printf("coordinator started: id=%s raft=%s grpc=%s http=%s shards=%d node_query_timeout_ms=%d fan_out_limit=%d\n",
		nodeID, bindAddr, grpcAddr, httpAddr, totalShards, nodeQueryTimeoutMs, fanOutLimit)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	fmt.Println("shutting down...")
	cancel()
	grpcSrv.GracefulStop()
	if err := httpSrv.Shutdown(context.Background()); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	if err := r.Shutdown().Error(); err != nil {
		log.Printf("raft shutdown: %v", err)
	}
	fmt.Println("coordinator stopped")
}

func parsePeers(peersStr, selfID, selfAddr string) []raft.Server {
	if peersStr == "" {
		return []raft.Server{{
			ID:      raft.ServerID(selfID),
			Address: raft.ServerAddress(selfAddr),
		}}
	}
	var servers []raft.Server
	for _, pair := range strings.Split(peersStr, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) != 2 {
			continue
		}
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(strings.TrimSpace(parts[0])),
			Address: raft.ServerAddress(strings.TrimSpace(parts[1])),
		})
	}
	return servers
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
