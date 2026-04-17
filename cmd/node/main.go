package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/cluster"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func main() {
	nodeID := envOrDefault("NODE_ID", "node-local")
	dataDir := envOrDefault("DATA_DIR", "./data")
	grpcAddr := envOrDefault("GRPC_ADDR", ":50051")
	// NODE_GRPC_ADDR is the address advertised to the coordinator cluster — must be routable
	// from other nodes (e.g. "node-1:50051" in Docker Compose). Defaults to grpcAddr for local dev.
	advertisedAddr := envOrDefault("NODE_GRPC_ADDR", grpcAddr)
	maxSegBytes := envInt64OrDefault("MAX_SEGMENT_BYTES", 64*1024*1024)
	coordinatorAddrs := envOrDefault("COORDINATOR_ADDRS", "")
	heartbeatInterval := time.Duration(envIntOrDefault("HEARTBEAT_INTERVAL_SECONDS", 5)) * time.Second

	manager, err := storage.NewManager(dataDir, maxSegBytes)
	if err != nil {
		log.Fatalf("storage.NewManager: %v", err)
	}

	idx := index.NewIndex()
	if err := idx.RebuildFromSegments(manager.SegmentPaths(), storage.ReadSegment); err != nil {
		log.Fatalf("index rebuild: %v", err)
	}

	ingestSrv := ingest.NewServer(manager, idx)
	querySrv := query.NewQueryServer(query.NewLocalExecutor(idx, manager))

	grpcSrv := grpc.NewServer()
	logengine.RegisterIngestServiceServer(grpcSrv, ingestSrv)
	logengine.RegisterQueryServiceServer(grpcSrv, querySrv)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("net.Listen %s: %v", grpcAddr, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register with the coordinator cluster if configured.
	// A background goroutine retries registration every heartbeatInterval until it succeeds,
	// then transitions into the heartbeat loop. Both exit when ctx is cancelled.
	if coordinatorAddrs != "" {
		addrs := cluster.ParseAddrs(coordinatorAddrs)
		clusterClient, err := cluster.NewClusterClient(addrs, nodeID)
		if err != nil {
			log.Printf("cluster client init: %v (continuing without cluster registration)", err)
		} else {
			defer clusterClient.Close()
			go func() {
				for {
					regCtx, regCancel := context.WithTimeout(ctx, 30*time.Second)
					shards, err := clusterClient.Register(regCtx, advertisedAddr)
					regCancel()
					if err == nil {
						fmt.Printf("registered with coordinator: shards=%v\n", shards)
						sender := cluster.NewHeartbeatSender(clusterClient, heartbeatInterval)
						sender.Run(ctx) // blocks until ctx cancelled
						return
					}
					log.Printf("cluster register: %v (retrying in %s)", err, heartbeatInterval)
					select {
					case <-ctx.Done():
						return
					case <-time.After(heartbeatInterval):
					}
				}
			}()
		}
	}

	fmt.Printf("node started: id=%s addr=%s data=%s\n", nodeID, grpcAddr, dataDir)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()

	<-stop
	fmt.Println("shutting down...")
	cancel()
	grpcSrv.GracefulStop()
	if err := manager.Close(); err != nil {
		log.Printf("manager close: %v", err)
	}
	fmt.Println("node stopped")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt64OrDefault(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
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
