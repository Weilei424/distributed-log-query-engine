// cmd/node/main.go
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/cluster"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/observability"
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
	"github.com/Weilei424/distributed-log-query-engine/internal/replication"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func main() {
	observability.Register(prometheus.DefaultRegisterer)

	nodeID := envOrDefault("NODE_ID", "node-local")
	nodeLogger := observability.NewLogger("node", nodeID)
	metricsAddr := envOrDefault("METRICS_ADDR", ":9090")

	dataDir := envOrDefault("DATA_DIR", "./data")
	grpcAddr := envOrDefault("GRPC_ADDR", ":50051")
	advertisedAddr := envOrDefault("NODE_GRPC_ADDR", grpcAddr)
	maxSegBytes := envInt64OrDefault("MAX_SEGMENT_BYTES", 64*1024*1024)
	coordinatorAddrs := envOrDefault("COORDINATOR_ADDRS", "")
	heartbeatInterval := time.Duration(envIntOrDefault("HEARTBEAT_INTERVAL_SECONDS", 5)) * time.Second
	totalShards := envIntOrDefault("TOTAL_SHARDS", 4)

	// Start metrics HTTP server.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsMux}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			nodeLogger.Warn("metrics server error", zap.Error(err))
		}
	}()

	manager, err := storage.NewManager(dataDir, maxSegBytes, storage.WithNodeID(nodeID))
	if err != nil {
		log.Fatalf("storage.NewManager: %v", err)
	}

	idx := index.NewIndex()
	if err := idx.RebuildFromSegments(manager.SegmentPaths(), storage.ReadSegment); err != nil {
		log.Fatalf("index rebuild: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var ingestSrv *ingest.Server
	var querySrv *query.QueryServer

	if coordinatorAddrs != "" {
		addrs := cluster.ParseAddrs(coordinatorAddrs)
		clusterClient, err := cluster.NewClusterClient(addrs, nodeID)
		if err != nil {
			nodeLogger.Warn("cluster client init failed, starting in local mode", zap.Error(err))
			ingestSrv = ingest.NewLocalServer(manager, idx)
		} else {
			defer clusterClient.Close()

			// Register with the coordinator.
			regCtx, regCancel := context.WithTimeout(ctx, 30*time.Second)
			shards, err := clusterClient.Register(regCtx, advertisedAddr)
			regCancel()
			if err != nil {
				// Registration failed — start in local-only mode with no cluster membership.
				// The node will NOT heartbeat and will NOT appear healthy in cluster metadata.
				// Restart the node once the coordinator is reachable to enable distributed mode.
				nodeLogger.Error("cluster register failed; starting in local-only mode (restart required to join cluster)", zap.Error(err))
				ingestSrv = ingest.NewLocalServer(manager, idx)
			} else {
				nodeLogger.Info("registered with coordinator", zap.Ints("shards", shards))

				// Start state cache (initial refresh before accepting traffic).
				stateCache := cluster.NewStateCache(clusterClient, 5*time.Second, nodeLogger)
				stateCache.Refresh(ctx)
				go stateCache.Run(ctx)

				// Run catch-up for shards this node owns as replica.
				if catchUpState, err := clusterClient.GetClusterState(ctx); err != nil {
					nodeLogger.Error("catch-up: get cluster state failed, skipping catch-up", zap.Error(err))
				} else {
					ingest.CatchUp(ctx, nodeID, totalShards, catchUpState, manager, idx, nodeLogger)
				}

				// Build orchestrator.
				repl := replication.NewReplicator(totalShards, nodeID, nodeLogger)
				defer repl.Stop()
				orch := ingest.NewOrchestrator(nodeID, totalShards, stateCache, manager, idx, repl)
				ingestSrv = ingest.NewServer(orch, nodeID, totalShards, manager, idx)

				// Start heartbeat.
				sender := cluster.NewHeartbeatSender(clusterClient, heartbeatInterval, nodeID, nodeLogger)
				go sender.Run(ctx)
			}
		}
	} else {
		ingestSrv = ingest.NewLocalServer(manager, idx)
	}

	ingestSrv.SetLogger(nodeLogger)

	querySrv = query.NewQueryServer(query.NewLocalExecutor(idx, manager), nodeID, nodeLogger)

	grpcSrv := grpc.NewServer()
	logengine.RegisterIngestServiceServer(grpcSrv, ingestSrv)
	logengine.RegisterQueryServiceServer(grpcSrv, querySrv)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("net.Listen %s: %v", grpcAddr, err)
	}

	nodeLogger.Info("node started", zap.String("addr", grpcAddr), zap.String("data", dataDir))

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()

	<-stop
	nodeLogger.Info("shutting down")
	cancel()
	grpcSrv.GracefulStop()
	if err := manager.Close(); err != nil {
		log.Printf("manager close: %v", err)
	}
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	if err := metricsSrv.Shutdown(shutCtx); err != nil {
		nodeLogger.Warn("metrics server shutdown error", zap.Error(err))
	}
	nodeLogger.Info("node stopped")
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
