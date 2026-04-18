// cmd/node/main.go
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
	"google.golang.org/grpc/credentials/insecure"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/cluster"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
	"github.com/Weilei424/distributed-log-query-engine/internal/replication"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func main() {
	nodeID := envOrDefault("NODE_ID", "node-local")
	dataDir := envOrDefault("DATA_DIR", "./data")
	grpcAddr := envOrDefault("GRPC_ADDR", ":50051")
	advertisedAddr := envOrDefault("NODE_GRPC_ADDR", grpcAddr)
	maxSegBytes := envInt64OrDefault("MAX_SEGMENT_BYTES", 64*1024*1024)
	coordinatorAddrs := envOrDefault("COORDINATOR_ADDRS", "")
	heartbeatInterval := time.Duration(envIntOrDefault("HEARTBEAT_INTERVAL_SECONDS", 5)) * time.Second
	totalShards := envIntOrDefault("TOTAL_SHARDS", 4)

	manager, err := storage.NewManager(dataDir, maxSegBytes)
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
			log.Printf("cluster client init: %v (starting in local mode)", err)
			ingestSrv = ingest.NewLocalServer(manager, idx)
		} else {
			defer clusterClient.Close()

			// Register with the coordinator.
			regCtx, regCancel := context.WithTimeout(ctx, 30*time.Second)
			shards, err := clusterClient.Register(regCtx, advertisedAddr)
			regCancel()
			if err != nil {
				log.Printf("cluster register: %v (starting in degraded mode; will retry)", err)
				// Retry in background.
				go func() {
					for {
						select {
						case <-ctx.Done():
							return
						case <-time.After(heartbeatInterval):
						}
						regCtx, regCancel := context.WithTimeout(ctx, 30*time.Second)
						shards, err := clusterClient.Register(regCtx, advertisedAddr)
						regCancel()
						if err != nil {
							log.Printf("cluster register retry: %v", err)
							continue
						}
						fmt.Printf("registered with coordinator (retry): shards=%v\n", shards)
						sender := cluster.NewHeartbeatSender(clusterClient, heartbeatInterval)
						sender.Run(ctx)
						return
					}
				}()
				ingestSrv = ingest.NewLocalServer(manager, idx)
			} else {
				fmt.Printf("registered with coordinator: shards=%v\n", shards)

				// Start state cache (initial refresh before accepting traffic).
				stateCache := cluster.NewStateCache(clusterClient, 5*time.Second)
				stateCache.Refresh(ctx)
				go stateCache.Run(ctx)

				// Run catch-up for shards this node owns as replica.
				runCatchUp(ctx, nodeID, totalShards, clusterClient, stateCache, manager, idx)

				// Build orchestrator.
				repl := replication.NewReplicator(totalShards)
				defer repl.Stop()
				orch := ingest.NewOrchestrator(nodeID, totalShards, stateCache, manager, idx, repl)
				ingestSrv = ingest.NewServer(orch, nodeID, totalShards, manager, idx)

				// Start heartbeat.
				sender := cluster.NewHeartbeatSender(clusterClient, heartbeatInterval)
				go sender.Run(ctx)
			}
		}
	} else {
		ingestSrv = ingest.NewLocalServer(manager, idx)
	}

	querySrv = query.NewQueryServer(query.NewLocalExecutor(idx, manager))

	grpcSrv := grpc.NewServer()
	logengine.RegisterIngestServiceServer(grpcSrv, ingestSrv)
	logengine.RegisterQueryServiceServer(grpcSrv, querySrv)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("net.Listen %s: %v", grpcAddr, err)
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

// runCatchUp fetches entries from the primary for each shard this node replicates.
// Runs synchronously before the server starts accepting traffic.
// Skips silently if the primary is unreachable.
func runCatchUp(ctx context.Context, nodeID string, totalShards int, clusterClient *cluster.ClusterClient, stateReader *cluster.StateCache, manager *storage.Manager, idx *index.Index) {
	state, err := clusterClient.GetClusterState(ctx)
	if err != nil {
		log.Printf("catch-up: get cluster state failed: %v (skipping)", err)
		return
	}

	for shardID, sr := range state.Shards {
		if sr.ReplicaNode != nodeID {
			continue
		}
		primaryAddr := ""
		if n, ok := state.Nodes[sr.PrimaryNode]; ok {
			primaryAddr = n.Address
		}
		if primaryAddr == "" {
			log.Printf("catch-up: shard %d primary address unknown, skipping", shardID)
			continue
		}

		sinceNs := latestReceivedAtForShard(shardID, totalShards, manager)

		conn, err := grpc.NewClient(primaryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("catch-up: dial primary %s for shard %d: %v (skipping)", primaryAddr, shardID, err)
			continue
		}

		fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		resp, err := logengine.NewIngestServiceClient(conn).FetchShardEntries(fetchCtx, &logengine.FetchShardEntriesRequest{
			ShardId:     int32(shardID),
			SinceUnixNs: sinceNs,
		})
		cancel()
		conn.Close()

		if err != nil {
			log.Printf("catch-up: FetchShardEntries shard %d from %s: %v (skipping)", shardID, primaryAddr, err)
			continue
		}

		for _, pb := range resp.Entries {
			e := ingest.ProtoToEntry(pb)
			segPath, err := manager.AppendWithPath(e)
			if err != nil {
				log.Printf("catch-up: append entry %s: %v", e.ID, err)
				continue
			}
			idx.Add(e, segPath)
		}
		log.Printf("catch-up: shard %d caught up %d entries from %s", shardID, len(resp.Entries), primaryAddr)
	}
}

// latestReceivedAtForShard returns the largest received_at nanosecond timestamp
// among all local entries that belong to the given shard. Returns 0 if none.
func latestReceivedAtForShard(shardID, totalShards int, manager *storage.Manager) int64 {
	entries, err := manager.ReadSegments(manager.SegmentPaths())
	if err != nil {
		return 0
	}
	var latest int64
	for _, e := range entries {
		if totalShards > 0 && ingest.ShardID(e.Service, totalShards) != shardID {
			continue
		}
		if e.ReceivedAt > latest {
			latest = e.ReceivedAt
		}
	}
	return latest
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
