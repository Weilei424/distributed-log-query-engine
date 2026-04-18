package ingest

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

// CatchUp fetches missing entries from the primary for each shard this node replicates.
// Runs synchronously; skips silently if a primary is unreachable.
// Returns the number of entries appended across all shards.
func CatchUp(ctx context.Context, nodeID string, totalShards int, state metadata.ClusterState, manager *storage.Manager, idx *index.Index) int {
	appended := 0
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

		sinceNs := LatestReceivedAtForShard(shardID, totalShards, manager)

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
			e := ProtoToEntry(pb)
			segPath, err := manager.AppendWithPath(e)
			if err != nil {
				log.Printf("catch-up: append entry %s: %v", e.ID, err)
				continue
			}
			idx.Add(e, segPath)
			appended++
		}
		log.Printf("catch-up: shard %d caught up %d entries from %s", shardID, len(resp.Entries), primaryAddr)
	}
	return appended
}

// LatestReceivedAtForShard returns the largest received_at nanosecond timestamp
// among all local entries that belong to the given shard. Returns 0 if none.
func LatestReceivedAtForShard(shardID, totalShards int, manager *storage.Manager) int64 {
	entries, err := manager.ReadSegments(manager.SegmentPaths())
	if err != nil {
		return 0
	}
	var latest int64
	for _, e := range entries {
		if totalShards > 0 && ShardID(e.Service, totalShards) != shardID {
			continue
		}
		if e.ReceivedAt > latest {
			latest = e.ReceivedAt
		}
	}
	return latest
}
