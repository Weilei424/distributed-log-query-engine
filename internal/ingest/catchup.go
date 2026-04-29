package ingest

import (
	"context"
	"time"

	"go.uber.org/zap"
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
func CatchUp(ctx context.Context, nodeID string, totalShards int, state metadata.ClusterState, manager *storage.Manager, idx *index.Index, logger *zap.Logger) int {
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
			logger.Warn("catch-up: primary address unknown, skipping",
				zap.Int("shard_id", shardID),
			)
			continue
		}

		sinceNs := LatestReceivedAtForShard(shardID, totalShards, manager)
		// Collect IDs already present at the watermark. FetchShardEntries uses >=
		// to avoid missing entries that share the same timestamp, so we deduplicate
		// here rather than filter on the primary side.
		knownIDs := localIDsAtOrAfterNs(shardID, totalShards, sinceNs, manager)

		conn, err := grpc.NewClient(primaryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logger.Error("catch-up: dial primary failed, skipping shard",
				zap.Int("shard_id", shardID),
				zap.String("primary_addr", primaryAddr),
				zap.Error(err),
			)
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
			logger.Error("catch-up: FetchShardEntries failed, skipping shard",
				zap.Int("shard_id", shardID),
				zap.String("primary_addr", primaryAddr),
				zap.Error(err),
			)
			continue
		}

		shardAppended := 0
		for _, pb := range resp.Entries {
			if knownIDs[pb.Id] {
				continue
			}
			e := ProtoToEntry(pb)
			segPath, err := manager.AppendWithPath(e)
			if err != nil {
				logger.Error("catch-up: append entry failed",
					zap.Int("shard_id", shardID),
					zap.String("entry_id", e.ID),
					zap.Error(err),
				)
				continue
			}
			idx.Add(e, segPath)
			shardAppended++
			appended++
		}
		logger.Info("catch-up: shard caught up",
			zap.Int("shard_id", shardID),
			zap.Int("entries_appended", shardAppended),
			zap.String("primary_addr", primaryAddr),
		)
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

// localIDsAtOrAfterNs returns the set of entry IDs already present locally for
// the given shard whose received_at >= sinceNs. Used by CatchUp to deduplicate
// entries returned at the watermark boundary by FetchShardEntries.
func localIDsAtOrAfterNs(shardID, totalShards int, sinceNs int64, manager *storage.Manager) map[string]bool {
	entries, err := manager.ReadSegments(manager.SegmentPaths())
	if err != nil {
		return nil
	}
	ids := make(map[string]bool)
	for _, e := range entries {
		if totalShards > 0 && ShardID(e.Service, totalShards) != shardID {
			continue
		}
		if e.ReceivedAt >= sinceNs {
			ids[e.ID] = true
		}
	}
	return ids
}
