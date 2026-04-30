package ingest

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/metadata"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

// CatchUp first transfers any missing closed segment files from the primary,
// then falls back to entry-level catch-up for the active segment tail.
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
			logger.Warn("catch-up: primary address unknown, skipping", zap.Int("shard_id", shardID))
			continue
		}

		conn, err := grpc.NewClient(primaryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logger.Error("catch-up: dial primary failed, skipping shard",
				zap.Int("shard_id", shardID), zap.Error(err))
			continue
		}

		client := logengine.NewIngestServiceClient(conn)

		// Phase 1: transfer missing closed segment files.
		n := transferMissingSegments(ctx, shardID, manager, idx, client, logger)
		appended += n

		// Phase 2: entry-level catch-up for active segment tail.
		sinceNs := LatestReceivedAtForShard(shardID, totalShards, manager)
		knownIDs := localIDsAtOrAfterNs(shardID, totalShards, sinceNs, manager)

		fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		resp, err := client.FetchShardEntries(fetchCtx, &logengine.FetchShardEntriesRequest{
			ShardId:     int32(shardID),
			SinceUnixNs: sinceNs,
		})
		cancel()
		conn.Close()

		if err != nil {
			logger.Error("catch-up: FetchShardEntries failed, skipping shard",
				zap.Int("shard_id", shardID), zap.Error(err))
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

func transferMissingSegments(ctx context.Context, shardID int, manager *storage.Manager, idx *index.Index, client logengine.IngestServiceClient, logger *zap.Logger) int {
	listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	resp, err := client.ListSegments(listCtx, &logengine.ListSegmentsRequest{ShardId: int32(shardID)})
	cancel()
	if err != nil {
		return 0
	}

	local := make(map[string]struct{})
	for _, p := range manager.ListClosedSegments() {
		local[filepath.Base(p)] = struct{}{}
	}
	// Never overwrite the replica's active segment with a closed copy from the primary.
	activeName := manager.ActiveSegmentName()

	appended := 0
	for _, name := range resp.SegmentNames {
		if _, ok := local[name]; ok {
			continue
		}
		if name == activeName {
			continue
		}

		transferCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		stream, err := client.TransferSegment(transferCtx, &logengine.TransferSegmentRequest{
			SegmentName: name,
			ShardId:     int32(shardID),
		})
		if err != nil {
			cancel()
			continue
		}

		destPath := filepath.Join(manager.Dir(), name)
		tmp := destPath + ".tmp"
		f, err := os.Create(tmp)
		if err != nil {
			cancel()
			continue
		}

		ok := true
		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				ok = false
				break
			}
			if _, err := f.Write(chunk.Chunk); err != nil {
				ok = false
				break
			}
		}
		f.Sync()
		f.Close()
		cancel()

		if !ok {
			os.Remove(tmp)
			continue
		}
		if err := os.Rename(tmp, destPath); err != nil {
			os.Remove(tmp)
			continue
		}

		if err := manager.LoadSegment(destPath); err == nil {
			entries, err := manager.ReadSegments([]string{destPath})
			if err == nil {
				for _, e := range entries {
					idx.Add(e, destPath)
					appended++
				}
			}
		}
		logger.Info("catch-up: segment file transferred",
			zap.String("segment", name), zap.Int("shard_id", shardID))
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
		if totalShards > 0 && ShardID(e.Namespace, e.Service, totalShards) != shardID {
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
		if totalShards > 0 && ShardID(e.Namespace, e.Service, totalShards) != shardID {
			continue
		}
		if e.ReceivedAt >= sinceNs {
			ids[e.ID] = true
		}
	}
	return ids
}
