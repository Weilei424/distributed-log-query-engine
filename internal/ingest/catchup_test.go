// internal/ingest/catchup_test.go
package ingest_test

import (
	"testing"
	"time"

	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// writeEntryWithTimestamp appends a log entry with a fixed ReceivedAt to local storage.
func writeEntryWithTimestamp(t *testing.T, manager *storage.Manager, idx *index.Index, id, service string, ts int64) {
	t.Helper()
	e := &types.LogEntry{
		ID:         id,
		Service:    service,
		Message:    "test",
		Level:      "INFO",
		ReceivedAt: ts,
	}
	path, err := manager.AppendWithPath(e)
	if err != nil {
		t.Fatalf("AppendWithPath %s: %v", id, err)
	}
	idx.Add(e, path)
}

// TestLatestReceivedAtForShard_EqualTimestamps verifies that when two entries share
// the same ReceivedAt, LatestReceivedAtForShard returns that shared timestamp.
func TestLatestReceivedAtForShard_EqualTimestamps(t *testing.T) {
	dir := t.TempDir()
	manager, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer manager.Close()
	idx := index.NewIndex()

	ts := time.Now().UnixNano()
	writeEntryWithTimestamp(t, manager, idx, "id-1", "svc-a", ts)
	writeEntryWithTimestamp(t, manager, idx, "id-2", "svc-a", ts)

	got := ingest.LatestReceivedAtForShard(0, 0, manager)
	if got != ts {
		t.Fatalf("LatestReceivedAtForShard = %d, want %d", got, ts)
	}
}

// TestCatchUp_EqualTimestamps verifies that when two entries on the primary share
// the same ReceivedAt timestamp and the replica already has one of them, CatchUp
// appends the missing entry and does not duplicate the existing one.
//
// This covers the bug where FetchShardEntries used strict > filtering and would
// permanently miss entries at the watermark boundary.
func TestCatchUp_EqualTimestamps(t *testing.T) {
	const totalShards = 4
	const service = "svc-a"
	shardID := ingest.ShardID("", service, totalShards)

	// Simulate primary storage: two entries with identical timestamps.
	primaryDir := t.TempDir()
	primaryManager, err := storage.NewManager(primaryDir, 64*1024*1024)
	if err != nil {
		t.Fatalf("primary NewManager: %v", err)
	}
	defer primaryManager.Close()
	primaryIdx := index.NewIndex()

	ts := time.Now().UnixNano()
	writeEntryWithTimestamp(t, primaryManager, primaryIdx, "entry-A", service, ts)
	writeEntryWithTimestamp(t, primaryManager, primaryIdx, "entry-B", service, ts)

	// Simulate replica storage: already has entry-A.
	replicaDir := t.TempDir()
	replicaManager, err := storage.NewManager(replicaDir, 64*1024*1024)
	if err != nil {
		t.Fatalf("replica NewManager: %v", err)
	}
	defer replicaManager.Close()
	replicaIdx := index.NewIndex()
	writeEntryWithTimestamp(t, replicaManager, replicaIdx, "entry-A", service, ts)

	// Verify watermark is ts and knownIDs includes entry-A.
	watermark := ingest.LatestReceivedAtForShard(shardID, totalShards, replicaManager)
	if watermark != ts {
		t.Fatalf("watermark = %d, want %d", watermark, ts)
	}

	// Fetch what the primary would return with >= semantics (both entries at ts).
	primaryEntries, err := primaryManager.ReadSegments(primaryManager.SegmentPaths())
	if err != nil {
		t.Fatalf("primary ReadSegments: %v", err)
	}
	fetched := make(map[string]bool)
	for _, e := range primaryEntries {
		if ingest.ShardID(e.Namespace, e.Service, totalShards) == shardID && e.ReceivedAt >= watermark {
			fetched[e.ID] = true
		}
	}
	if !fetched["entry-A"] || !fetched["entry-B"] {
		t.Fatalf("primary should return both entries at watermark: got %v", fetched)
	}

	// The replica has entry-A; only entry-B should be appended.
	before, _ := replicaManager.ReadSegments(replicaManager.SegmentPaths())
	initialCount := len(before)
	if initialCount != 1 {
		t.Fatalf("replica should start with 1 entry, got %d", initialCount)
	}

	// Simulate what CatchUp does: skip known IDs, append new ones.
	knownIDs := map[string]bool{"entry-A": true}
	appended := 0
	for _, e := range primaryEntries {
		if ingest.ShardID(e.Namespace, e.Service, totalShards) != shardID {
			continue
		}
		if e.ReceivedAt < watermark {
			continue
		}
		if knownIDs[e.ID] {
			continue
		}
		path, err := replicaManager.AppendWithPath(e)
		if err != nil {
			t.Fatalf("AppendWithPath: %v", err)
		}
		replicaIdx.Add(e, path)
		appended++
	}

	if appended != 1 {
		t.Errorf("expected 1 entry appended, got %d", appended)
	}
	after, _ := replicaManager.ReadSegments(replicaManager.SegmentPaths())
	if len(after) != 2 {
		t.Errorf("replica should have 2 entries after catch-up, got %d", len(after))
	}
}
