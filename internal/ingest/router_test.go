// internal/ingest/router_test.go
package ingest_test

import (
	"testing"

	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
)

func TestShardID_Deterministic(t *testing.T) {
	got1 := ingest.ShardID("payments", 16)
	got2 := ingest.ShardID("payments", 16)
	if got1 != got2 {
		t.Fatalf("ShardID not deterministic: %d != %d", got1, got2)
	}
}

func TestShardID_InRange(t *testing.T) {
	for _, svc := range []string{"auth", "payments", "api", "worker", "cache", "db"} {
		id := ingest.ShardID(svc, 16)
		if id < 0 || id >= 16 {
			t.Fatalf("ShardID(%q, 16) = %d, want [0, 16)", svc, id)
		}
	}
}

func TestShardID_EmptyService(t *testing.T) {
	id := ingest.ShardID("", 16)
	if id < 0 || id >= 16 {
		t.Fatalf("ShardID(\"\", 16) = %d, out of range", id)
	}
}

func TestShardID_Distribution(t *testing.T) {
	services := []string{"auth", "payments", "api", "worker", "cache", "db", "search", "notify"}
	seen := make(map[int]bool)
	for _, svc := range services {
		seen[ingest.ShardID(svc, 16)] = true
	}
	if len(seen) < 4 {
		t.Fatalf("poor distribution: only %d distinct shards for %d services", len(seen), len(services))
	}
}
