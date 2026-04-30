package coordinator

import (
	"testing"
	"time"

	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

func makeResult(n int) *types.QueryResult {
	entries := make([]*types.LogEntry, n)
	for i := range entries {
		entries[i] = &types.LogEntry{ID: "x"}
	}
	return &types.QueryResult{Entries: entries, Total: int32(n)}
}

func TestQueryCache_HitAndMiss(t *testing.T) {
	c := NewQueryCache(5*time.Second, 10)
	c.Put("key1", makeResult(3))

	got, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if len(got.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got.Entries))
	}

	_, ok = c.Get("key2")
	if ok {
		t.Fatal("expected cache miss for unknown key")
	}
}

func TestQueryCache_TTLExpiry(t *testing.T) {
	c := NewQueryCache(50*time.Millisecond, 10)
	c.Put("k", makeResult(1))
	time.Sleep(100 * time.Millisecond)
	_, ok := c.Get("k")
	if ok {
		t.Fatal("expected cache miss after TTL expiry")
	}
}

func TestQueryCache_LRUEviction(t *testing.T) {
	c := NewQueryCache(10*time.Second, 2)
	c.Put("a", makeResult(1))
	c.Put("b", makeResult(1))
	c.Put("c", makeResult(1))
	_, ok := c.Get("a")
	if ok {
		t.Fatal("expected 'a' to be evicted")
	}
	_, ok = c.Get("b")
	if !ok {
		t.Fatal("expected 'b' to still be present")
	}
}

func TestQueryCache_UpdateRefreshesLRU(t *testing.T) {
	c := NewQueryCache(10*time.Second, 2)
	c.Put("a", makeResult(1))
	c.Put("b", makeResult(1))
	c.Get("a")
	c.Put("c", makeResult(1))
	_, ok := c.Get("b")
	if ok {
		t.Fatal("expected 'b' to be evicted after 'a' was accessed")
	}
}
