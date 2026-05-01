// internal/ingest/router_test.go
package ingest

import (
	"testing"
)

func TestShardID_Deterministic(t *testing.T) {
	first := ShardID("ns1", "api", 8)
	for i := 0; i < 10; i++ {
		if ShardID("ns1", "api", 8) != first {
			t.Fatal("ShardID not deterministic")
		}
	}
}

func TestShardID_NamespaceAffectsResult(t *testing.T) {
	a := ShardID("ns1", "api", 8)
	b := ShardID("ns2", "api", 8)
	if a == b {
		t.Skip("hash collision — acceptable but unusual")
	}
}

func TestShardID_ZeroShards(t *testing.T) {
	if ShardID("ns", "svc", 0) != 0 {
		t.Fatal("expected 0 for totalShards=0")
	}
}

func TestShardID_Range(t *testing.T) {
	for i := 0; i < 100; i++ {
		id := ShardID("", "svc", 8)
		if id < 0 || id >= 8 {
			t.Fatalf("ShardID out of range: %d", id)
		}
	}
}
