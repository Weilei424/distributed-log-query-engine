package storage

import (
	"testing"
	"time"

	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

func makeTestManager(t *testing.T, maxBytes int64) *Manager {
	t.Helper()
	dir := t.TempDir()
	m, err := NewManager(dir, maxBytes)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func TestCompactor_MergePass(t *testing.T) {
	m := makeTestManager(t, 1)
	now := time.Now().UnixNano()

	entries := []*types.LogEntry{
		{ID: "a", Timestamp: now, Service: "svc", Level: "INFO", Message: "alpha"},
		{ID: "b", Timestamp: now + 1, Service: "svc", Level: "INFO", Message: "beta"},
		{ID: "c", Timestamp: now + 2, Service: "svc", Level: "INFO", Message: "gamma"},
	}
	for _, e := range entries {
		if err := m.Append(e); err != nil {
			t.Fatal(err)
		}
	}

	closedBefore := len(m.ListClosedSegments())
	if closedBefore < 2 {
		t.Fatalf("expected at least 2 closed segments, got %d", closedBefore)
	}

	c := NewCompactor(m, CompactorConfig{
		MergeThresholdBytes: 10 * 1024 * 1024,
		RetentionDays:       0,
		IntervalSeconds:     0,
	})
	c.runMergePass()

	closedAfter := len(m.ListClosedSegments())
	if closedAfter >= closedBefore {
		t.Fatalf("expected fewer segments after merge, got %d (was %d)", closedAfter, closedBefore)
	}

	all, err := m.ReadSegments(m.SegmentPaths())
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < len(entries) {
		t.Fatalf("expected %d entries after merge, got %d", len(entries), len(all))
	}
}

func TestCompactor_RetentionPass(t *testing.T) {
	m := makeTestManager(t, 1)
	oldNs := time.Now().Add(-10 * 24 * time.Hour).UnixNano()

	old := &types.LogEntry{ID: "old", Timestamp: oldNs, Service: "svc", Level: "INFO", Message: "old"}
	fresh := &types.LogEntry{ID: "fresh", Timestamp: time.Now().UnixNano(), Service: "svc", Level: "INFO", Message: "fresh"}

	if err := m.Append(old); err != nil {
		t.Fatal(err)
	}
	if err := m.Append(fresh); err != nil {
		t.Fatal(err)
	}

	c := NewCompactor(m, CompactorConfig{
		MergeThresholdBytes: 0,
		RetentionDays:       7,
		IntervalSeconds:     0,
	})
	c.runRetentionPass()

	all, err := m.ReadSegments(m.SegmentPaths())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range all {
		if e.ID == "old" {
			t.Fatal("old entry should have been deleted by retention pass")
		}
	}
}

func TestCompactor_RetentionDisabled(t *testing.T) {
	m := makeTestManager(t, 1)
	oldNs := time.Now().Add(-30 * 24 * time.Hour).UnixNano()
	if err := m.Append(&types.LogEntry{ID: "x", Timestamp: oldNs, Service: "s", Message: "m"}); err != nil {
		t.Fatal(err)
	}

	c := NewCompactor(m, CompactorConfig{RetentionDays: 0})
	c.runRetentionPass()

	all, _ := m.ReadSegments(m.SegmentPaths())
	found := false
	for _, e := range all {
		if e.ID == "x" {
			found = true
		}
	}
	if !found {
		t.Fatal("entry should not be deleted when RetentionDays=0")
	}
}
