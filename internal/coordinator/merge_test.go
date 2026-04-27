package coordinator

import (
	"errors"
	"testing"

	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

func mkEntry(id string, ts int64) *types.LogEntry {
	return &types.LogEntry{ID: id, Timestamp: ts, Service: "svc", Message: "msg"}
}

func TestMergeResults_Sort(t *testing.T) {
	parts := []nodeResult{
		{nodeID: "n1", entries: []*types.LogEntry{mkEntry("a", 300), mkEntry("b", 100)}},
		{nodeID: "n2", entries: []*types.LogEntry{mkEntry("c", 200)}},
	}
	out := MergeResults(parts, 0, 0)
	if out.partial {
		t.Error("expected partial=false, no errors in parts")
	}
	if len(out.entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(out.entries))
	}
	// Sorted timestamp desc: a(300), c(200), b(100)
	wantOrder := []string{"a", "c", "b"}
	for i, e := range out.entries {
		if e.ID != wantOrder[i] {
			t.Errorf("position %d: want %q, got %q", i, wantOrder[i], e.ID)
		}
	}
	if out.total != 3 {
		t.Errorf("expected total=3, got %d", out.total)
	}
}

func TestMergeResults_TieBreaker(t *testing.T) {
	// Same timestamp — sort by ID ascending
	parts := []nodeResult{
		{nodeID: "n1", entries: []*types.LogEntry{mkEntry("z", 100), mkEntry("a", 100)}},
	}
	out := MergeResults(parts, 0, 0)
	if out.entries[0].ID != "a" || out.entries[1].ID != "z" {
		t.Errorf("tie-breaker: want [a z], got [%s %s]", out.entries[0].ID, out.entries[1].ID)
	}
}

func TestMergeResults_Dedup(t *testing.T) {
	e := mkEntry("dup-id", 100)
	parts := []nodeResult{
		{nodeID: "n1", entries: []*types.LogEntry{e}},
		{nodeID: "n2", entries: []*types.LogEntry{e}},
	}
	out := MergeResults(parts, 0, 0)
	if len(out.entries) != 1 {
		t.Fatalf("expected 1 entry after dedup, got %d", len(out.entries))
	}
	if out.total != 1 {
		t.Errorf("expected total=1, got %d", out.total)
	}
}

func TestMergeResults_Pagination(t *testing.T) {
	parts := []nodeResult{
		{nodeID: "n1", entries: []*types.LogEntry{
			mkEntry("e1", 500),
			mkEntry("e2", 400),
			mkEntry("e3", 300),
			mkEntry("e4", 200),
			mkEntry("e5", 100),
		}},
	}
	// offset=2, limit=2 → global sorted [e1,e2,e3,e4,e5] → skip 2 → [e3,e4,e5] → take 2 → [e3,e4]
	out := MergeResults(parts, 2, 2)
	if out.total != 5 {
		t.Errorf("expected total=5 (before pagination), got %d", out.total)
	}
	if len(out.entries) != 2 {
		t.Fatalf("expected 2 entries (limit=2), got %d", len(out.entries))
	}
	if out.entries[0].ID != "e3" || out.entries[1].ID != "e4" {
		t.Errorf("wrong pagination result: got [%s %s], want [e3 e4]",
			out.entries[0].ID, out.entries[1].ID)
	}
}

func TestMergeResults_Partial(t *testing.T) {
	parts := []nodeResult{
		{nodeID: "n1", entries: []*types.LogEntry{mkEntry("x", 100)}},
		{nodeID: "n2", err: errors.New("context deadline exceeded")},
	}
	out := MergeResults(parts, 0, 0)
	if !out.partial {
		t.Error("expected partial=true when a node has an error")
	}
	if len(out.entries) != 1 {
		t.Fatalf("expected 1 entry from healthy node, got %d", len(out.entries))
	}
	if out.entries[0].ID != "x" {
		t.Errorf("expected entry x, got %s", out.entries[0].ID)
	}
}

// TestMergeResults_NodeTotalPreferred verifies that when nodes report non-zero
// totals, MergeResults uses their sum rather than the candidate count. This
// matters when the per-node fetch limit truncates results below the true match
// count — the reported total should not be capped by the candidate window.
func TestMergeResults_NodeTotalPreferred(t *testing.T) {
	parts := []nodeResult{
		// Node reports 1500 matches but only returned 1000 due to its limit.
		{nodeID: "n1", total: 1500, entries: []*types.LogEntry{mkEntry("e1", 100)}},
		{nodeID: "n2", total: 800, entries: []*types.LogEntry{mkEntry("e2", 200)}},
	}
	out := MergeResults(parts, 0, 10)
	if out.total != 2300 {
		t.Errorf("expected total=2300 (sum of node totals), got %d", out.total)
	}
	if len(out.entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(out.entries))
	}
}

func TestMergeResults_AllFailed(t *testing.T) {
	parts := []nodeResult{
		{nodeID: "n1", err: errors.New("timeout")},
		{nodeID: "n2", err: errors.New("timeout")},
	}
	out := MergeResults(parts, 0, 0)
	if !out.partial {
		t.Error("expected partial=true when all nodes fail")
	}
	if len(out.entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(out.entries))
	}
	if out.total != 0 {
		t.Errorf("expected total=0, got %d", out.total)
	}
}
