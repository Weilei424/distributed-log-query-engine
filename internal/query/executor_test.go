package query_test

import (
	"context"
	"testing"

	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// newExecutor creates a Manager with entries written and an Index populated from
// those entries, then returns a LocalExecutor over them.
func newExecutor(t *testing.T, entries []*types.LogEntry) *query.LocalExecutor {
	t.Helper()
	m, err := storage.NewManager(t.TempDir(), 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	idx := index.NewIndex()
	for _, e := range entries {
		if err := m.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
		idx.Add(e, m.ActiveSegmentPath())
	}
	return query.NewLocalExecutor(idx, m)
}

func TestExecute_KeywordFilter(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "user login failed", Timestamp: 100},
		{ID: "2", Service: "svc", Message: "disk write error", Timestamp: 200},
	}
	ex := newExecutor(t, entries)

	result, err := ex.Execute(context.Background(), &types.QueryRequest{Keyword: "login"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 1 {
		t.Errorf("expected Total=1, got %d", result.Total)
	}
	if len(result.Entries) != 1 || result.Entries[0].ID != "1" {
		t.Errorf("expected entry 1, got %+v", result.Entries)
	}
}

func TestExecute_KeywordCaseInsensitive(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "User Login Success", Timestamp: 100},
	}
	ex := newExecutor(t, entries)

	result, err := ex.Execute(context.Background(), &types.QueryRequest{Keyword: "login"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 1 {
		t.Errorf("expected Total=1 for case-insensitive keyword, got %d", result.Total)
	}
}

func TestExecute_TimeRangeFilter(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "alpha", Timestamp: 100},
		{ID: "2", Service: "svc", Message: "alpha", Timestamp: 500},
		{ID: "3", Service: "svc", Message: "alpha", Timestamp: 900},
	}
	ex := newExecutor(t, entries)

	result, err := ex.Execute(context.Background(), &types.QueryRequest{StartTime: 200, EndTime: 700})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 1 {
		t.Errorf("expected Total=1, got %d", result.Total)
	}
	if len(result.Entries) != 1 || result.Entries[0].ID != "2" {
		t.Errorf("expected only entry 2, got %+v", result.Entries)
	}
}

func TestExecute_Pagination(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "alpha", Timestamp: 100},
		{ID: "2", Service: "svc", Message: "alpha", Timestamp: 200},
		{ID: "3", Service: "svc", Message: "alpha", Timestamp: 300},
	}
	ex := newExecutor(t, entries)

	// Sorted descending: 300(ID=3), 200(ID=2), 100(ID=1). Offset=1 skips 300.
	result, err := ex.Execute(context.Background(), &types.QueryRequest{Limit: 2, Offset: 1})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 3 {
		t.Errorf("expected Total=3 (before pagination), got %d", result.Total)
	}
	if len(result.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result.Entries))
	}
	if result.Entries[0].ID != "2" || result.Entries[1].ID != "1" {
		t.Errorf("unexpected order: %q %q", result.Entries[0].ID, result.Entries[1].ID)
	}
}

func TestExecute_SortedDescending(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "event", Timestamp: 300},
		{ID: "2", Service: "svc", Message: "event", Timestamp: 100},
		{ID: "3", Service: "svc", Message: "event", Timestamp: 200},
	}
	ex := newExecutor(t, entries)

	result, err := ex.Execute(context.Background(), &types.QueryRequest{Keyword: "event"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(result.Entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(result.Entries))
	}
	if result.Entries[0].ID != "1" || result.Entries[1].ID != "3" || result.Entries[2].ID != "2" {
		t.Errorf("wrong order: %q %q %q",
			result.Entries[0].ID, result.Entries[1].ID, result.Entries[2].ID)
	}
}

func TestExecute_NoFilters_ReturnsAll(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "alpha", Timestamp: 100},
		{ID: "2", Service: "svc", Message: "beta", Timestamp: 200},
	}
	ex := newExecutor(t, entries)

	result, err := ex.Execute(context.Background(), &types.QueryRequest{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 2 {
		t.Errorf("expected Total=2, got %d", result.Total)
	}
}

func TestExecute_OffsetBeyondTotal_ReturnsEmpty(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "alpha", Timestamp: 100},
	}
	ex := newExecutor(t, entries)

	result, err := ex.Execute(context.Background(), &types.QueryRequest{Offset: 10})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 1 {
		t.Errorf("expected Total=1, got %d", result.Total)
	}
	if len(result.Entries) != 0 {
		t.Errorf("expected 0 entries after offset exceeds total, got %d", len(result.Entries))
	}
}

func TestExecute_KeywordPartialToken_NoMatch(t *testing.T) {
	// Word-boundary semantics: "log" is not a token in "user login failed",
	// so it must return no results through the full executor path.
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "user login failed", Timestamp: 100},
	}
	ex := newExecutor(t, entries)

	result, err := ex.Execute(context.Background(), &types.QueryRequest{Keyword: "log"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 0 {
		t.Errorf("expected Total=0 for partial-token keyword 'log', got %d", result.Total)
	}
}

func TestExecute_NegativeLimit_ReturnsError(t *testing.T) {
	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "alpha", Timestamp: 100},
	}
	ex := newExecutor(t, entries)

	_, err := ex.Execute(context.Background(), &types.QueryRequest{Limit: -1})
	if err == nil {
		t.Fatal("expected error for negative limit, got nil")
	}
}
