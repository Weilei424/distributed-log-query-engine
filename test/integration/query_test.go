package integration_test

import (
	"context"
	"testing"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

func TestQuerySingleNode_Filters(t *testing.T) {
	dir := t.TempDir()
	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	idx := index.NewIndex()
	srv := ingest.NewLocalServer(m, idx)
	ex := query.NewLocalExecutor(idx, m)
	ctx := context.Background()

	entries := []*logengine.LogEntry{
		{Id: "1", Service: "auth", Level: "INFO", Message: "user login success", Timestamp: 100},
		{Id: "2", Service: "db", Level: "ERROR", Message: "connection timeout", Timestamp: 200},
		{Id: "3", Service: "auth", Level: "WARN", Message: "user login failed", Timestamp: 300},
	}
	for _, e := range entries {
		if _, err := srv.Ingest(ctx, &logengine.IngestRequest{Entry: e}); err != nil {
			t.Fatalf("Ingest %s: %v", e.Id, err)
		}
	}

	t.Run("keyword filter", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{QueryString: "login"})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 2 {
			t.Errorf("expected Total=2, got %d", result.Total)
		}
	})

	t.Run("service filter", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{Service: "db"})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 1 || result.Entries[0].ID != "2" {
			t.Errorf("expected entry 2 for service=db, got %+v", result.Entries)
		}
	})

	t.Run("time range filter", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{StartTime: 150, EndTime: 350})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 2 {
			t.Errorf("expected Total=2 for time range [150,350], got %d", result.Total)
		}
	})

	t.Run("combined filters", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{
			QueryString: "login",
			Service:     "auth",
			StartTime:   200,
			EndTime:     400,
		})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 1 || result.Entries[0].ID != "3" {
			t.Errorf("expected entry 3 for combined filters, got %+v", result.Entries)
		}
	})
}

func TestQuerySingleNode_IndexRebuildAfterRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Phase 1: ingest entries and close the node.
	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	idx := index.NewIndex()
	srv := ingest.NewLocalServer(m, idx)

	entries := []*logengine.LogEntry{
		{Id: "1", Service: "auth", Level: "INFO", Message: "token expired", Timestamp: 100},
		{Id: "2", Service: "cache", Level: "WARN", Message: "cache miss rate high", Timestamp: 200},
	}
	for _, e := range entries {
		if _, err := srv.Ingest(ctx, &logengine.IngestRequest{Entry: e}); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: reopen and rebuild index from disk.
	m2, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager reopen: %v", err)
	}
	t.Cleanup(func() { m2.Close() })

	idx2 := index.NewIndex()
	if err := idx2.RebuildFromSegments(m2.SegmentPaths(), storage.ReadSegment); err != nil {
		t.Fatalf("RebuildFromSegments: %v", err)
	}
	ex := query.NewLocalExecutor(idx2, m2)

	t.Run("keyword query after restart", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{QueryString: "token"})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 1 || result.Entries[0].ID != "1" {
			t.Errorf("expected entry 1 for 'token', got %+v", result.Entries)
		}
	})

	t.Run("service query after restart", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{Service: "cache"})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 1 || result.Entries[0].ID != "2" {
			t.Errorf("expected entry 2 for service=cache, got %+v", result.Entries)
		}
	})

	t.Run("all entries present after restart", func(t *testing.T) {
		result, err := ex.Execute(ctx, &types.QueryRequest{})
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if result.Total != 2 {
			t.Errorf("expected Total=2 after restart, got %d", result.Total)
		}
	})
}
