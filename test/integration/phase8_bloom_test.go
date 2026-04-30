package integration_test

import (
	"context"
	"os"
	"testing"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/query"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// TestBloomFilter_CorrectResultsWithBloomEnabled verifies that enabling bloom
// filters does not affect query correctness — results must match with or without bloom.
func TestBloomFilter_CorrectResultsWithBloomEnabled(t *testing.T) {
	os.Setenv("BLOOM_ENABLED", "true")
	t.Cleanup(func() { os.Unsetenv("BLOOM_ENABLED") })

	dir := t.TempDir()
	// Use a tiny segment cap so rotation happens after the first entry,
	// ensuring a closed segment with a bloom sidecar exists before querying.
	m, err := storage.NewManager(dir, 1)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	idx := index.NewIndex()
	srv := ingest.NewLocalServer(m, idx)
	ex := query.NewLocalExecutor(idx, m)
	ctx := context.Background()

	entries := []*logengine.LogEntry{
		{Service: "svc", Level: "ERROR", Message: "database connection failed", Timestamp: 100},
		{Service: "svc", Level: "INFO", Message: "server started", Timestamp: 200},
	}
	for _, e := range entries {
		if _, err := srv.Ingest(ctx, &logengine.IngestRequest{Entry: e}); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	result, err := ex.Execute(ctx, &types.QueryRequest{QueryString: "database"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Total != 1 {
		t.Fatalf("expected Total=1 for 'database', got %d (entries: %+v)", result.Total, result.Entries)
	}

	result2, err := ex.Execute(ctx, &types.QueryRequest{QueryString: "started"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result2.Total != 1 {
		t.Fatalf("expected Total=1 for 'started', got %d", result2.Total)
	}

	result3, err := ex.Execute(ctx, &types.QueryRequest{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result3.Total != 2 {
		t.Fatalf("expected Total=2 with no filter, got %d", result3.Total)
	}
}
