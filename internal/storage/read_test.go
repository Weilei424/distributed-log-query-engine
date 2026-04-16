package storage_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

func TestReadSegment_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	now := time.Now().UnixNano()
	want := []*types.LogEntry{
		{ID: "a", Service: "svc", Level: "INFO", Message: "hello world", Timestamp: now},
		{ID: "b", Service: "svc", Level: "WARN", Message: "goodbye world", Timestamp: now + 1},
	}
	for _, e := range want {
		if err := m.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	paths := m.SegmentPaths()
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := storage.ReadSegment(paths[0])
	if err != nil {
		t.Fatalf("ReadSegment: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	if got[0].ID != "a" || got[1].ID != "b" {
		t.Errorf("unexpected IDs: %q %q", got[0].ID, got[1].ID)
	}
	if got[0].Message != "hello world" || got[1].Message != "goodbye world" {
		t.Errorf("unexpected messages: %q %q", got[0].Message, got[1].Message)
	}
}

func TestReadSegment_Empty(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "*.seg")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	name := f.Name()
	f.Close()

	entries, err := storage.ReadSegment(name)
	if err != nil {
		t.Fatalf("ReadSegment on empty file: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
}

func TestReadSegment_NotFound(t *testing.T) {
	_, err := storage.ReadSegment(filepath.Join(t.TempDir(), "missing.seg"))
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

func TestManager_ActiveSegmentPath(t *testing.T) {
	dir := t.TempDir()
	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	path := m.ActiveSegmentPath()
	if path == "" {
		t.Fatal("expected non-empty active segment path")
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("ActiveSegmentPath %q does not exist: %v", path, err)
	}
}

func TestManager_ReadSegments_MultipleSegments(t *testing.T) {
	dir := t.TempDir()
	// Small segment size forces rotation after the first entry.
	m, err := storage.NewManager(dir, 1)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	entries := []*types.LogEntry{
		{ID: "1", Service: "svc", Message: "first"},
		{ID: "2", Service: "svc", Message: "second"},
	}
	for _, e := range entries {
		if err := m.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	paths := m.SegmentPaths()
	if len(paths) < 2 {
		t.Fatalf("expected at least 2 segments after forced rotation, got %d", len(paths))
	}

	got, err := m.ReadSegments(paths)
	if err != nil {
		t.Fatalf("ReadSegments: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries across segments, got %d", len(got))
	}
	if got[0].ID != "1" || got[1].ID != "2" {
		t.Errorf("unexpected IDs: %q %q", got[0].ID, got[1].ID)
	}
}
