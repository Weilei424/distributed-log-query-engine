package storage_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

func makeEntry(i int) *types.LogEntry {
	return &types.LogEntry{
		ID:      fmt.Sprintf("id-%d", i),
		Service: "test-svc",
		Level:   "INFO",
		Message: fmt.Sprintf("message %d", i),
	}
}

func countRecords(t *testing.T, paths []string) int {
	t.Helper()
	total := 0
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open segment %s: %v", filepath.Base(path), err)
		}
		for {
			_, err := storage.ReadRecord(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("ReadRecord in %s: %v", filepath.Base(path), err)
			}
			total++
		}
		f.Close()
	}
	return total
}

func TestManager_AppendAndRestart(t *testing.T) {
	dir := t.TempDir()

	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	const n = 10
	for i := 0; i < n; i++ {
		if err := m.Append(makeEntry(i)); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m2, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager reopen: %v", err)
	}
	t.Cleanup(func() { m2.Close() })

	total := countRecords(t, m2.SegmentPaths())
	if total != n {
		t.Errorf("expected %d records after restart, got %d", n, total)
	}
}

func TestManager_Rotation(t *testing.T) {
	dir := t.TempDir()

	m, err := storage.NewManager(dir, 128) // tiny threshold
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	for i := 0; i < 20; i++ {
		if err := m.Append(makeEntry(i)); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}

	paths := m.SegmentPaths()
	if len(paths) < 2 {
		t.Errorf("expected at least 2 segment files after rotation, got %d", len(paths))
	}

	total := countRecords(t, paths)
	if total != 20 {
		t.Errorf("expected 20 total records across all segments, got %d", total)
	}
}

func TestManager_SegmentPathsOrdered(t *testing.T) {
	dir := t.TempDir()

	m, err := storage.NewManager(dir, 128)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	for i := 0; i < 20; i++ {
		if err := m.Append(makeEntry(i)); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}

	paths := m.SegmentPaths()
	for i := 1; i < len(paths); i++ {
		if filepath.Base(paths[i]) <= filepath.Base(paths[i-1]) {
			t.Errorf("paths not ascending: %s >= %s",
				filepath.Base(paths[i]), filepath.Base(paths[i-1]))
		}
	}
}

func TestManager_EmptyDirCreatesFirstSegment(t *testing.T) {
	dir := t.TempDir()

	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })

	paths := m.SegmentPaths()
	if len(paths) != 1 {
		t.Errorf("expected 1 segment in new dir, got %d", len(paths))
	}
	if filepath.Base(paths[0]) != "00000000000000000001.seg" {
		t.Errorf("expected '00000000000000000001.seg', got %q", filepath.Base(paths[0]))
	}
}
