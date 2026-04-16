package index_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

func makeEntry(id, service, message string, ts int64) *types.LogEntry {
	return &types.LogEntry{ID: id, Service: service, Message: message, Timestamp: ts}
}

func TestResolve_KeywordHit(t *testing.T) {
	idx := index.NewIndex()
	idx.Add(makeEntry("e1", "svc", "user login failed", 100), "/seg/a")

	paths := idx.Resolve("login", "", 0, 0)
	if len(paths) != 1 || paths[0] != "/seg/a" {
		t.Errorf("expected [\"/seg/a\"], got %v", paths)
	}
}

func TestResolve_KeywordMiss(t *testing.T) {
	idx := index.NewIndex()
	idx.Add(makeEntry("e1", "svc", "user login failed", 100), "/seg/a")

	paths := idx.Resolve("timeout", "", 0, 0)
	if len(paths) != 0 {
		t.Errorf("expected empty, got %v", paths)
	}
}

func TestResolve_CaseInsensitive(t *testing.T) {
	idx := index.NewIndex()
	idx.Add(makeEntry("e1", "svc", "User Login Failed", 100), "/seg/a")

	paths := idx.Resolve("login", "", 0, 0)
	if len(paths) != 1 || paths[0] != "/seg/a" {
		t.Errorf("expected [\"/seg/a\"] for lowercase keyword on uppercase message, got %v", paths)
	}
}

func TestResolve_TimeRangePrune(t *testing.T) {
	idx := index.NewIndex()
	idx.Add(makeEntry("e1", "svc", "alpha", 100), "/seg/a")
	idx.Add(makeEntry("e2", "svc", "alpha", 500), "/seg/b")

	// Only /seg/b has entries within [300, 600].
	paths := idx.Resolve("alpha", "", 300, 600)
	if len(paths) != 1 || paths[0] != "/seg/b" {
		t.Errorf("expected [\"/seg/b\"], got %v", paths)
	}
}

func TestResolve_ServiceFilter(t *testing.T) {
	idx := index.NewIndex()
	idx.Add(makeEntry("e1", "svc-a", "hello world", 100), "/seg/a")
	idx.Add(makeEntry("e2", "svc-b", "hello world", 200), "/seg/b")

	paths := idx.Resolve("hello", "svc-a", 0, 0)
	if len(paths) != 1 || paths[0] != "/seg/a" {
		t.Errorf("expected [\"/seg/a\"], got %v", paths)
	}
}

func TestResolve_NoFilters_ReturnsAllSegments(t *testing.T) {
	idx := index.NewIndex()
	idx.Add(makeEntry("e1", "svc", "foo", 100), "/seg/a")
	idx.Add(makeEntry("e2", "svc", "bar", 200), "/seg/b")

	paths := idx.Resolve("", "", 0, 0)
	if len(paths) != 2 {
		t.Errorf("expected 2 segments, got %v", paths)
	}
}

func TestRebuildFromSegments_ProducesCorrectIndex(t *testing.T) {
	data := map[string][]*types.LogEntry{
		"/seg/a": {makeEntry("e1", "svc", "foo bar", 100)},
		"/seg/b": {makeEntry("e2", "svc", "baz qux", 200)},
	}
	readFn := func(path string) ([]*types.LogEntry, error) {
		e, ok := data[path]
		if !ok {
			return nil, fmt.Errorf("unknown path: %s", path)
		}
		return e, nil
	}

	idx := index.NewIndex()
	if err := idx.RebuildFromSegments([]string{"/seg/a", "/seg/b"}, readFn); err != nil {
		t.Fatalf("RebuildFromSegments: %v", err)
	}

	if paths := idx.Resolve("foo", "", 0, 0); len(paths) != 1 || paths[0] != "/seg/a" {
		t.Errorf("expected /seg/a for 'foo', got %v", paths)
	}
	if paths := idx.Resolve("baz", "", 0, 0); len(paths) != 1 || paths[0] != "/seg/b" {
		t.Errorf("expected /seg/b for 'baz', got %v", paths)
	}
}

func TestRebuildFromSegments_ReadFnError(t *testing.T) {
	readFn := func(path string) ([]*types.LogEntry, error) {
		return nil, fmt.Errorf("read error")
	}
	idx := index.NewIndex()
	err := idx.RebuildFromSegments([]string{"/seg/a"}, readFn)
	if err == nil {
		t.Error("expected error from failing readFn, got nil")
	}
}

func TestAdd_Concurrent(t *testing.T) {
	idx := index.NewIndex()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := makeEntry(fmt.Sprintf("e%d", i), "svc", fmt.Sprintf("token%d message", i), int64(i))
			idx.Add(e, fmt.Sprintf("/seg/%d", i%3))
			idx.Resolve(fmt.Sprintf("token%d", i), "", 0, 0)
		}(i)
	}
	wg.Wait()
}
