package index

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// nonAlphanumeric matches sequences of characters that are not lowercase letters or digits.
var nonAlphanumeric = regexp.MustCompile(`[^a-z0-9]+`)

// SegmentMeta records the observed timestamp bounds for a segment file.
type SegmentMeta struct {
	MinTime int64
	MaxTime int64
}

// Index is a thread-safe in-memory inverted index.
// It maps message tokens and service names to segment paths, and tracks
// per-segment time bounds for time-range pruning.
type Index struct {
	mu              sync.RWMutex
	tokenSegments   map[string]map[string]struct{} // token → set of segment paths
	serviceSegments map[string]map[string]struct{} // service → set of segment paths
	segmentMeta     map[string]SegmentMeta         // segment path → time bounds
}

// NewIndex returns an initialized empty Index.
func NewIndex() *Index {
	return &Index{
		tokenSegments:   make(map[string]map[string]struct{}),
		serviceSegments: make(map[string]map[string]struct{}),
		segmentMeta:     make(map[string]SegmentMeta),
	}
}

// tokenize lowercases s and splits on non-alphanumeric character sequences.
// Empty tokens are omitted.
func tokenize(s string) []string {
	lower := strings.ToLower(s)
	parts := nonAlphanumeric.Split(lower, -1)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Add registers entry in the index under segmentPath.
// Safe for concurrent use.
func (idx *Index) Add(entry *types.LogEntry, segmentPath string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, tok := range tokenize(entry.Message) {
		if idx.tokenSegments[tok] == nil {
			idx.tokenSegments[tok] = make(map[string]struct{})
		}
		idx.tokenSegments[tok][segmentPath] = struct{}{}
	}

	if entry.Service != "" {
		if idx.serviceSegments[entry.Service] == nil {
			idx.serviceSegments[entry.Service] = make(map[string]struct{})
		}
		idx.serviceSegments[entry.Service][segmentPath] = struct{}{}
	}

	meta, ok := idx.segmentMeta[segmentPath]
	if !ok {
		meta = SegmentMeta{MinTime: entry.Timestamp, MaxTime: entry.Timestamp}
	} else {
		if entry.Timestamp < meta.MinTime {
			meta.MinTime = entry.Timestamp
		}
		if entry.Timestamp > meta.MaxTime {
			meta.MaxTime = entry.Timestamp
		}
	}
	idx.segmentMeta[segmentPath] = meta
}

// Resolve returns the sorted set of segment paths that may contain entries
// matching the given keyword, service, and time range.
// Empty keyword or service, and zero time bounds, are ignored.
func (idx *Index) Resolve(keyword, service string, startTime, endTime int64) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Start with all known segments.
	// segmentMeta is the authoritative set of indexed segments; Add always writes it,
	// so any path in tokenSegments or serviceSegments is guaranteed to be here too.
	candidates := make(map[string]struct{})
	for path := range idx.segmentMeta {
		candidates[path] = struct{}{}
	}

	// Intersect by keyword tokens (only when keyword is non-empty).
	if keyword != "" {
		for _, tok := range tokenize(keyword) {
			segs, ok := idx.tokenSegments[tok]
			if !ok {
				return nil
			}
			for path := range candidates {
				if _, found := segs[path]; !found {
					delete(candidates, path)
				}
			}
			if len(candidates) == 0 {
				return nil
			}
		}
	}

	// Intersect by service.
	if service != "" {
		segs, ok := idx.serviceSegments[service]
		if !ok {
			return nil
		}
		for path := range candidates {
			if _, found := segs[path]; !found {
				delete(candidates, path)
			}
		}
	}

	// Prune by time range.
	for path := range candidates {
		meta := idx.segmentMeta[path]
		if startTime > 0 && meta.MaxTime < startTime {
			delete(candidates, path)
			continue
		}
		if endTime > 0 && meta.MinTime > endTime {
			delete(candidates, path)
		}
	}

	paths := make([]string, 0, len(candidates))
	for path := range candidates {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// RebuildFromSegments repopulates the index from a list of segment files.
// readFn is called for each path to load its entries.
// Returns a wrapped error if any segment cannot be read.
func (idx *Index) RebuildFromSegments(paths []string, readFn func(string) ([]*types.LogEntry, error)) error {
	for _, path := range paths {
		entries, err := readFn(path)
		if err != nil {
			return fmt.Errorf("rebuild index from %s: %w", path, err)
		}
		for _, entry := range entries {
			idx.Add(entry, path)
		}
	}
	return nil
}
