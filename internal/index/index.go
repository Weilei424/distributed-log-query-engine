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
// It maps message tokens, service names, and namespaces to segment paths, and
// tracks per-segment time bounds for time-range pruning.
type Index struct {
	mu                sync.RWMutex
	tokenSegments     map[string]map[string]struct{} // token → set of segment paths
	serviceSegments   map[string]map[string]struct{} // service → set of segment paths
	namespaceSegments map[string]map[string]struct{} // namespace → set of segment paths
	segmentMeta       map[string]SegmentMeta         // segment path → time bounds
}

// NewIndex returns an initialized empty Index.
func NewIndex() *Index {
	return &Index{
		tokenSegments:     make(map[string]map[string]struct{}),
		serviceSegments:   make(map[string]map[string]struct{}),
		namespaceSegments: make(map[string]map[string]struct{}),
		segmentMeta:       make(map[string]SegmentMeta),
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

	ns := entry.Namespace
	if idx.namespaceSegments[ns] == nil {
		idx.namespaceSegments[ns] = make(map[string]struct{})
	}
	idx.namespaceSegments[ns][segmentPath] = struct{}{}

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
// matching the given tokens, namespace, service, and time range.
// Empty tokens slice, empty namespace or service, and zero time bounds, are ignored.
func (idx *Index) Resolve(tokens []string, namespace, service string, startTime, endTime int64) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Start with all known segments.
	candidates := make(map[string]struct{})
	for path := range idx.segmentMeta {
		candidates[path] = struct{}{}
	}

	// Intersect by required tokens. Each token must appear in the segment.
	for _, tok := range tokens {
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

	// Intersect by namespace (non-empty only).
	if namespace != "" {
		segs, ok := idx.namespaceSegments[namespace]
		if !ok {
			return nil
		}
		for path := range candidates {
			if _, found := segs[path]; !found {
				delete(candidates, path)
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

// TokenCount returns the number of unique tokens in the index.
func (idx *Index) TokenCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.tokenSegments)
}

// RemoveSegment removes all index entries that point to path.
// Called by compaction after merging or deleting a segment.
func (idx *Index) RemoveSegment(path string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(idx.segmentMeta, path)
	for tok, segs := range idx.tokenSegments {
		delete(segs, path)
		if len(segs) == 0 {
			delete(idx.tokenSegments, tok)
		}
	}
	for svc, segs := range idx.serviceSegments {
		delete(segs, path)
		if len(segs) == 0 {
			delete(idx.serviceSegments, svc)
		}
	}
	for ns, segs := range idx.namespaceSegments {
		delete(segs, path)
		if len(segs) == 0 {
			delete(idx.namespaceSegments, ns)
		}
	}
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
