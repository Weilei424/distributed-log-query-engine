package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"google.golang.org/protobuf/proto"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/observability"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

var nonAlphanumericRe = regexp.MustCompile(`[^a-z0-9]+`)

const segmentNameFmt = "%020d.seg"

// Manager owns the data directory and the active segment.
// It is safe for concurrent use.
type Manager struct {
	mu              sync.Mutex
	dir             string
	maxSegmentBytes int64
	active          *Segment
	nextSeq         uint64
	paths           []string
	nodeID          string
	bloomEnabled    bool
	blooms          map[string]*bloom.BloomFilter
}

// ManagerOption configures a Manager.
type ManagerOption func(*Manager)

// WithNodeID sets the node ID used in Prometheus metric labels.
func WithNodeID(id string) ManagerOption {
	return func(m *Manager) { m.nodeID = id }
}

// NewManager opens or creates dir, scans for existing *.seg files,
// and reopens the most recent one as the active segment.
// Creates the first segment if the directory is empty.
func NewManager(dir string, maxSegmentBytes int64, opts ...ManagerOption) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", dir, err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil {
		return nil, fmt.Errorf("glob segments: %w", err)
	}
	sort.Strings(matches)

	nextSeq, err := nextSeqFromMatches(matches)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		dir:             dir,
		maxSegmentBytes: maxSegmentBytes,
		paths:           matches,
		nextSeq:         nextSeq,
		bloomEnabled:    os.Getenv("BLOOM_ENABLED") == "true",
		blooms:          make(map[string]*bloom.BloomFilter),
	}
	for _, opt := range opts {
		opt(m)
	}

	if len(matches) == 0 {
		if err := m.openNewSegment(); err != nil {
			return nil, err
		}
	} else {
		seg, err := OpenSegment(matches[len(matches)-1])
		if err != nil {
			return nil, fmt.Errorf("reopen active segment: %w", err)
		}
		m.active = seg
	}

	// Load bloom sidecars for all closed segments (all except the active one).
	if m.bloomEnabled {
		closedPaths := m.paths
		if len(closedPaths) > 0 {
			closedPaths = closedPaths[:len(closedPaths)-1]
		}
		for _, p := range closedPaths {
			if bf, err := ReadBloom(BloomPath(p)); err == nil {
				m.blooms[p] = bf
			}
		}
	}

	observability.MountedSegmentsTotal.WithLabelValues(m.nodeID).Set(float64(len(m.paths)))
	return m, nil
}

// Append serializes entry to protobuf and appends to the active segment.
// Rotates to a new segment if the size threshold would be crossed.
func (m *Manager) Append(entry *types.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appendLocked(entry)
}

// AppendWithPath writes entry to the active segment and returns the segment
// path used for the write. The path is captured atomically under the
// manager's lock, so the returned path is guaranteed to match the segment
// the entry was actually written to.
func (m *Manager) AppendWithPath(entry *types.LogEntry) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.appendLocked(entry); err != nil {
		return "", err
	}
	return m.paths[len(m.paths)-1], nil
}

// appendLocked performs the actual append. Must be called with m.mu held.
func (m *Manager) appendLocked(entry *types.LogEntry) error {
	if m.active == nil {
		return fmt.Errorf("append to closed manager")
	}

	pb := &logengine.LogEntry{
		Id:         entry.ID,
		Timestamp:  entry.Timestamp,
		ReceivedAt: entry.ReceivedAt,
		Namespace:  entry.Namespace,
		Service:    entry.Service,
		Level:      entry.Level,
		Message:    entry.Message,
		Fields:     entry.Fields,
	}

	data, err := proto.Marshal(pb)
	if err != nil {
		return fmt.Errorf("marshal log entry: %w", err)
	}

	recordSize := int64(4 + len(data))
	if m.active.Size()+recordSize > m.maxSegmentBytes {
		if err := m.rotate(); err != nil {
			return fmt.Errorf("rotate segment: %w", err)
		}
	}

	start := time.Now()
	if err := m.active.Append(data); err != nil {
		return err
	}
	observability.AppendDuration.WithLabelValues(m.nodeID).Observe(time.Since(start).Seconds())
	observability.ActiveSegmentBytes.WithLabelValues(m.nodeID).Set(float64(m.active.Size()))
	return nil
}

// SegmentPaths returns the absolute paths of all segment files in sequence order.
func (m *Manager) SegmentPaths() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.paths))
	copy(out, m.paths)
	return out
}

// Close closes the active segment.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil {
		return nil
	}
	err := m.active.Close()
	m.active = nil
	return err
}

func (m *Manager) openNewSegment() error {
	name := fmt.Sprintf(segmentNameFmt, m.nextSeq)
	path := filepath.Join(m.dir, name)
	seg, err := OpenSegment(path)
	if err != nil {
		return fmt.Errorf("open new segment %s: %w", name, err)
	}
	m.active = seg
	m.paths = append(m.paths, path)
	m.nextSeq++
	return nil
}

func (m *Manager) rotate() error {
	closingPath := m.paths[len(m.paths)-1]
	if err := m.active.Close(); err != nil {
		return fmt.Errorf("close active segment before rotation: %w", err)
	}

	if m.bloomEnabled {
		if entries, err := ReadSegment(closingPath); err == nil {
			var tokens []string
			for _, e := range entries {
				tokens = append(tokens, tokenizeEntry(e)...)
			}
			bf := BuildBloom(tokens, uint(len(tokens)))
			if err := WriteBloom(BloomPath(closingPath), bf); err == nil {
				m.blooms[closingPath] = bf
			}
		}
	}

	if err := m.openNewSegment(); err != nil {
		return err
	}
	observability.MountedSegmentsTotal.WithLabelValues(m.nodeID).Set(float64(len(m.paths)))
	return nil
}

// BloomFor returns the bloom filter for the given segment path, or nil if not loaded.
func (m *Manager) BloomFor(segPath string) *bloom.BloomFilter {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.blooms[segPath]
}

func tokenizeEntry(e *types.LogEntry) []string {
	var out []string
	lower := strings.ToLower(e.Message)
	for _, tok := range nonAlphanumericRe.Split(lower, -1) {
		if tok != "" {
			out = append(out, tok)
		}
	}
	out = append(out, strings.ToLower(e.Level))
	out = append(out, strings.ToLower(e.Service))
	out = append(out, strings.ToLower(e.Namespace))
	return out
}

// marshalLogEntry serializes e to protobuf bytes. Used by compaction.
func marshalLogEntry(e *types.LogEntry) ([]byte, error) {
	pb := &logengine.LogEntry{
		Id:         e.ID,
		Timestamp:  e.Timestamp,
		ReceivedAt: e.ReceivedAt,
		Namespace:  e.Namespace,
		Service:    e.Service,
		Level:      e.Level,
		Message:    e.Message,
		Fields:     e.Fields,
	}
	data, err := proto.Marshal(pb)
	if err != nil {
		return nil, fmt.Errorf("marshal log entry: %w", err)
	}
	return data, nil
}

// ListClosedSegments returns paths of all segments except the active one.
func (m *Manager) ListClosedSegments() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.paths) <= 1 {
		return nil
	}
	out := make([]string, len(m.paths)-1)
	copy(out, m.paths[:len(m.paths)-1])
	return out
}

// RemapSegment replaces oldPath with newPath in the manager's path list and bloom map.
func (m *Manager) RemapSegment(oldPath, newPath string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, p := range m.paths {
		if p == oldPath {
			m.paths[i] = newPath
		}
	}
	if bf, ok := m.blooms[oldPath]; ok {
		m.blooms[newPath] = bf
		delete(m.blooms, oldPath)
	}
}

// DeleteSegment removes path from the manager's path list and bloom map.
// Does NOT delete the file from disk — caller is responsible.
func (m *Manager) DeleteSegment(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.paths[:0]
	for _, p := range m.paths {
		if p != path {
			out = append(out, p)
		}
	}
	m.paths = out
	delete(m.blooms, path)
}

// LoadSegment registers a newly transferred segment file with the manager.
// The segment must already exist on disk.
func (m *Manager) LoadSegment(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	active := m.paths[len(m.paths)-1]
	rest := m.paths[:len(m.paths)-1]
	rest = append(rest, path)
	m.paths = append(rest, active)
	if m.bloomEnabled {
		if bf, err := ReadBloom(BloomPath(path)); err == nil {
			m.blooms[path] = bf
		}
	}
	return nil
}

// Dir returns the data directory path.
func (m *Manager) Dir() string {
	return m.dir
}

// nextSeqFromMatches returns the next sequence number by parsing the last filename.
// Falls back to 1 if matches is empty.
func nextSeqFromMatches(matches []string) (uint64, error) {
	if len(matches) == 0 {
		return 1, nil
	}
	base := filepath.Base(matches[len(matches)-1])
	base = strings.TrimSuffix(base, ".seg")
	n, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse segment sequence from %q: %w", base, err)
	}
	return n + 1, nil
}
