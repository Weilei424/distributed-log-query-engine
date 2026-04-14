package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

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
}

// NewManager opens or creates dir, scans for existing *.seg files,
// and reopens the most recent one as the active segment.
// Creates the first segment if the directory is empty.
func NewManager(dir string, maxSegmentBytes int64) (*Manager, error) {
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

	return m, nil
}

// Append serializes entry to protobuf and appends to the active segment.
// Rotates to a new segment if the size threshold would be crossed.
func (m *Manager) Append(entry *types.LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.active == nil {
		return fmt.Errorf("append to closed manager")
	}

	pb := &logengine.LogEntry{
		Id:         entry.ID,
		Timestamp:  entry.Timestamp,
		ReceivedAt: entry.ReceivedAt,
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

	return m.active.Append(data)
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
	if err := m.active.Close(); err != nil {
		return fmt.Errorf("close active segment before rotation: %w", err)
	}
	return m.openNewSegment()
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
