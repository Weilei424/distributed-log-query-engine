package storage

import (
	"fmt"
	"io"
	"os"

	"google.golang.org/protobuf/proto"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// ReadSegment reads all log entries from the segment file at path.
// Returns an empty slice without error for a zero-byte file.
func ReadSegment(path string) ([]*types.LogEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	defer f.Close()

	var entries []*types.LogEntry
	for {
		data, err := ReadRecord(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read record from %s: %w", path, err)
		}
		var pb logengine.LogEntry
		if err := proto.Unmarshal(data, &pb); err != nil {
			return nil, fmt.Errorf("unmarshal record from %s: %w", path, err)
		}
		entries = append(entries, storageProtoToEntry(&pb))
	}
	return entries, nil
}

// storageProtoToEntry converts a proto LogEntry to *types.LogEntry.
func storageProtoToEntry(pb *logengine.LogEntry) *types.LogEntry {
	return &types.LogEntry{
		ID:         pb.Id,
		Timestamp:  pb.Timestamp,
		ReceivedAt: pb.ReceivedAt,
		Service:    pb.Service,
		Level:      pb.Level,
		Message:    pb.Message,
		Fields:     pb.Fields,
	}
}

// ReadSegments reads all entries from the given segment paths in order.
// Returns a wrapped error if any segment cannot be read.
func (m *Manager) ReadSegments(paths []string) ([]*types.LogEntry, error) {
	var all []*types.LogEntry
	for _, path := range paths {
		entries, err := ReadSegment(path)
		if err != nil {
			return nil, fmt.Errorf("read segments: %w", err)
		}
		all = append(all, entries...)
	}
	return all, nil
}

// ActiveSegmentPath returns the absolute path of the currently active segment.
// Returns an empty string if the manager is closed.
func (m *Manager) ActiveSegmentPath() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil || len(m.paths) == 0 {
		return ""
	}
	return m.paths[len(m.paths)-1]
}
