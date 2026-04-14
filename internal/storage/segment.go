package storage

import (
	"fmt"
	"io"
	"os"
)

// Segment represents a single open segment file.
type Segment struct {
	file *os.File
	size int64
}

// OpenSegment opens or creates the segment file at path.
// If the file already exists it seeks to the end so appends do not overwrite existing data.
func OpenSegment(path string) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat segment %s: %w", path, err)
	}
	size := info.Size()
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("seek segment %s: %w", path, err)
	}
	return &Segment{file: f, size: size}, nil
}

// Append frames data as a length-prefixed record and syncs to disk.
func (s *Segment) Append(data []byte) error {
	if err := WriteRecord(s.file, data); err != nil {
		return fmt.Errorf("segment append: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("segment sync: %w", err)
	}
	s.size += int64(4 + len(data))
	return nil
}

// Size returns the current byte size of the segment file.
func (s *Segment) Size() int64 {
	return s.size
}

// Close closes the underlying file.
func (s *Segment) Close() error {
	if err := s.file.Close(); err != nil {
		return fmt.Errorf("close segment: %w", err)
	}
	return nil
}
