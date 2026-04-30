package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// CompactorConfig controls compaction behavior.
type CompactorConfig struct {
	// MergeThresholdBytes: segments smaller than this are eligible for merging. 0 disables.
	MergeThresholdBytes int64
	// RetentionDays: segments whose newest entry is older than this many days are deleted. 0 disables.
	RetentionDays int
	// IntervalSeconds: how often to run both passes. 0 means manual-trigger only (for tests).
	IntervalSeconds int
}

// DefaultCompactorConfig returns sane production defaults.
func DefaultCompactorConfig() CompactorConfig {
	return CompactorConfig{
		MergeThresholdBytes: 32 * 1024 * 1024,
		RetentionDays:       7,
		IntervalSeconds:     300,
	}
}

// Compactor runs merge and retention passes over closed segments on a configurable interval.
type Compactor struct {
	manager *Manager
	idx     *index.Index
	cfg     CompactorConfig
}

// NewCompactor creates a Compactor without index updates.
func NewCompactor(manager *Manager, cfg CompactorConfig) *Compactor {
	return &Compactor{manager: manager, cfg: cfg}
}

// NewCompactorWithIndex creates a Compactor that also updates idx on merge/delete.
func NewCompactorWithIndex(manager *Manager, idx *index.Index, cfg CompactorConfig) *Compactor {
	return &Compactor{manager: manager, idx: idx, cfg: cfg}
}

// Start runs both passes on the configured interval until ctx is canceled.
func (c *Compactor) Start(ctx context.Context) {
	if c.cfg.IntervalSeconds <= 0 {
		return
	}
	ticker := time.NewTicker(time.Duration(c.cfg.IntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runMergePass()
			c.runRetentionPass()
		}
	}
}

// runMergePass merges contiguous runs of small closed segments.
func (c *Compactor) runMergePass() {
	if c.cfg.MergeThresholdBytes <= 0 {
		return
	}
	closed := c.manager.ListClosedSegments()
	if len(closed) < 2 {
		return
	}

	var run []string
	flush := func() {
		if len(run) >= 2 {
			c.mergeRun(run)
		}
		run = nil
	}
	for _, path := range closed {
		info, err := os.Stat(path)
		if err != nil {
			flush()
			continue
		}
		if info.Size() < c.cfg.MergeThresholdBytes {
			run = append(run, path)
		} else {
			flush()
		}
	}
	flush()
}

func (c *Compactor) mergeRun(paths []string) {
	all, err := c.manager.ReadSegments(paths)
	if err != nil {
		return
	}

	// Allocate a numeric segment name from the manager's sequence counter so
	// that restarts can always parse the filename as a uint64.
	c.manager.mu.Lock()
	newPath := filepath.Join(c.manager.dir, fmt.Sprintf(segmentNameFmt, c.manager.nextSeq))
	c.manager.nextSeq++
	c.manager.mu.Unlock()

	seg, err := OpenSegment(newPath)
	if err != nil {
		return
	}

	var tokens []string
	for _, e := range all {
		pb, err := marshalLogEntry(e)
		if err != nil {
			seg.Close()
			os.Remove(newPath)
			return
		}
		if err := seg.Append(pb); err != nil {
			seg.Close()
			os.Remove(newPath)
			return
		}
		if c.manager.bloomEnabled {
			tokens = append(tokens, tokenizeEntry(e)...)
		}
	}
	seg.Close()

	if c.manager.bloomEnabled && len(tokens) > 0 {
		bf := BuildBloom(tokens, uint(len(tokens)))
		_ = WriteBloom(BloomPath(newPath), bf)
	}

	// Atomically swap old paths for new path in the manager.
	c.manager.mu.Lock()
	active := c.manager.paths[len(c.manager.paths)-1]
	var kept []string
	for _, p := range c.manager.paths[:len(c.manager.paths)-1] {
		skip := false
		for _, op := range paths {
			if p == op {
				skip = true
				break
			}
		}
		if !skip {
			kept = append(kept, p)
		}
	}
	kept = append(kept, newPath)
	c.manager.paths = append(kept, active)
	if c.manager.bloomEnabled {
		if bf, err := ReadBloom(BloomPath(newPath)); err == nil {
			c.manager.blooms[newPath] = bf
		}
		for _, op := range paths {
			delete(c.manager.blooms, op)
		}
	}
	c.manager.mu.Unlock()

	for _, op := range paths {
		os.Remove(op)
		os.Remove(BloomPath(op))
		if c.idx != nil {
			c.idx.RemoveSegment(op)
		}
	}

	if c.idx != nil {
		for _, e := range all {
			c.idx.Add(e, newPath)
		}
	}
}

// runRetentionPass deletes closed segments older than RetentionDays.
func (c *Compactor) runRetentionPass() {
	if c.cfg.RetentionDays <= 0 {
		return
	}
	cutoff := time.Now().Add(-time.Duration(c.cfg.RetentionDays) * 24 * time.Hour).UnixNano()
	closed := c.manager.ListClosedSegments()
	for _, path := range closed {
		entries, err := c.manager.ReadSegments([]string{path})
		if err != nil {
			continue
		}
		maxTS := int64(0)
		for _, e := range entries {
			if e.Timestamp > maxTS {
				maxTS = e.Timestamp
			}
		}
		if maxTS > 0 && maxTS < cutoff {
			c.manager.DeleteSegment(path)
			os.Remove(path)
			os.Remove(BloomPath(path))
			if c.idx != nil {
				c.idx.RemoveSegment(path)
			}
		}
	}
}

// Ensure types import is used (types.LogEntry in mergeRun parameter).
var _ *types.LogEntry
