package storage

import (
	"context"
	"os"
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

	// Write merged data to paths[0]+".tmp", then rename atomically to paths[0].
	// Reusing paths[0]'s name keeps the merged segment's sequence number below
	// the active segment's sequence, preserving the restart invariant that the
	// lexicographically last (or ACTIVE-marked) segment is always the active one.
	newPath := paths[0]
	tmpPath := newPath + ".tmp"

	// Remove any leftover temp file from a previous crashed merge so OpenSegment
	// starts from an empty file rather than appending to stale data.
	os.Remove(tmpPath)
	seg, err := OpenSegment(tmpPath)
	if err != nil {
		return
	}

	var tokens []string
	for _, e := range all {
		pb, err := marshalLogEntry(e)
		if err != nil {
			seg.Close()
			os.Remove(tmpPath)
			return
		}
		if err := seg.Append(pb); err != nil {
			seg.Close()
			os.Remove(tmpPath)
			return
		}
		if c.manager.bloomEnabled {
			tokens = append(tokens, tokenizeEntry(e)...)
		}
	}
	seg.Close()

	// Remove ALL merged paths from the index BEFORE renaming the temp file over
	// paths[0]. This closes the duplicate window: no query can simultaneously
	// read merged paths[0] (new data) and still-indexed paths[1:] (old data).
	// There is a brief window where merged data is invisible to new queries;
	// that is acceptable for a background compaction operation.
	if c.idx != nil {
		for _, op := range paths {
			c.idx.RemoveSegment(op)
		}
	}

	if err := os.Rename(tmpPath, newPath); err != nil {
		// Rename failed; restore the index from the still-present old files.
		os.Remove(tmpPath)
		if c.idx != nil {
			for _, op := range paths {
				restored, readErr := c.manager.ReadSegments([]string{op})
				if readErr == nil {
					for _, e := range restored {
						c.idx.Add(e, op)
					}
				}
			}
		}
		return
	}

	// Write bloom sidecar for paths[0] (the merged output).
	if c.manager.bloomEnabled && len(tokens) > 0 {
		bf := BuildBloom(tokens, uint(len(tokens)))
		_ = WriteBloom(BloomPath(newPath), bf)
	}

	// Re-add all merged entries under paths[0]. New queries can now see the
	// merged data. Entries from paths[0]'s old content are implicitly included
	// since all was read from all paths before the merge.
	if c.idx != nil {
		for _, e := range all {
			c.idx.Add(e, newPath)
		}
	}

	// Update manager path list and bloom map (removes paths[1:]).
	c.manager.mu.Lock()
	skipSet := make(map[string]bool, len(paths)-1)
	for _, op := range paths[1:] {
		skipSet[op] = true
	}
	out := c.manager.paths[:0:len(c.manager.paths)]
	for _, p := range c.manager.paths {
		if !skipSet[p] {
			out = append(out, p)
		}
	}
	c.manager.paths = out
	if c.manager.bloomEnabled {
		for _, op := range paths[1:] {
			delete(c.manager.blooms, op)
		}
		delete(c.manager.blooms, newPath)
		if bf, err := ReadBloom(BloomPath(newPath)); err == nil {
			c.manager.blooms[newPath] = bf
		}
	}
	c.manager.mu.Unlock()

	// Disk deletes are last: index and manager no longer reference paths[1:].
	for _, op := range paths[1:] {
		os.Remove(op)
		os.Remove(BloomPath(op))
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
			// Remove from manager and index before disk delete so concurrent
			// readers never get a file-not-found for a path they just resolved.
			c.manager.DeleteSegment(path)
			if c.idx != nil {
				c.idx.RemoveSegment(path)
			}
			os.Remove(path)
			os.Remove(BloomPath(path))
		}
	}
}

// Ensure types import is used (types.LogEntry in mergeRun parameter).
var _ *types.LogEntry
