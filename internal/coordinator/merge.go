package coordinator

import (
	"sort"

	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// nodeResult holds the query result from one storage node.
type nodeResult struct {
	nodeID  string
	entries []*types.LogEntry
	total   int32
	err     error
}

// mergeOutput is the result of combining results from multiple nodes.
type mergeOutput struct {
	entries []*types.LogEntry
	total   int32
	partial bool
}

// MergeResults combines partial results from multiple nodes into a single
// sorted, deduplicated, paginated result. It is a pure function with no
// external dependencies.
//
// Deduplication is by entry ID (same entry may appear on primary and replica).
// Sort order is timestamp descending, entry ID ascending as tie-breaker.
// total is the sum of per-node totals reported by successful nodes. This is a
// cross-node approximation: replicated entries may inflate the count, but it
// honors the proto contract that total is before offset/limit rather than
// being capped by the candidate window.
// partial is true when any nodeResult has a non-nil err.
// When limit is 0, all entries after the offset are returned.
func MergeResults(parts []nodeResult, offset, limit int32) mergeOutput {
	var partial bool
	var nodeTotal int32
	seen := make(map[string]struct{})
	var combined []*types.LogEntry

	for _, p := range parts {
		if p.err != nil {
			partial = true
			continue
		}
		nodeTotal += p.total
		for _, e := range p.entries {
			if _, ok := seen[e.ID]; ok {
				continue
			}
			seen[e.ID] = struct{}{}
			combined = append(combined, e)
		}
	}

	sort.Slice(combined, func(i, j int) bool {
		if combined[i].Timestamp != combined[j].Timestamp {
			return combined[i].Timestamp > combined[j].Timestamp
		}
		return combined[i].ID < combined[j].ID
	})

	// Fall back to candidate count when nodes report 0 (avoids returning
	// total=0 for nodes that do not populate the field).
	total := nodeTotal
	if total == 0 {
		total = int32(len(combined))
	}

	off := int(offset)
	if off > len(combined) {
		off = len(combined)
	}
	combined = combined[off:]

	if limit > 0 && int(limit) < len(combined) {
		combined = combined[:int(limit)]
	}

	return mergeOutput{
		entries: combined,
		total:   total,
		partial: partial,
	}
}
