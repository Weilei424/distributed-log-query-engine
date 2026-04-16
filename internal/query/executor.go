package query

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

var nonAlphanumeric = regexp.MustCompile(`[^a-z0-9]+`)

// tokenize lowercases s and splits on non-alphanumeric sequences, omitting empty tokens.
// Must stay in sync with index.tokenize so executor and index use identical word boundaries.
func tokenize(s string) []string {
	parts := nonAlphanumeric.Split(strings.ToLower(s), -1)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// tokenSet returns the set of tokens in s, for O(1) membership checks.
func tokenSet(s string) map[string]struct{} {
	toks := tokenize(s)
	set := make(map[string]struct{}, len(toks))
	for _, t := range toks {
		set[t] = struct{}{}
	}
	return set
}

const defaultLimit = 100

// LocalExecutor runs log queries against the local index and segment files.
type LocalExecutor struct {
	index   *index.Index
	manager *storage.Manager
}

// NewLocalExecutor returns a LocalExecutor backed by idx and manager.
func NewLocalExecutor(idx *index.Index, manager *storage.Manager) *LocalExecutor {
	return &LocalExecutor{index: idx, manager: manager}
}

// Execute runs req against the local index and returns matching log entries.
func (e *LocalExecutor) Execute(ctx context.Context, req *types.QueryRequest) (*types.QueryResult, error) {
	start := time.Now()

	if req.Limit < 0 {
		return nil, fmt.Errorf("limit must be non-negative")
	}
	if req.Limit == 0 {
		req.Limit = defaultLimit
	}
	if req.Offset < 0 {
		return nil, fmt.Errorf("offset must be non-negative")
	}

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("query canceled: %w", err)
	}

	paths := e.index.Resolve(req.Keyword, req.Service, req.StartTime, req.EndTime)

	var raw []*types.LogEntry
	if len(paths) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("query canceled before disk read: %w", err)
		}
		var err error
		raw, err = e.manager.ReadSegments(paths)
		if err != nil {
			return nil, fmt.Errorf("execute query: %w", err)
		}
	}

	// Tokenize the keyword once; match requires all keyword tokens to appear as
	// exact words in the message. This is consistent with how the index stores tokens.
	kwTokens := tokenize(req.Keyword)
	filtered := make([]*types.LogEntry, 0, len(raw))
	for _, entry := range raw {
		if len(kwTokens) > 0 {
			msgTokenSet := tokenSet(entry.Message)
			match := true
			for _, kwTok := range kwTokens {
				if _, found := msgTokenSet[kwTok]; !found {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}
		if req.Service != "" && entry.Service != req.Service {
			continue
		}
		if req.StartTime > 0 && entry.Timestamp < req.StartTime {
			continue
		}
		if req.EndTime > 0 && entry.Timestamp > req.EndTime {
			continue
		}
		filtered = append(filtered, entry)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].Timestamp != filtered[j].Timestamp {
			return filtered[i].Timestamp > filtered[j].Timestamp
		}
		return filtered[i].ID < filtered[j].ID // stable tie-breaker for deterministic pagination
	})

	total := int32(len(filtered))

	// Apply offset.
	offset := int(req.Offset)
	if offset > len(filtered) {
		offset = len(filtered)
	}
	filtered = filtered[offset:]

	// Apply limit.
	limit := int(req.Limit)
	if limit > len(filtered) {
		limit = len(filtered)
	}
	filtered = filtered[:limit]

	return &types.QueryResult{
		Entries: filtered,
		Total:   total,
		TookMs:  time.Since(start).Milliseconds(),
	}, nil
}
