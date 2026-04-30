package query

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
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

// matchesNode evaluates whether entry matches the AST node.
func matchesNode(entry *types.LogEntry, node Node) bool {
	if node == nil {
		return true
	}
	switch n := node.(type) {
	case AndNode:
		return matchesNode(entry, n.Left) && matchesNode(entry, n.Right)
	case OrNode:
		return matchesNode(entry, n.Left) || matchesNode(entry, n.Right)
	case TermNode:
		_, ok := tokenSet(entry.Message)[n.Token]
		return ok
	case FieldNode:
		return matchField(entry, n.Field, n.Value)
	}
	return false
}

func matchField(entry *types.LogEntry, field, value string) bool {
	switch strings.ToLower(field) {
	case "level":
		return strings.EqualFold(entry.Level, value)
	case "service":
		return entry.Service == value
	case "namespace":
		return entry.Namespace == value
	case "message":
		_, ok := tokenSet(entry.Message)[strings.ToLower(value)]
		return ok
	default:
		return entry.Fields[field] == value
	}
}

// bloomDefiniteMiss returns true when the bloom filter guarantees no entry
// in this segment can match the AST. Only AND-required TermNodes are checked;
// OR nodes are conservative (never skip).
func bloomDefiniteMiss(bf *bloom.BloomFilter, node Node) bool {
	switch n := node.(type) {
	case AndNode:
		return bloomDefiniteMiss(bf, n.Left) || bloomDefiniteMiss(bf, n.Right)
	case OrNode:
		return false
	case TermNode:
		return !bf.TestString(n.Token)
	case FieldNode:
		return !bf.TestString(strings.ToLower(n.Value))
	}
	return false
}

// collectTermTokens extracts all TermNode tokens from the AST for index lookup.
// For OR nodes, no tokens are returned (conservative: either branch might match).
func collectTermTokens(node Node) []string {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case AndNode:
		left := collectTermTokens(n.Left)
		right := collectTermTokens(n.Right)
		return append(left, right...)
	case OrNode:
		return nil
	case TermNode:
		return []string{n.Token}
	case FieldNode:
		return nil
	}
	return nil
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

	ast, err := Parse(req.QueryString)
	if err != nil {
		return nil, fmt.Errorf("parse query: %w", err)
	}
	var indexTokens []string
	if ast != nil {
		indexTokens = collectTermTokens(ast)
	}
	paths := e.index.Resolve(indexTokens, req.Namespace, req.Service, req.StartTime, req.EndTime)

	// Bloom-prune: skip segments where bloom guarantees no match.
	prunedPaths := paths[:0:len(paths)]
	for _, p := range paths {
		if bf := e.manager.BloomFor(p); bf != nil && ast != nil {
			if bloomDefiniteMiss(bf, ast) {
				continue
			}
		}
		prunedPaths = append(prunedPaths, p)
	}

	var raw []*types.LogEntry
	if len(prunedPaths) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("query canceled before disk read: %w", err)
		}
		var err error
		raw, err = e.manager.ReadSegments(prunedPaths)
		if err != nil {
			return nil, fmt.Errorf("execute query: %w", err)
		}
	}

	filtered := make([]*types.LogEntry, 0, len(raw))
	for _, entry := range raw {
		if req.Namespace != "" && entry.Namespace != req.Namespace {
			continue
		}
		if req.Service != "" && entry.Service != req.Service {
			continue
		}
		if !matchesNode(entry, ast) {
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
