package types

// QueryRequest describes the parameters for a local log query.
// StartTime and EndTime are Unix nanoseconds; zero means unbounded.
// Limit of zero uses the server default (100).
type QueryRequest struct {
	QueryString string
	Namespace   string
	Service     string
	StartTime   int64
	EndTime     int64
	Limit       int32
	Offset      int32
}

// QueryResult holds the output of a log query.
type QueryResult struct {
	Entries []*LogEntry
	Total   int32 // lower-bound candidate count before limit/offset (see Architecture Notes Decision 7)
	TookMs  int64
	Partial bool // true if one or more source nodes did not respond within the deadline
}
