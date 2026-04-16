package types

// QueryRequest describes the parameters for a local log query.
// StartTime and EndTime are Unix nanoseconds; zero means unbounded.
// Limit of zero uses the server default (100).
type QueryRequest struct {
	Keyword   string
	Service   string
	StartTime int64
	EndTime   int64
	Limit     int32
	Offset    int32
}

// QueryResult holds the output of a local log query.
type QueryResult struct {
	Entries []*LogEntry
	Total   int32 // total matching entries before limit/offset
	TookMs  int64
}
