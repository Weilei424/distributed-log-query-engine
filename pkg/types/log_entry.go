// Package types defines shared domain types used across internal packages.
// These types are intentionally decoupled from generated protobuf code so that
// core packages can be tested and reasoned about without proto dependencies.
package types

// LogEntry represents a single log record in the system.
// Timestamp and ReceivedAt are Unix nanoseconds.
// Timestamp is assigned by the producer; ReceivedAt is assigned by the ingest path.
type LogEntry struct {
	ID         string
	Timestamp  int64
	ReceivedAt int64
	Service    string
	Level      string
	Message    string
	Fields     map[string]string
}
