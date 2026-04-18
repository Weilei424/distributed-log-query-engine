// internal/ingest/convert.go
package ingest

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/pkg/types"
)

// ProtoToEntry converts a proto LogEntry to the internal types.LogEntry.
func ProtoToEntry(pb *logengine.LogEntry) *types.LogEntry {
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

// EntryToProto converts an internal types.LogEntry to proto LogEntry.
func EntryToProto(e *types.LogEntry) *logengine.LogEntry {
	return &logengine.LogEntry{
		Id:         e.ID,
		Timestamp:  e.Timestamp,
		ReceivedAt: e.ReceivedAt,
		Service:    e.Service,
		Level:      e.Level,
		Message:    e.Message,
		Fields:     e.Fields,
	}
}

// GenerateID returns a random ID for entries that omit one on ingest.
// Format: "auto-<16 hex chars>".
func GenerateID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("auto-%d", time.Now().UnixNano())
	}
	return "auto-" + hex.EncodeToString(b)
}
