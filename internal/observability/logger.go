package observability

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger returns a production zap logger pre-seeded with component and node_id fields.
// Keys use the stable names from the Phase 7 spec: "timestamp", "level", "message".
func NewLogger(component, nodeID string) *zap.Logger {
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.TimeKey = "timestamp"
	cfg.EncoderConfig.MessageKey = "message"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	l := zap.Must(cfg.Build())
	return l.With(zap.String("component", component), zap.String("node_id", nodeID))
}
