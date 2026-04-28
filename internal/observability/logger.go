package observability

import "go.uber.org/zap"

// NewLogger returns a production zap logger pre-seeded with component and node_id fields.
func NewLogger(component, nodeID string) *zap.Logger {
	l := zap.Must(zap.NewProduction())
	return l.With(zap.String("component", component), zap.String("node_id", nodeID))
}
