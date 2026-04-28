package observability

import (
	"context"
	"fmt"
	"math/rand"
)

type contextKey int

const requestIDKey contextKey = 0

// NewRequestID returns a random hex request ID.
func NewRequestID() string {
	return fmt.Sprintf("req-%x", rand.Uint64())
}

// WithRequestID attaches id to ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestIDFromContext returns the request ID stored in ctx, or "" if absent.
func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey).(string)
	return v
}
