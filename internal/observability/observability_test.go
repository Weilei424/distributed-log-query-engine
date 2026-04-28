package observability_test

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Weilei424/distributed-log-query-engine/internal/observability"
)

func TestRegister_GathersMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	observability.Register(reg)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	if len(mfs) == 0 {
		t.Fatal("expected at least one metric family after Register")
	}
}

func TestRequestID_RoundTrip(t *testing.T) {
	id := observability.NewRequestID()
	ctx := observability.WithRequestID(context.Background(), id)
	if got := observability.RequestIDFromContext(ctx); got != id {
		t.Fatalf("got %q, want %q", got, id)
	}
}

func TestRequestIDFromContext_Missing(t *testing.T) {
	if got := observability.RequestIDFromContext(context.Background()); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
}

func TestNewLogger_NotNil(t *testing.T) {
	if l := observability.NewLogger("test", "node-1"); l == nil {
		t.Fatal("expected non-nil logger")
	}
}
