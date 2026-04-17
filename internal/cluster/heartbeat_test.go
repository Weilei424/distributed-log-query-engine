package cluster_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Weilei424/distributed-log-query-engine/internal/cluster"
)

// stubSender counts calls and never errors.
type stubSender struct {
	calls int64
}

func (s *stubSender) SendHeartbeat(ctx context.Context) error {
	atomic.AddInt64(&s.calls, 1)
	return nil
}

func TestHeartbeatSender_StopsOnCancel(t *testing.T) {
	stub := &stubSender{}
	sender := cluster.NewHeartbeatSender(stub, 20*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sender.Run(ctx)
		close(done)
	}()

	time.Sleep(70 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("HeartbeatSender did not stop after context cancel")
	}

	calls := atomic.LoadInt64(&stub.calls)
	if calls < 2 {
		t.Errorf("expected at least 2 heartbeat calls in 70ms, got %d", calls)
	}
}
