package ingest_test

import (
	"context"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/index"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func newTestServer(t *testing.T) *ingest.Server {
	t.Helper()
	m, err := storage.NewManager(t.TempDir(), 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return ingest.NewServer(m, index.NewIndex())
}

func TestIngest_Success(t *testing.T) {
	srv := newTestServer(t)
	resp, err := srv.Ingest(context.Background(), &logengine.IngestRequest{
		Entry: &logengine.LogEntry{
			Id:      "abc123",
			Service: "test-svc",
			Message: "hello",
		},
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if !resp.Ok {
		t.Error("expected Ok=true")
	}
	if resp.Id != "abc123" {
		t.Errorf("expected Id=abc123, got %q", resp.Id)
	}
}

func TestIngest_NilEntry(t *testing.T) {
	srv := newTestServer(t)
	_, err := srv.Ingest(context.Background(), &logengine.IngestRequest{})
	if err == nil {
		t.Fatal("expected error for nil entry")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestIngest_MissingService(t *testing.T) {
	srv := newTestServer(t)
	_, err := srv.Ingest(context.Background(), &logengine.IngestRequest{
		Entry: &logengine.LogEntry{Message: "no service"},
	})
	if err == nil {
		t.Fatal("expected error for missing service")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestIngest_MissingMessage(t *testing.T) {
	srv := newTestServer(t)
	_, err := srv.Ingest(context.Background(), &logengine.IngestRequest{
		Entry: &logengine.LogEntry{Service: "svc"},
	})
	if err == nil {
		t.Fatal("expected error for missing message")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", err)
	}
}

func TestIngest_ReceivedAtIsSet(t *testing.T) {
	dir := t.TempDir()
	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	srv := ingest.NewServer(m, index.NewIndex())

	before := time.Now().UnixNano()
	resp, err := srv.Ingest(context.Background(), &logengine.IngestRequest{
		Entry: &logengine.LogEntry{Service: "svc", Message: "msg"}, // id omitted; server auto-generates
	})
	after := time.Now().UnixNano()
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Response must carry the generated ID, not an empty string.
	if resp.Id == "" {
		t.Error("expected non-empty response ID for auto-generated entry")
	}

	// Read back and verify ReceivedAt is in [before, after] and stored ID matches response.
	paths := m.SegmentPaths()
	if len(paths) == 0 {
		t.Fatal("no segment paths available")
	}
	f, err := os.Open(paths[0])
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	defer f.Close()
	data, err := storage.ReadRecord(f)
	if err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	var pb logengine.LogEntry
	if err := proto.Unmarshal(data, &pb); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if pb.ReceivedAt < before || pb.ReceivedAt > after {
		t.Errorf("ReceivedAt=%d not in [%d, %d]", pb.ReceivedAt, before, after)
	}
	if pb.Id == "" {
		t.Error("expected non-empty ID in stored proto for auto-generated entry")
	}
	if pb.Id != resp.Id {
		t.Errorf("stored ID %q does not match response ID %q", pb.Id, resp.Id)
	}
}

func TestIngestBatch_CountsAcceptedAndRejected(t *testing.T) {
	srv := newTestServer(t)
	resp, err := srv.IngestBatch(context.Background(), &logengine.IngestBatchRequest{
		Entries: []*logengine.LogEntry{
			{Id: "1", Service: "svc", Message: "ok"},
			{Id: "2", Message: "missing service"}, // rejected
			{Id: "3", Service: "svc", Message: "ok"},
		},
	})
	if err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	if resp.Accepted != 2 {
		t.Errorf("expected Accepted=2, got %d", resp.Accepted)
	}
	if resp.Rejected != 1 {
		t.Errorf("expected Rejected=1, got %d", resp.Rejected)
	}
}
