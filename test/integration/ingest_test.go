package integration_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	logengine "github.com/Weilei424/distributed-log-query-engine/internal/api/gen/logengine/v1"
	"github.com/Weilei424/distributed-log-query-engine/internal/ingest"
	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func TestIngestAndPersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: ingest entries
	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	srv := ingest.NewServer(m)

	entries := []*logengine.LogEntry{
		{Id: "e1", Service: "svc-a", Level: "INFO", Message: "first message"},
		{Id: "e2", Service: "svc-b", Level: "WARN", Message: "second message"},
		{Id: "e3", Service: "svc-a", Level: "ERROR", Message: "third message"},
	}

	for _, e := range entries {
		if _, err := srv.Ingest(context.Background(), &logengine.IngestRequest{Entry: e}); err != nil {
			t.Fatalf("Ingest %s: %v", e.Id, err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: reopen and verify records on disk
	m2, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager reopen: %v", err)
	}
	t.Cleanup(func() { m2.Close() })

	var found []*logengine.LogEntry
	for _, path := range m2.SegmentPaths() {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", filepath.Base(path), err)
		}
		for {
			data, err := storage.ReadRecord(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("ReadRecord in %s: %v", filepath.Base(path), err)
			}
			var pb logengine.LogEntry
			if err := proto.Unmarshal(data, &pb); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			found = append(found, &pb)
		}
		f.Close()
	}

	if len(found) != len(entries) {
		t.Fatalf("expected %d records after restart, got %d", len(entries), len(found))
	}

	for i, want := range entries {
		got := found[i]
		if got.Id != want.Id {
			t.Errorf("record[%d] Id: got %q, want %q", i, got.Id, want.Id)
		}
		if got.Service != want.Service {
			t.Errorf("record[%d] Service: got %q, want %q", i, got.Service, want.Service)
		}
		if got.Message != want.Message {
			t.Errorf("record[%d] Message: got %q, want %q", i, got.Message, want.Message)
		}
		if got.ReceivedAt == 0 {
			t.Errorf("record[%d] ReceivedAt should be non-zero", i)
		}
	}
}

func TestIngestBatch_AllEntriesOnDisk(t *testing.T) {
	dir := t.TempDir()

	m, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	srv := ingest.NewServer(m)

	batch := &logengine.IngestBatchRequest{
		Entries: []*logengine.LogEntry{
			{Id: "b1", Service: "svc", Message: "batch one"},
			{Id: "b2", Service: "svc", Message: "batch two"},
			{Id: "b3", Service: "svc", Message: "batch three"},
		},
	}

	resp, err := srv.IngestBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	if resp.Accepted != 3 {
		t.Errorf("expected Accepted=3, got %d", resp.Accepted)
	}
	if resp.Rejected != 0 {
		t.Errorf("expected Rejected=0, got %d", resp.Rejected)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify all 3 are on disk
	m2, err := storage.NewManager(dir, 64*1024*1024)
	if err != nil {
		t.Fatalf("NewManager reopen: %v", err)
	}
	t.Cleanup(func() { m2.Close() })

	count := 0
	for _, path := range m2.SegmentPaths() {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", filepath.Base(path), err)
		}
		for {
			_, err := storage.ReadRecord(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("ReadRecord: %v", err)
			}
			count++
		}
		f.Close()
	}

	if count != 3 {
		t.Errorf("expected 3 records on disk, got %d", count)
	}
}
