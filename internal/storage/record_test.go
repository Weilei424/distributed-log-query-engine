package storage_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func TestWriteReadRecord_RoundTrip(t *testing.T) {
	data := []byte("hello world")
	var buf bytes.Buffer
	if err := storage.WriteRecord(&buf, data); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	got, err := storage.ReadRecord(&buf)
	if err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestReadRecord_EOF(t *testing.T) {
	var buf bytes.Buffer
	_, err := storage.ReadRecord(&buf)
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestWriteReadRecord_EmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	if err := storage.WriteRecord(&buf, []byte{}); err != nil {
		t.Fatalf("WriteRecord empty: %v", err)
	}
	got, err := storage.ReadRecord(&buf)
	if err != nil {
		t.Fatalf("ReadRecord empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty payload, got %q", got)
	}
}

func TestReadRecord_TruncatedLength(t *testing.T) {
	buf := bytes.NewReader([]byte{0x00, 0x00}) // only 2 of 4 bytes
	_, err := storage.ReadRecord(buf)
	if err == nil || err == io.EOF {
		t.Fatalf("expected non-EOF error for truncated length, got %v", err)
	}
}

func TestReadRecord_TruncatedData(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0x00, 0x00, 0x00, 0x0a}) // length = 10
	buf.Write([]byte{0x01, 0x02, 0x03})       // only 3 bytes of data
	_, err := storage.ReadRecord(&buf)
	if err == nil {
		t.Fatal("expected error for truncated data, got nil")
	}
}

func TestWriteReadRecord_MultipleSequential(t *testing.T) {
	records := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}
	var buf bytes.Buffer
	for _, r := range records {
		if err := storage.WriteRecord(&buf, r); err != nil {
			t.Fatalf("WriteRecord: %v", err)
		}
	}
	for i, want := range records {
		got, err := storage.ReadRecord(&buf)
		if err != nil {
			t.Fatalf("ReadRecord[%d]: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("record[%d]: got %q, want %q", i, got, want)
		}
	}
	// Verify EOF after all records consumed
	_, err := storage.ReadRecord(&buf)
	if err != io.EOF {
		t.Errorf("expected io.EOF after all records, got %v", err)
	}
}
