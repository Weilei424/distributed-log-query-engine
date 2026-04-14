package storage_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Weilei424/distributed-log-query-engine/internal/storage"
)

func TestSegment_AppendAndReadBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000000000000000001.seg")

	seg, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	data := []byte("test record")
	if err := seg.Append(data); err != nil {
		t.Fatalf("Append: %v", err)
	}
	seg.Close()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open for read: %v", err)
	}
	defer f.Close()

	got, err := storage.ReadRecord(f)
	if err != nil {
		t.Fatalf("ReadRecord: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestSegment_SizeGrows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000000000000000001.seg")

	seg, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()

	if seg.Size() != 0 {
		t.Errorf("initial size: got %d, want 0", seg.Size())
	}

	data := []byte("hello")
	if err := seg.Append(data); err != nil {
		t.Fatalf("Append: %v", err)
	}

	want := int64(4 + len(data)) // 4-byte header + data
	if seg.Size() != want {
		t.Errorf("size after append: got %d, want %d", seg.Size(), want)
	}
}

func TestSegment_MultipleAppendsSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000000000000000001.seg")

	records := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	seg, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	for _, r := range records {
		if err := seg.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	seg.Close()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open for read: %v", err)
	}
	defer f.Close()

	for i, want := range records {
		got, err := storage.ReadRecord(f)
		if err != nil {
			t.Fatalf("ReadRecord[%d]: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("record[%d]: got %q, want %q", i, got, want)
		}
	}
}

func TestSegment_ReopenAppendsToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00000000000000000001.seg")

	seg, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment first: %v", err)
	}
	if err := seg.Append([]byte("before")); err != nil {
		t.Fatalf("Append before: %v", err)
	}
	seg.Close()

	seg2, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment second: %v", err)
	}
	if err := seg2.Append([]byte("after")); err != nil {
		t.Fatalf("Append after: %v", err)
	}
	seg2.Close()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open for read: %v", err)
	}
	defer f.Close()

	for _, want := range [][]byte{[]byte("before"), []byte("after")} {
		got, err := storage.ReadRecord(f)
		if err != nil {
			t.Fatalf("ReadRecord: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
