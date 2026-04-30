package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBloomRoundTrip(t *testing.T) {
	tokens := []string{"error", "timeout", "service", "api"}
	bf := BuildBloom(tokens, 1000)
	for _, tok := range tokens {
		if !bf.TestString(tok) {
			t.Fatalf("bloom should contain %q", tok)
		}
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test.bloom")
	if err := WriteBloom(path, bf); err != nil {
		t.Fatalf("WriteBloom: %v", err)
	}

	bf2, err := ReadBloom(path)
	if err != nil {
		t.Fatalf("ReadBloom: %v", err)
	}
	for _, tok := range tokens {
		if !bf2.TestString(tok) {
			t.Fatalf("after round-trip: bloom should contain %q", tok)
		}
	}
}

func TestBloomPath(t *testing.T) {
	got := BloomPath("/data/00000000000000000001.seg")
	want := "/data/00000000000000000001.bloom"
	if got != want {
		t.Fatalf("want %s, got %s", want, got)
	}
}

func TestReadBloom_MissingFile(t *testing.T) {
	_, err := ReadBloom("/nonexistent/path.bloom")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestWriteBloom_CreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bloom")
	bf := BuildBloom([]string{"hello"}, 100)
	if err := WriteBloom(path, bf); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file not created: %v", err)
	}
}
