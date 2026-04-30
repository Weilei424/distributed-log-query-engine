package storage

import (
	"fmt"
	"os"
	"strings"

	"github.com/bits-and-blooms/bloom/v3"
)

// BuildBloom creates a bloom filter from the given token slice.
// expectedN is the estimated total number of distinct tokens for sizing.
func BuildBloom(tokens []string, expectedN uint) *bloom.BloomFilter {
	if expectedN < 100 {
		expectedN = 100
	}
	bf := bloom.NewWithEstimates(expectedN, 0.01)
	for _, tok := range tokens {
		bf.AddString(strings.ToLower(tok))
	}
	return bf
}

// WriteBloom serializes bf to path atomically (write to .tmp, rename).
func WriteBloom(path string, bf *bloom.BloomFilter) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create bloom tmp %s: %w", tmp, err)
	}
	if _, err := bf.WriteTo(f); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write bloom %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("sync bloom %s: %w", path, err)
	}
	f.Close()
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename bloom %s: %w", path, err)
	}
	return nil
}

// ReadBloom deserializes a bloom filter from path.
func ReadBloom(path string) (*bloom.BloomFilter, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open bloom %s: %w", path, err)
	}
	defer f.Close()
	bf := &bloom.BloomFilter{}
	if _, err := bf.ReadFrom(f); err != nil {
		return nil, fmt.Errorf("read bloom %s: %w", path, err)
	}
	return bf, nil
}

// BloomPath returns the sidecar bloom path for a given segment path.
// e.g. "/data/00000000000000000001.seg" → "/data/00000000000000000001.bloom"
func BloomPath(segPath string) string {
	return strings.TrimSuffix(segPath, ".seg") + ".bloom"
}
