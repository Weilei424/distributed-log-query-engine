package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// WriteRecord writes data to w as a length-prefixed record.
// Format: [4-byte big-endian uint32 length][data bytes]
func WriteRecord(w io.Writer, data []byte) error {
	if len(data) > math.MaxUint32 {
		return fmt.Errorf("write record: payload length %d overflows uint32", len(data))
	}
	length := uint32(len(data))
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return fmt.Errorf("write record length: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("write record data: %w", err)
	}
	return nil
}

// ReadRecord reads one length-prefixed record from r.
// Returns io.EOF when r is exhausted at a record boundary.
// Returns a wrapped error (not io.EOF) if the stream is truncated mid-record.
func ReadRecord(r io.Reader) ([]byte, error) {
	const maxRecordSize = 64 * 1024 * 1024 // 64 MiB
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		if err == io.EOF {
			return nil, io.EOF // clean record boundary
		}
		return nil, fmt.Errorf("read record length: %w", err)
	}
	if length > maxRecordSize {
		return nil, fmt.Errorf("read record: payload length %d exceeds maximum %d bytes", length, maxRecordSize)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("read record data (expected %d bytes): %w", length, err)
	}
	return data, nil
}
