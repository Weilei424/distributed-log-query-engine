// internal/ingest/router.go
package ingest

import "hash/fnv"

// ShardID computes the shard ID for a log entry based on its service name.
// Uses FNV-1a hash modulo totalShards. Deterministic across all nodes:
// given the same service and total shard count, every node returns the same ID.
func ShardID(service string, totalShards int) int {
	if totalShards <= 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(service))
	return int(h.Sum32()) % totalShards
}
