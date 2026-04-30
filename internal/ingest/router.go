// internal/ingest/router.go
package ingest

import "hash/fnv"

// ShardID computes the shard ID for a log entry based on its namespace and service.
// Uses FNV-1a hash of "namespace:service" modulo totalShards.
// Deterministic across all nodes: given the same namespace, service, and total
// shard count, every node returns the same ID.
func ShardID(namespace, service string, totalShards int) int {
	if totalShards <= 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(namespace + ":" + service))
	return int(h.Sum32()) % totalShards
}
