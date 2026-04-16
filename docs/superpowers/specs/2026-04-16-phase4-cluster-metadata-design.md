# Phase 4 Design — Multi-Node Cluster Formation and Metadata Coordination

**Date:** 2026-04-16
**Phase:** 4
**Status:** Approved

---

## Overview

Phase 4 turns the single-node system into a distributed cluster with coordination and metadata management. Three coordinator processes form a Raft consensus cluster that owns all cluster state. Storage nodes register with the coordinator cluster on startup and send periodic heartbeats. The coordinator tracks node liveness and manages a shard ownership map.

---

## Architecture

### Components

**`cmd/coordinator`** — new real binary (currently a stub). Three instances run as a Raft cluster. One is the Raft leader at any time; it accepts all write operations (node registration, heartbeat updates, shard reassignment). All coordinators can serve reads.

**`cmd/node`** — updated with two new startup behaviors:
1. Call `RegisterNode` on the coordinator cluster before accepting traffic
2. Run a background `HeartbeatSender` goroutine after successful registration

**`internal/metadata`** — Raft FSM, cluster state types, gRPC `ClusterService` implementation

**`internal/cluster`** — client-side registration and heartbeat sender for storage nodes

**`proto/logengine/v1/cluster.proto`** — new `ClusterService` RPC definitions

### Request Flow (node startup)

1. Storage node reads `COORDINATOR_ADDR` env var
2. Calls `RegisterNode` on any coordinator
3. Non-leader returns `FAILED_PRECONDITION` with leader address; node retries against leader
4. Leader applies `RegisterNode` command through Raft; returns assigned shard IDs and leader address
5. Node stores shard assignments and starts `HeartbeatSender`

### Request Flow (liveness)

1. Storage node sends `Heartbeat` every 5s to coordinator leader
2. Leader applies `UpdateHeartbeat` command through Raft (bumps `LastSeen`)
3. Liveness checker goroutine (leader only) ticks every 5s
4. If `now - LastSeen > 15s` for a healthy node → apply `MarkUnhealthy` command through Raft
5. `MarkUnhealthy` sets node status to `"unhealthy"` and clears `PrimaryNode` on all its shards

---

## Raft FSM and Cluster State

### State types (`internal/metadata/state.go`)

```go
type NodeStatus string

const (
    NodeHealthy   NodeStatus = "healthy"
    NodeUnhealthy NodeStatus = "unhealthy"
)

type NodeRecord struct {
    ID       string
    Address  string     // advertised gRPC address
    Shards   []int
    Status   NodeStatus
    LastSeen int64      // unix nanoseconds
}

type ShardRecord struct {
    ShardID     int
    PrimaryNode string   // node ID, empty string if unowned
}

type ClusterState struct {
    Nodes  map[string]NodeRecord   // keyed by node ID
    Shards map[int]ShardRecord     // keyed by shard ID
}
```

### FSM commands (`internal/metadata/fsm.go`)

| Command | Description |
|---|---|
| `RegisterNode` | Upsert node record. Assign unclaimed shards to the new node. |
| `UpdateHeartbeat` | Bump `LastSeen` for the node. |
| `MarkUnhealthy` | Set status to `"unhealthy"`. Clear `PrimaryNode` on all node shards. |
| `Snapshot` | Serialize `ClusterState` to JSON. |
| `Restore` | Deserialize `ClusterState` from JSON snapshot. |

### Shard assignment

- Total shard count: 16 (configurable via `TOTAL_SHARDS` env var)
- On `RegisterNode`: assign all currently unowned shards to the new node
- On `MarkUnhealthy`: shards become unowned (empty `PrimaryNode`)
- No shard stealing between healthy nodes in Phase 4
- Shards are reclaimed greedily on next successful `RegisterNode`

### Raft configuration

- Transport: `hashicorp/raft` TCP transport
- Peers: provided via `RAFT_PEERS=id1=addr1,id2=addr2,id3=addr3` env var
- Each coordinator also reads `RAFT_NODE_ID` and `RAFT_BIND_ADDR`
- Raft log and snapshots stored to disk under `RAFT_DATA_DIR`
- Bootstrap: all coordinators call `raft.BootstrapCluster` with the full peer list on first start (no existing state). Raft deduplicates concurrent bootstrap calls safely. Subsequent restarts skip bootstrap if log state exists on disk.
- Single-node mode (for local dev/tests without Docker Compose): set `RAFT_PEERS` to only this node's own ID/address; it bootstraps as a single-node leader.

---

## Proto: ClusterService

**File:** `proto/logengine/v1/cluster.proto`

```protobuf
service ClusterService {
  rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  rpc GetClusterState(GetClusterStateRequest) returns (GetClusterStateResponse);
}
```

### RegisterNode

- Request: `node_id`, `grpc_address`
- Response: `assigned_shards` (repeated int32), `leader_address`
- Write path — only leader processes; non-leader returns `FAILED_PRECONDITION` with leader address in error detail

### Heartbeat

- Request: `node_id`
- Response: `ok` (bool), `leader_address`
- Write path — same leader enforcement as `RegisterNode`

### GetClusterState

- Request: empty
- Response: repeated `NodeInfo` (id, address, shards, status, last_seen_unix_ns), repeated `ShardInfo` (shard_id, primary_node)
- Can be served by any coordinator (stale reads acceptable)

---

## Package Design

### `internal/metadata`

| File | Responsibility |
|---|---|
| `state.go` | `NodeRecord`, `ShardRecord`, `ClusterState` types |
| `fsm.go` | Raft FSM: `Apply`, `Snapshot`, `Restore`, command structs |
| `server.go` | gRPC `ClusterService` implementation; routes writes through Raft |

### `internal/cluster`

| File | Responsibility |
|---|---|
| `client.go` | `ClusterClient`: `Register`, `SendHeartbeat`, leader redirect logic |
| `heartbeat.go` | `HeartbeatSender`: ticker goroutine, context cancellation, failure logging |

### `cmd/coordinator/main.go`

1. Parse env vars: `RAFT_NODE_ID`, `RAFT_BIND_ADDR`, `RAFT_DATA_DIR`, `RAFT_PEERS`, `GRPC_PORT` (default `:9000`), `HTTP_PORT` (default `:8080`), `TOTAL_SHARDS` (default `16`)
2. Bootstrap Raft with TCP transport
3. Start gRPC server with `ClusterService`
4. Start HTTP server with `GET /status` handler (calls local FSM read, serializes to JSON)
5. Start liveness checker goroutine (only acts when this node is Raft leader)
6. Graceful shutdown on SIGINT/SIGTERM

### `cmd/node/main.go` additions

1. Read new env vars: `COORDINATOR_ADDR`, `NODE_GRPC_ADDR`
2. Call `ClusterClient.Register` before accepting traffic
3. Log assigned shards; continue in degraded mode if coordinator unreachable
4. Start `HeartbeatSender` after successful registration
5. Cancel heartbeat context on shutdown before gRPC graceful stop

---

## Liveness Checker

Runs in coordinator binary as a goroutine. Only the Raft leader takes action.

- Tick interval: `HeartbeatInterval` (5s, configurable via `HEARTBEAT_INTERVAL_SECONDS`)
- Timeout threshold: `HeartbeatTimeout` (15s = 3× interval, configurable via `HEARTBEAT_TIMEOUT_SECONDS`)
- On each tick: read current FSM state, iterate all `healthy` nodes, apply `MarkUnhealthy` for any node where `now - LastSeen > HeartbeatTimeout`
- Non-leader coordinators skip the action (check `raft.State() == raft.Leader`)

---

## HTTP `/status` Endpoint

- Path: `GET /status`
- Port: `HTTP_PORT` (default `:8080`)
- Response: JSON serialization of `GetClusterState` result
- Served directly from local FSM state (no gRPC round-trip)
- Useful for `curl` during demos and debugging

Example response shape:
```json
{
  "nodes": [
    { "id": "node-1", "address": "node-1:50051", "shards": [0,3,6,9,12], "status": "healthy", "last_seen_unix_ns": 1713276000000000000 }
  ],
  "shards": [
    { "shard_id": 0, "primary_node": "node-1" }
  ]
}
```

---

## Leader Redirect Protocol

Write RPCs (`RegisterNode`, `Heartbeat`) enforce leader-only writes:

1. Non-leader coordinator checks `raft.Leader()` to get current leader address
2. Returns gRPC status `FAILED_PRECONDITION` with the leader address as the error message
3. `ClusterClient` on storage node: on `FAILED_PRECONDITION`, parse leader address, reconnect, retry once
4. If the cluster has no leader yet (election in progress), coordinator returns `UNAVAILABLE`; client backs off with a short sleep (500ms) and retries up to 5 times before returning error
5. If retry also fails after leader redirect, return error to caller; node retries on next heartbeat tick

---

## Docker Compose

Three coordinator services added to `deployments/docker-compose/docker-compose.yml`:

- `coordinator-1`, `coordinator-2`, `coordinator-3`
- Each configured with `RAFT_NODE_ID`, `RAFT_BIND_ADDR`, `RAFT_DATA_DIR`, `RAFT_PEERS` (all three peers listed)
- Three storage node services updated with `COORDINATOR_ADDR=coordinator-1:9000` and `NODE_GRPC_ADDR=<service>:50051`
- Node services declare `depends_on: [coordinator-1, coordinator-2, coordinator-3]`

---

## Testing

### Unit tests

| Target | What to test |
|---|---|
| `internal/metadata/fsm_test.go` | `RegisterNode` assigns shards; `MarkUnhealthy` clears shard ownership; `UpdateHeartbeat` bumps LastSeen; snapshot/restore round-trip |
| `internal/metadata/shard_test.go` | Shard assignment distribution across N nodes; unowned shards claimed on registration |
| `internal/cluster/heartbeat_test.go` | Sender stops cleanly on context cancel; failure does not panic |

### Integration tests

| Target | What to test |
|---|---|
| `test/integration/cluster_test.go` | Three-node coordinator cluster forms; all three appear in cluster state; storage node registers and appears in node registry with shard assignments |
| `test/integration/rejoin_test.go` | Storage node registers, stops, restarts, calls `RegisterNode` again; node appears healthy in cluster state with shard assignments |
| `test/integration/liveness_test.go` | Storage node registers, then stops sending heartbeats; after timeout, node is marked unhealthy and its shards are unowned |

### Validation commands

```bash
make test    # all unit and integration tests pass
make lint    # no lint errors
make build   # go build ./... exits 0
```

---

## Known Limitations and Upgrade Path

- The coordinator cluster has no automatic client-side load balancing — storage nodes always target the current leader. In production, a load balancer or client-side round-robin would front the coordinator cluster.
- Shard reassignment on node failure leaves shards unowned; Phase 5 will add routing that handles unowned shards explicitly.
- Raft log compaction via snapshots is wired but snapshot frequency tuning is out of scope for Phase 4.
- No TLS on Raft transport or gRPC in Phase 4.

---

## Dependencies to Add

- `github.com/hashicorp/raft` v1.x
- `github.com/hashicorp/raft-boltdb` (Raft log store backed by BoltDB)

Run `go get github.com/hashicorp/raft github.com/hashicorp/raft-boltdb` after adding.
