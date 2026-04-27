# Distributed Log Query Engine in Go — Architecture Notes

## 1. Project Overview

This project is a distributed log query engine written in Go. It is designed to ingest logs from multiple producers, store them across multiple nodes, and execute queries in parallel across the cluster.

The system is intentionally smaller than a production observability platform, but it should preserve the core engineering ideas that make systems like distributed log backends credible in interviews:

- append-oriented ingestion
- segment-based local storage
- indexing for faster search
- cluster coordination and metadata ownership
- distributed query fan-out
- partial failure handling
- observability and operability

The target outcome is a system that is practical to build alone, easy to explain, and strong enough to demonstrate distributed systems judgment.

---

## 2. Architecture Goals

### Primary goals
- Build a working end-to-end distributed system in Go
- Keep the design simple enough to finish and defend in interviews
- Show clear ownership boundaries between ingestion, storage, metadata, and querying
- Prefer correctness and explainability over premature feature breadth

### Non-goals for v1
- Full text ranking or advanced relevance scoring
- Complex query language comparable to LogQL or SQL
- Global exactly-once ingestion guarantees
- Strong cross-region durability
- Unlimited cardinality optimizations

---

## 3. Technology Stack

## Core
- **Go** for all services and internal libraries
- **gRPC** for internal service-to-service communication
- **HTTP** for external ingestion and query entrypoints if needed for easier testing
- **Protocol Buffers** for service contracts and typed request and response models

## Storage and Indexing
- **Local disk** for append-only segment storage
- **Write-Ahead Log or append-only segment files** for durable writes
- **In-memory inverted index** for v1 keyword lookup
- **Optional BadgerDB** only if persistent local index storage becomes necessary

## Coordination
- **HashiCorp Raft** or a small wrapper around a Raft library for metadata consistency
- **Membership and heartbeat logic** managed by the control plane or metadata leader

## Deployment and Operations
- **Docker** for local packaging
- **Docker Compose** for fast multi-node local development
- **Kubernetes** for containerized distributed deployment demos
- **Prometheus** for metrics collection
- **Grafana** for dashboards

## Testing and Tooling
- **Go test** for unit and integration testing
- **Makefile** for repeatable developer workflows
- **golangci-lint** for linting and static checks
- **buf** if protobuf generation is used heavily

---

## 4. High-Level System Model

The project should be split into a small number of clearly named components.

### API Gateway or Front Door
Receives external write and query requests. For local development, this can be merged into the coordinator binary. For cleaner architecture, keep the responsibility thin:

- validate requests
- assign request IDs
- forward writes to the owning ingest path
- forward queries to the query coordinator

### Ingest Path
Accepts log records and routes them to the correct shard owner.

Responsibilities:
- validate log schema
- enrich with receive timestamp if needed
- determine shard placement
- forward to primary storage node
- optionally trigger replication to followers

### Storage Node
Owns local data for one or more shards.

Responsibilities:
- append logs to active segment
- rotate segments by size or time
- maintain in-memory index updates
- serve local query execution
- expose health and storage metrics

### Metadata or Control Plane
Maintains cluster state.

Responsibilities:
- node registry
- shard ownership map
- replica assignments
- heartbeat and liveness tracking
- leadership decisions through Raft-backed state

### Query Coordinator
Receives distributed queries and orchestrates fan-out.

Responsibilities:
- parse query request
- resolve relevant shards and nodes
- dispatch requests in parallel
- enforce deadlines and partial failure policy
- merge, sort, and paginate results

### Background Workers
Run maintenance tasks.

Responsibilities:
- compaction
- index rebuilds
- stale segment cleanup
- replica catch-up
- metrics and diagnostics aggregation if needed

---

## 5. Key Decisions

## Decision 1: Start with a single binary layout, then separate packages cleanly
For speed, early phases can run as one process type with role-based configuration. Internally, packages should still separate control plane, storage, indexing, and querying.

Reason:
- faster implementation
- fewer deployment variables early
- easier debugging

Constraint:
- keep internal interfaces clean so the code can later split into multiple binaries if needed

## Decision 2: Use segment-based append-only local storage
Each storage node persists logs to append-only files called segments. Active segments receive writes. Closed segments become queryable and eligible for maintenance jobs.

Reason:
- simple durability model
- good write performance
- easy to explain in interviews
- natural foundation for compaction and bloom filters later

## Decision 3: Use in-memory indexing first
Maintain a lightweight inverted index in memory for recently written or currently mounted segments.

Reason:
- easier to build than a complex persistent indexing engine
- enough to demonstrate query acceleration
- persistent indexing can be added later if needed

Tradeoff:
- restart recovery is slower if indexes must be rebuilt from disk

## Decision 4: Use shard ownership for both writes and reads
Partition by a stable routing key, such as service name, stream name, or log source.

Reason:
- deterministic placement
- easier debugging
- easier explanation of ownership and replication

Tradeoff:
- hotspot risk if routing keys are skewed

## Decision 5: Keep replication basic in v1
Use primary plus replica replication with asynchronous follower update as the default.

Reason:
- enough to demonstrate fault tolerance
- easier than implementing complex quorum write semantics

Tradeoff:
- small durability window under primary failure
- eventual consistency for replicas

## Decision 6: Partial query success is acceptable
If one node is slow or unavailable, the system may return partial results with a clear partial flag and diagnostics.

Reason:
- realistic distributed systems behavior
- better user experience than total failure for every degraded condition

---

## Decision 7: QueryResponse.total is a lower bound, not an exact count
The coordinator asks each node for at most `max(fan_out_limit, offset+limit)` entries. The `total` field in `QueryResponse` is the deduplicated count of candidates returned across all responding nodes, before offset/limit are applied.

This means `total` can undercount when a node holds more matching entries than the fetch window. A separate per-node count RPC would be needed for an exact total, which is out of scope for v1.

Callers must treat `total` as a lower bound. The proto comment documents this explicitly. The pagination window (offset+limit) is always fully satisfiable regardless of this limitation.

---

## 6. Data Model

A v1 log entry should be simple and stable.

```text
LogEntry {
  id            string
  timestamp     int64
  received_at   int64
  service       string
  level         string
  message       string
  fields        map[string]string
}
```

### Notes
- `id` helps with de-duplication and debugging; if omitted by the producer, the ingest server assigns a random `auto-<hex>` ID so that all stored entries carry a non-empty ID
- `timestamp` comes from producer when trusted
- `received_at` is assigned by the ingest path for operational visibility
- `fields` allows light structured metadata without forcing a rigid schema

---

## 7. Write Path

1. Client sends a log ingestion request
2. Gateway or coordinator validates the payload
3. Routing logic computes target shard
4. Request is sent to the primary storage node for that shard
5. Primary appends to the active segment and updates the local index
6. Replication request is sent to replica nodes
7. Success is returned once the chosen durability condition is met

### v1 durability suggestion
Acknowledge after local durable append on the primary. Replication can happen immediately after but not block the client in v1.

This is a practical tradeoff for a portfolio project.

---

## 8. Read Path

1. Client submits a query with keyword and optional time bounds
2. Query coordinator parses the request
3. Metadata layer resolves relevant shards or nodes
4. Coordinator fans out the query in parallel
5. Each storage node searches local indexes and segment candidates
6. Partial results are returned to the coordinator
7. Coordinator merges, sorts, trims, and paginates the final response
8. Response includes result count, latency, and partial success metadata when applicable

---

## 9. Storage and Indexing Notes

## Segment layout
A segment should include:
- segment metadata header
- ordered log records
- optional lightweight footer or checkpoint metadata

### Rotation strategy
Rotate when either condition is met:
- file size threshold
- time window threshold

This avoids oversized files and simplifies maintenance.

## Index strategy
For v1, index by:
- keyword token to record references
- segment time bounds
- optional service name to segment mapping

### Query pruning
Before scanning a segment, prune by:
- time range overlap
- service filter if present
- bloom filter in a later phase

---

## 10. Consistency Model

### Metadata consistency
Strong consistency is preferred for cluster metadata, shard ownership, and replica assignment. This is where Raft fits.

### Data consistency
Eventual consistency is acceptable for replica reads in v1.

### Query consistency
Queries should target primaries by default if correctness is more important than availability. Replica reads can be added as an optimization later.

---

## 11. Failure Handling Expectations

The project should explicitly define degraded behavior.

### Primary node failure
- metadata layer marks node unhealthy after heartbeat timeout
- shard ownership is reassigned if failover logic exists
- writes for affected shards may fail briefly during reassignment
- reads may fallback to replicas if supported

### Slow query node
- coordinator applies per-node timeout
- late node result is dropped
- final response is returned with partial status

### Restarted node
- node re-registers with the cluster
- local segments are reopened
- index is rebuilt or reloaded
- replica catch-up runs if needed

### Metadata leader failure
- Raft elects a new leader
- writes depending on metadata updates pause briefly until leadership stabilizes

---

## 12. Design Constraints

These constraints should remain fixed unless there is a strong reason to change them.

### Constraint 1: Finishable scope
The project must stay finishable by one engineer. Avoid adding features that do not materially improve the distributed systems signal.

### Constraint 2: Clean internal boundaries
Even if components run in one process early, package boundaries should reflect true responsibilities.

### Constraint 3: No unnecessary infrastructure dependencies
Do not add Kafka, Redis, or external databases unless they clearly solve a real project need. The strongest version of this project is one where the core system is implemented by you.

### Constraint 4: Explainability matters
Every major subsystem should be easy to explain in two minutes.

### Constraint 5: Deterministic behavior beats cleverness
Prefer clear routing, simple replication, and predictable failure policy over complex optimizations.

---

## 13. Observability Standards

Observability should not be treated as a stretch goal. It is part of the system contract.

## Metrics
Every node should expose Prometheus metrics.

### Minimum required metrics
- ingestion request rate
- ingestion failure count
- append latency
- active segment size
- number of mounted segments
- index size or token count
- local query latency
- coordinator fan-out latency
- query timeout count
- partial query response count
- heartbeat health status
- replication lag if replication exists

## Logging
Use structured logs with stable keys.

### Required fields
- timestamp
- level
- component
- node_id
- request_id
- shard_id
- message

Avoid vague free-form logs for core code paths.

## Tracing
Full distributed tracing is optional. If not added, query coordinator debug logs must still clearly show:
- request received
- nodes targeted
- nodes responded
- timeout or failure events
- final merge duration

## Dashboards
Grafana dashboards should at least show:
- ingestion throughput over time
- query p50 and p95 latency
- node health
- segment growth
- error counts

---

## 14. Environments

## Local development
The default development environment should support fast iteration.

Recommended setup:
- run a single node with local disk paths
- run three nodes through Docker Compose for distributed testing
- use local Prometheus and Grafana for metrics validation

## Integration environment
A lightweight multi-node container environment that mirrors realistic service boundaries.

Recommended requirements:
- at least three nodes
- one metadata leader
- multiple shard owners
- one demo dashboard

## Kubernetes demo environment
Used for deployment proof and resume signal.

Suggested scope:
- StatefulSet or Deployment-based nodes depending on role
- ConfigMaps for node config
- persistent volumes for storage nodes
- Services for RPC and metrics endpoints

---

## 15. Repository Layout

A clean repository layout matters because the project will likely be built with AI assistance and reviewed later.

```text
.
├── cmd/
│   ├── node/
│   ├── coordinator/
│   └── cli/
├── internal/
│   ├── api/
│   ├── cluster/
│   ├── config/
│   ├── coordinator/
│   ├── ingest/
│   ├── index/
│   ├── metadata/
│   ├── replication/
│   ├── storage/
│   └── observability/
├── pkg/
│   └── types/
├── proto/
├── deployments/
│   ├── docker-compose/
│   └── kubernetes/
├── docs/
│   ├── architecture/
│   └── planning/
│       └── BACKLOG.md
├── test/
│   ├── integration/
│   └── fixtures/
├── Makefile
└── README.md
```

### Layout notes
- `internal/` should contain most implementation logic
- `cmd/` should stay thin
- `docs/planning/BACKLOG.md` is the plan-of-record checklist for agents
- `deployments/` should separate local and cluster deployment assets

---

## 16. Planning and Documentation Expectations

The implementation plan defines phase-level direction.

The backlog should define actionable checklist items.

### Recommended planning flow
1. Update architecture notes if a structural decision changes
2. Update `docs/planning/BACKLOG.md`
3. Implement the smallest coherent slice
4. Run tests and capture evidence
5. Request review
6. Update docs to reflect the final behavior

---

## 17. Testing Standards

## Unit tests
Required for:
- segment writing and rotation
- index insertion and lookup
- query parsing
- shard routing
- merge logic

## Integration tests
Required for:
- end-to-end single-node ingestion and query
- multi-node shard routing
- distributed query fan-out
- degraded query behavior under one-node failure

## Demo tests
Should exist as scripts or Make targets for:
- starting a three-node cluster
- ingesting sample logs
- executing a distributed query
- killing a node and showing degraded or recovered behavior

---

## 18. Security and Operational Guardrails

This is not a security-heavy project, but some guardrails still help.

### Minimum guardrails
- validate payload size
- bound query time range and page size
- avoid unbounded memory growth in query aggregation
- sanitize log line serialization and parser behavior
- never silently ignore replication or query errors

---

## 19. Future Extensions

The following are good future extensions after the core project is stable:

- bloom filters for segment skipping
- compaction jobs
- persistent index snapshots
- richer boolean query language
- query caching
- tenant isolation
- retention policies
- rate limiting and auth

These should not block the first complete version.

---

## 20. Definition of a Strong v1

The project is strong enough when all of the following are true:

- multiple nodes can run together
- logs can be ingested and persisted durably
- keyword and time-range queries work correctly
- distributed query fan-out works across nodes
- partial failure behavior is observable and documented
- dashboards and metrics exist
- the architecture and tradeoffs are easy to explain without hand-waving

That is the point where the project becomes genuinely useful for interviews and portfolio review.
