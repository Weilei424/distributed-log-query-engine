# Phase 7: Observability, Deployment, and Reliability — Design Spec

## Goal

Make the system production-style enough to monitor, test, and demo convincingly: Prometheus metrics on all services, structured logging with `uber-go/zap`, provisioned Grafana dashboards, real Dockerfiles, Docker Compose with full observability stack, a Helm chart for Kubernetes, and a load test program.

---

## Approach

Observability-first sequencing:

1. Build the `internal/observability/` package (metrics, logger, request ID)
2. Wire instrumentation into existing services
3. Build real Dockerfiles and extend Docker Compose with Prometheus + Grafana
4. Build Helm chart for Kubernetes demo deployment
5. Build load test program
6. Write failure demo runbook

---

## Tech Stack

- `uber-go/zap` — structured logging
- `github.com/prometheus/client_golang` — Prometheus metrics + HTTP handler
- Docker multi-stage builds (golang:1.24-alpine → alpine:3.19)
- Helm 3 for Kubernetes chart
- Plain Go binary for load testing (no external load test tooling)

---

## Section 1: `internal/observability/` Package

Three files, all new.

### `internal/observability/metrics.go`

Defines all Prometheus metric variables and a `Register` function.

```go
func Register(reg prometheus.Registerer)
```

Metrics defined:

| Variable | Type | Labels | Purpose |
|---|---|---|---|
| `IngestRequestsTotal` | CounterVec | `node_id`, `status` | ingestion rate + failure count |
| `AppendDuration` | HistogramVec | `node_id` | append latency; buckets: 1ms–2s |
| `ActiveSegmentBytes` | GaugeVec | `node_id` | active segment file size |
| `MountedSegmentsTotal` | GaugeVec | `node_id` | number of open segments |
| `IndexTokenCount` | GaugeVec | `node_id` | in-memory index token count |
| `QueryDuration` | HistogramVec | `node_id`, `type` | local and fan-out query latency |
| `FanOutTimeoutsTotal` | Counter | — | per-node fan-out timeouts |
| `FanOutPartialTotal` | Counter | — | partial fan-out responses returned |
| `NodeHealthStatus` | GaugeVec | `node_id` | 1 = healthy, 0 = unhealthy |
| `ReplicationLagEntries` | GaugeVec | `node_id` | entries pending replication |

`type` label on `QueryDuration` takes values `"local"` or `"fanout"`.
`status` label on `IngestRequestsTotal` takes values `"ok"` or `"error"`.

### `internal/observability/logger.go`

```go
func NewLogger(component, nodeID string) *zap.Logger
```

Returns a `zap.Logger` pre-seeded with `component` and `node_id` fields. Uses `zap.NewProduction()` as the base config. Call sites add per-request fields with `.With(zap.String("request_id", id))`.

### `internal/observability/request_id.go`

```go
type contextKey int
const requestIDKey contextKey = 0

func WithRequestID(ctx context.Context, id string) context.Context
func RequestIDFromContext(ctx context.Context) string
func NewRequestID() string   // fmt.Sprintf("req-%x", rand.Uint64())
```

---

## Section 2: Instrumentation Wiring

No architectural changes — metric updates and log replacements added at call sites.

### `cmd/node/main.go`

- Call `observability.Register(prometheus.DefaultRegisterer)` at startup
- Initialize `zap.Logger` with `component="node"`, `node_id=NODE_ID`
- Read `METRICS_ADDR` env var (default `":9090"`); start HTTP server serving `promhttp.Handler()` at `/metrics`
- Pass logger down to ingest server, storage manager, index, query server

### `cmd/coordinator/main.go`

- Call `observability.Register(prometheus.DefaultRegisterer)` at startup
- Initialize `zap.Logger` with `component="coordinator"`
- Add `/metrics` route to existing HTTP mux (no new server needed)
- Pass logger down to fan-out executor

### `internal/ingest/server.go`

- Generate `request_id` at start of each `Ingest` / `IngestBatch` handler call
- Attach to context with `observability.WithRequestID`
- Increment `IngestRequestsTotal{node_id, status}` per entry processed
- Observe `AppendDuration{node_id}` around `storage.Manager.Append`
- Log each ingest: `request_id`, `shard_id`, `service`, entry count, duration

### `internal/storage/manager.go`

- After `Append`: set `ActiveSegmentBytes{node_id}` to active segment current size
- After rotation: update `MountedSegmentsTotal{node_id}`
- In `NewManager` (startup recovery): set `MountedSegmentsTotal` to recovered segment count

### `internal/index/`

- After `Add`: set `IndexTokenCount{node_id}` to current token map length

### `internal/query/server.go`

- Observe `QueryDuration{node_id, type="local"}` around executor call
- Log query: `request_id`, keyword, time range, result count, duration

### `internal/coordinator/fanout.go`

- Observe `QueryDuration{node_id="coordinator", type="fanout"}` around full fan-out
- Increment `FanOutTimeoutsTotal` per timed-out node
- Increment `FanOutPartialTotal` when result is partial
- Log fan-out: nodes targeted, nodes responded, timeout events, merge duration

### `internal/cluster/heartbeat.go`

- Set `NodeHealthStatus{node_id}` to `1` on successful heartbeat, `0` on failure

### `internal/replication/replicator.go`

- Set `ReplicationLagEntries{node_id}` to buffered channel length after each send

---

## Section 3: Docker Compose + Prometheus + Grafana

### Dockerfiles

Both `Dockerfile.node` and `Dockerfile.coordinator` become real two-stage builds:

```dockerfile
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /bin/app ./cmd/<role>

FROM alpine:3.19
COPY --from=builder /bin/app /app
ENTRYPOINT ["/app"]
```

### `deployments/docker-compose/prometheus.yml`

Scrapes all coordinators on port `8080` and all nodes on port `9090` at `/metrics`. Scrape interval: 15s.

### `docker-compose.yml` additions

- `prometheus` service: mounts `prometheus.yml`, exposes port `9090`, depends on nodes and coordinators
- `grafana` service: exposes port `3000`, mounts `grafana/provisioning/` and `grafana/dashboards/`
- Nodes: add `METRICS_ADDR=:9090` env var, expose port `9090`
- All services on a shared named network

### Grafana provisioning (auto-loaded, zero manual steps)

```
deployments/docker-compose/grafana/
├── provisioning/
│   ├── datasources/prometheus.yml    — datasource pointing to http://prometheus:9090
│   └── dashboards/provider.yml       — loads JSON dashboards from /etc/grafana/dashboards
└── dashboards/
    └── logengine.json                — dashboard JSON
```

Dashboard panels:
- Ingestion rate (`rate(IngestRequestsTotal[1m])`)
- Ingestion error rate (`rate(IngestRequestsTotal{status="error"}[1m])`)
- Append latency p50/p95
- Local query latency p50/p95
- Fan-out latency p50/p95
- Active segment size per node
- Node health status (stat panel, green/red)
- Replication lag per node

### `make run-local`

```makefile
run-local: build
	docker compose -f deployments/docker-compose/docker-compose.yml up --build
```

---

## Section 4: Helm Chart

Located at `deployments/kubernetes/helm/`.

```
helm/
├── Chart.yaml
├── values.yaml
└── templates/
    ├── namespace.yaml
    ├── coordinator-deployment.yaml
    ├── coordinator-service.yaml
    ├── node-statefulset.yaml
    ├── node-service.yaml
    ├── prometheus-configmap.yaml
    ├── prometheus-deployment.yaml
    └── prometheus-service.yaml
```

### `Chart.yaml`

```yaml
apiVersion: v2
name: logengine
version: 0.1.0
description: Distributed log query engine
```

### `values.yaml` tunables

```yaml
coordinator:
  replicas: 3
  image: logengine-coordinator:latest
  grpcPort: 9000
  httpPort: 8080
  totalShards: 16

node:
  replicas: 3
  image: logengine-node:latest
  grpcPort: 50051
  metricsPort: 9090
  storageSize: 1Gi

prometheus:
  enabled: true
  image: prom/prometheus:v2.51.0
```

Node StatefulSet uses a `volumeClaimTemplate` for `/data`. Coordinator env vars for Raft peer list are generated from `coordinator.replicas` via Helm template range.

Grafana is not included in the Helm chart — the Docker Compose stack serves as the observability demo environment.

---

## Section 5: Load Test + Failure Demo

### `test/load/main.go`

Standalone binary (not a `_test.go`). Flags:

```
-addr      string    coordinator gRPC address (default "localhost:9001")
-workers   int       concurrent goroutines (default 10)
-duration  duration  test duration (default 30s)
-mode      string    "ingest", "query", or "both" (default "both")
```

Ingest workers: tight loop of `IngestBatch` RPCs with randomized `service` and `message`. Query workers: tight loop of `Query` RPCs with random keywords. Latency tracked via sorted-slice accumulator for p50/p95.

Output summary at end:

```
--- Ingest ---
  total:    12,430 entries
  rate:     414/s
  errors:   3

--- Query ---
  total:    1,820 queries
  p50:      4ms
  p95:      18ms
  partial:  2.1%
```

Makefile target:

```makefile
load-test:
	go run ./test/load -addr=$(ADDR) -duration=$(DURATION) -mode=$(MODE)
```

### Failure demo runbook (`docs/runbooks/failure-demo.md`)

Documented steps:
1. `make run-local` — start full stack
2. `make load-test ADDR=localhost:9001 DURATION=60s` — ingest baseline data
3. `docker compose -f deployments/docker-compose/docker-compose.yml stop node-1`
4. Observe in Grafana: `NodeHealthStatus` for node-1 drops to 0, partial query responses increase
5. Run a manual query — confirm `partial=true` in response
6. `docker compose -f deployments/docker-compose/docker-compose.yml start node-1`
7. Observe node re-registers, health returns to 1, queries return full results

---

## Definition of Done

- `make build` passes
- `make test` passes
- `make lint` passes
- `make run-local` starts the full Docker Compose stack (nodes + coordinators + Prometheus + Grafana)
- Grafana dashboard loads automatically at `http://localhost:3000` with data after ingestion
- `make load-test` produces a summary report against a running cluster
- Helm chart passes `helm lint deployments/kubernetes/helm/`
- `docs/runbooks/failure-demo.md` exists and is accurate
- `docs/planning/BACKLOG.md` Phase 7 items all marked complete
