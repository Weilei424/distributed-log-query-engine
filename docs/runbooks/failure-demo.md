# Failure Demo Runbook

Demonstrates observable node failure and recovery using the local Docker Compose stack.

## Prerequisites

- Docker and Docker Compose installed
- `make run-local` has started the full stack

## Steps

### 1. Start the stack

```bash
make run-local
```

Wait ~10 seconds for all nodes to register and begin heartbeating.

### 2. Ingest baseline data

In a second terminal:

```bash
make load-test ADDR=localhost:9001 DURATION=20s MODE=ingest
```

### 3. Open Grafana

Navigate to http://localhost:3000 (no login required).
Open the **Log Engine** dashboard. Confirm:
- `NodeHealthStatus` shows 1 (green) for node-1, node-2, node-3
- Ingestion rate shows activity

### 4. Kill node-1

```bash
docker compose -f deployments/docker-compose/docker-compose.yml stop node-1
```

### 5. Observe degraded state

There are two distinct windows to observe:

**Window A — transition (0–15s after kill):** node-1 is unreachable but not yet marked unhealthy.
Fan-out still targets it and gets errors, so `partial=true`.

Run a query immediately after the kill to catch this window:

```bash
make load-test ADDR=localhost:9001 DURATION=5s MODE=query
# partial: should be > 0%
# Fan-Out Partial Total panel in Grafana should spike
```

**Window B — steady degraded state (after ~15s):** the coordinator marks node-1 unhealthy.
Fan-out skips it entirely; `partial=false` but results come only from node-2 and node-3.

On the Grafana dashboard after the transition:
- `NodeHealthStatus` for node-1 drops to 0 (red)
- Fan-out partial response rate returns to 0 (node is skipped, not failing)

```bash
make load-test ADDR=localhost:9001 DURATION=5s MODE=query
# partial: should be 0.0% (node-1 is skipped, not targeted)
```

### 6. Restart node-1

```bash
docker compose -f deployments/docker-compose/docker-compose.yml start node-1
```

### 7. Observe recovery

Within ~5 seconds, node-1 re-registers and begins heartbeating.
On the Grafana dashboard:
- `NodeHealthStatus` for node-1 returns to 1 (green)

Run another query to confirm node-1 is back in the fan-out target set:

```bash
make load-test ADDR=localhost:9001 DURATION=5s MODE=query
# partial: should be 0.0%
```
