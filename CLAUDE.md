# CLAUDE.md

## AI Roles

- **Claude (claude.ai/code)** — primary implementor. All code authoring, planning, execution, refactoring, and documentation updates happen here.
- **Codex** — code reviewer. After implementation milestones, Codex reviews the work for correctness, quality, and adherence to the current plan.

---

## Working Rules

1. Whenever a plan is generated or changed, also update the checklist in `docs/planning/BACKLOG.md`.
2. Whenever a checklist item from `docs/planning/BACKLOG.md` is done or changed, update its contents or status right away.
3. Do not start implementation work from memory alone. Read the relevant planning docs first.
4. Keep changes scoped to the current milestone unless a small adjacent fix is clearly necessary.
5. Prefer small, reviewable commits and tightly scoped pull request-sized changes.
6. Do not silently change architecture, interfaces, or repository layout. Reflect those changes in docs.
7. Every completed milestone must leave the repo in a runnable state.
8. When there is tension between speed and clarity, choose clarity.

---

## Mission

Build a distributed log query engine in Go that is small enough for one engineer to finish, but strong enough to demonstrate real distributed systems judgment.

The project should clearly show:
- durable append-oriented log ingestion
- segment-based storage
- indexing for query acceleration
- cluster metadata and shard ownership
- distributed query fan-out
- degraded behavior under partial failure
- observability and deployment maturity

---

## Project Overview

This repository contains a personal distributed systems project focused on ingesting, storing, and querying logs across multiple nodes.

The project is not trying to clone a full production log platform. It is trying to build the smallest version that still shows credible engineering decisions.

The implementation should prioritize:
- correctness
- clear responsibility boundaries
- stable interfaces
- good operational visibility
- explainable tradeoffs

---

## Technology Stack

### Core
- Go
- gRPC
- Protocol Buffers

### Storage and Query
- append-only local segment files
- in-memory inverted index for v1
- optional BadgerDB only if persistent indexing is needed later

### Coordination
- Raft-backed metadata leadership and cluster state

### Operations
- Docker
- Docker Compose
- Kubernetes
- Prometheus
- Grafana

### Tooling
- Go test
- golangci-lint
- Makefile
- buf if protobuf generation is used

Do not add new infrastructure dependencies unless they solve a real problem in the current phase.

---

## Planning Docs

The following docs are the source of truth, in this order:

1. `docs/planning/IMPLEMENTATION_PLAN.md` or the current implementation plan document
2. `docs/planning/ARCHITECTURE_NOTES.md`
3. `docs/planning/BACKLOG.md`
4. `README.md`

### Planning expectations
- The implementation plan defines phase-level direction
- The architecture notes define system structure and constraints
- The backlog defines executable checklist work
- The README should reflect the user-facing state of the repo

If these documents disagree, fix the disagreement before making broad implementation changes.

---

## Repository Layout

Use or evolve toward this layout:

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

### Layout rules
- Keep `cmd/` thin
- Keep business logic in `internal/`
- Keep shared types minimal and intentional
- Keep deployment assets out of core runtime packages
- Keep planning and architecture docs current

---

## Phase Execution Rules

For each phase, Claude must do the following in order:

1. Read the relevant phase in the implementation plan
2. Check `docs/planning/BACKLOG.md`
3. Expand the phase into concrete tasks if needed
4. Implement the smallest coherent vertical slice
5. Add or update tests
6. Run validation commands
7. Update docs and backlog statuses
8. Request Codex review after milestone completion

Never mark work complete unless code, tests, and docs all reflect the new state.

---

## Definition of Done

A task is done only when all of the following are true:

- implementation exists
- tests exist where appropriate
- linting and formatting pass
- docs are updated if behavior or design changed
- `docs/planning/BACKLOG.md` status is updated
- the project still runs locally

A milestone is done only when the related checklist items are complete and the system behavior can be demonstrated.

---

## Coding Standards

### General
- Prefer straightforward Go over clever abstractions
- Favor composition over inheritance-like patterns
- Keep packages small and responsibility-driven
- Use context propagation for request-scoped operations
- Return explicit errors with useful context

### APIs
- Keep request and response contracts stable
- Avoid premature feature sprawl in API design
- Make failure behavior explicit in responses when needed

### Concurrency
- Be conservative with goroutines
- Avoid hidden shared mutable state
- Make ownership and synchronization obvious
- Use channels only where they improve clarity

### Storage
- Keep on-disk formats versionable
- Avoid unbounded memory buffering in write or query paths
- Make segment rotation and reopen behavior deterministic

### Query path
- Bound fan-out work with timeouts and limits
- Make partial result handling explicit
- Keep merge logic deterministic and testable

---

## Testing Expectations

### Unit tests
Required for core logic such as:
- segment writes and rotation
- index insertion and lookup
- query parsing
- shard routing
- result merge and pagination

### Integration tests
Required for:
- single-node ingest and query
- multi-node shard ownership and routing
- distributed query fan-out
- degraded behavior under node failure or timeout

### Validation commands
At minimum, Claude should keep these working:

```bash
make test
make lint
make run-local
```

If the repo uses different commands, update this file and the README.

---

## Observability Standards

Every milestone should preserve or improve observability.

### Minimum standards
- Prometheus metrics exposed by all long-running services
- structured logs with stable keys
- request IDs propagated through write and query paths
- query and ingestion latency visible in metrics
- node health visible in metrics or status endpoints

Do not ship invisible behavior. If a failure mode exists, there should be a way to observe it.

---

## Design Constraints

Do not violate these without updating the architecture notes.

- Keep the scope finishable by one engineer
- Prefer deterministic behavior over maximum optimization
- Avoid unnecessary external dependencies
- Keep the system easy to explain in interviews
- Preserve clean boundaries between metadata, storage, and query execution

---

## Documentation Update Rules

Claude must update docs when any of the following changes:
- architecture or component boundaries
- repository layout
- commands to run or test the system
- backlog item status
- API contracts
- operational behavior under failure

Do not leave docs stale after implementation.

---

## Codex Review Handoff

After a milestone, prepare the repo for Codex review by making sure:
- the backlog reflects current status
- the scope of the milestone is documented
- known issues are listed clearly
- temporary shortcuts are called out directly
- commands for validation are easy to run

Codex should review for:
- correctness
- race conditions
- error handling quality
- alignment with architecture and plan
- missing tests
- over-engineering or unnecessary complexity

---

## Backlog Format Guidance

`docs/planning/BACKLOG.md` should use checklist-driven milestone tracking.

Recommended format:

```md
# BACKLOG

## Current Phase
- [ ] Task name
- [ ] Task name

## Next Phase
- [ ] Task name

## Done
- [x] Task name
```

When work changes, update the checklist immediately.

---

## Preferred Implementation Style

Claude should generally prefer this order:
- make the code correct
- make the interfaces clean
- make the system observable
- make the code easier to review
- optimize only after behavior is stable

This project wins by being credible and explainable, not by having the most features.
