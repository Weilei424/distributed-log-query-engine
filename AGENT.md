# AGENT.md

## AI Roles

- **Claude (claude.ai/code)** — primary implementor. All code authoring, planning, and execution happens here.
- **Codex** — code reviewer. After implementation milestones, Codex reviews the work for correctness, quality, and adherence to the plan.

---

## Purpose

This repository is built with an implementor and reviewer workflow.

Claude is responsible for building the system.
Codex is responsible for reviewing the system.

The goal is not just to produce code. The goal is to produce code that matches the architecture, stays aligned with the plan, and remains easy to explain and maintain.

---

## Core Workflow

1. Read the planning docs before changing code
2. Update `docs/planning/BACKLOG.md` when plans or tasks change
3. Implement the current milestone in small, coherent slices
4. Add or update tests
5. Update docs if behavior or structure changed
6. Hand off to Codex for review after milestone completion

---

## Agent Responsibilities

### Claude responsibilities
- plan implementation work
- write code
- refactor code
- update tests
- update docs
- keep backlog status current
- keep the repository runnable

### Codex responsibilities
- review milestone implementations
- check for correctness issues
- check for missing test coverage
- check for quality problems and design drift
- validate adherence to architecture and backlog intent

Codex should not become the primary author unless explicitly directed.

---

## Project Overview

The project is a distributed log query engine in Go.

It ingests logs, stores them on local segments across multiple nodes, tracks shard ownership through a metadata layer, and executes distributed queries by fanning out requests across the cluster.

The project should demonstrate:
- storage design
- indexing
- distributed coordination
- fault tolerance
- query execution
- observability

---

## Working Rules

- Always check the current plan before broad implementation work
- Keep `docs/planning/BACKLOG.md` synchronized with actual work
- Do not silently change architecture or repository layout
- Prefer small, reviewable changes over large speculative rewrites
- Do not add heavy dependencies without a clear reason
- Preserve clean package boundaries
- Keep the codebase runnable after each milestone

---

## Planning Docs

Agents should treat these documents as operational context:

- `docs/planning/IMPLEMENTATION_PLAN.md`
- `docs/architecture/ARCHITECTURE_NOTES.md`
- `docs/planning/BACKLOG.md`
- `README.md`

If a change affects system behavior, update the relevant doc.

---

## Review Triggers

Codex review should happen after:
- a phase is completed
- a storage format changes
- a query execution path changes
- a metadata or coordination path changes
- a major refactor lands
- a deployment or observability milestone lands

---

## Definition of Done

Work is done only when:
- the implementation exists
- tests are updated where appropriate
- docs are updated where appropriate
- backlog items are updated
- the repo still runs and validates cleanly

---

## Implementation Priorities

Use this priority order:

1. correctness
2. clear interfaces
3. observability
4. maintainability
5. performance tuning

Do not optimize early at the cost of clarity.

---

## Repository Expectations

The repository should trend toward this structure:

```text
cmd/
internal/
pkg/
proto/
deployments/
docs/
test/
```

Keep runtime logic in `internal/` and keep entrypoints in `cmd/`.

---

## Documentation Standards

When agents change any of the following, they should update docs in the same work cycle:
- architecture
- API contracts
- run commands
- repository layout
- backlog status
- failure behavior
- deployment flow

---

## Notes for Agents

This project should stay finishable by one engineer.

That means agents should value:
- simple designs
- explicit tradeoffs
- deterministic behavior
- visible failure modes
- readable code

A smaller but complete system is better than a broader but unfinished one.
