# Mini-Redis AI v2 — Agent Instructions

## Mission
Extend the existing Mini-Redis AI repository into a reliable, observable, production-inspired AI caching infrastructure system. Priorities are measurable backend engineering, concurrency correctness, fault tolerance, semantic-cache safety, and a public engineering showcase. Do not start over or replace existing subsystems without evidence.

## Read before implementation
1. `docs/CURRENT_MILESTONE.md` — current work and acceptance criteria.
2. `docs/ROADMAP_V2.md` — complete scoped roadmap.
3. `docs/PROJECT_CONTEXT.md` — architectural context and project goals.
4. `architecture.md`, `README.md`, and the relevant implementation and test files.

Treat actual source code and tests as authoritative about CURRENT behavior. Treat the roadmap as planned work, not as implemented functionality. Report discrepancies instead of silently assuming a feature exists. Never invent measured results or test outcomes.

## Current architecture to preserve
- Custom Python TCP Redis-inspired datastore with RESP parsing, TTL, AOF persistence and atomic queue commands.
- FastAPI `POST /inference` enqueues an asynchronous job; `GET /jobs/{job_id}` fetches its status. API does NOT directly run inference.
- Workers claim jobs with tokens and leases, heartbeat, retry, recover stale claims, embed prompts, run FAISS or linear semantic search, invoke provider on miss, save cache entries, and finish jobs.
- Redis Clone is the authoritative persistent state. FAISS indexes are worker-local, rebuildable accelerators.
- Existing metrics exposed via FastAPI and Prometheus; Grafana and structured logs already exist.

## Engineering rules
- Work on a dedicated Git feature branch. Do not push, merge, deploy, delete persistent volumes, or modify production/cloud infrastructure without explicit approval.
- Before coding, inspect the relevant code path, tests, and integration boundaries; state the design and anticipated edge cases briefly.
- Make narrow, incremental changes rather than large refactors. Retain existing interfaces and persistence formats unless migration is planned.
- Add deterministic unit/integration/concurrency/failure tests for every behavior change. Run the relevant tests and report exact commands and outcomes.
- Benchmark against an unchanged baseline and equivalent workloads; retain raw result files, parameters, hardware/runtime details and limitations. Never claim performance wins without measurements.
- Metrics must avoid unbounded label cardinality. Do not embed secrets or sensitive prompts in logs/metrics.
- Explain distributed correctness: ownership, atomicity, lease expiry, retries, fencing, crash recovery, and duplicate execution limits. At-least-once processing is not exactly-once execution.
- Update architecture diagrams, documentation, and milestone status when work is verified.
- Prefer correctness over adding buzzwords; explicitly surface risk or scope creep.

## Current work: Week 1
See `docs/CURRENT_MILESTONE.md`. First verify local multi-worker Compose operation and reproduce a stampede baseline; only then design distributed coalescing. Do not jump ahead into replication, eviction or the website.

## Deliverable format for each task
Report (1) files changed, (2) architecture/design rationale, (3) tests and actual results, (4) benchmarks/metrics, (5) risks and limitations, (6) next smallest step. Ask for approval on high-risk or ambiguous architectural changes.
