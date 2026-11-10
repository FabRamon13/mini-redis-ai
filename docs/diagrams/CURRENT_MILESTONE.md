# Current Milestone — Week 1: Baseline and Cache Stampede Prevention

Status: Not yet implemented/verified in v2. This is a planning checkpoint, not a declaration that tests have passed.
Historical target: October 13–19, 2026; current work is continuing beyond that original window. Do not treat historic dates as completion evidence.

## Objective
Demonstrate a reproducible concurrency race with multiple worker processes, then eliminate duplicate provider work for identical in-flight requests within a defined coordination scope, with failure-safe cleanup and measurable evidence.

## Verified source-level observations
- `fastapi_cache/main.py` enqueues a request asynchronously.
- `worker/worker.py:process_inference` embeds and searches cached semantic entries. On miss it sleeps `DEMO_INFERENCE_DELAY_SECONDS` (configurable), calls `generate_response`, and writes a semantic record.
- Redis Clone exposes provider-call, semantic-hit/miss and job counters via `/jobs/metrics`; `GET /metrics` exports Prometheus-style values.
- Local Compose defines one `worker` service and supports running multiple replicas of that service.
- Current implementation has no confirmed distributed in-flight coordination.

## Next task (do not skip)
1. Inspect Git status and relevant implementation/config/tests before any edits.
2. Create/use `feature/request-coalescing` branch.
3. Enable `DEMO_INFERENCE_DELAY_SECONDS=1` for local worker service only; start four workers against the same local Redis Clone (`docker compose up -d --build --scale worker=4`). Never remove persistent volumes without approval.
4. Verify healthy services, queue progress and the exact API contract.
5. Create `benchmarks/benchmark_stampede.py` to submit 100 identical new/cold inference prompts in a synchronized burst, poll all jobs to terminal states, and record provider calls delta, semantic hits/misses delta, failures and latency distributions. Make sure the prompt does not collide with previously cached semantic entries; isolate benchmark state without destroying user data.
6. Store baseline machine-readable results with environment/settings, and explain reproducibility limitations.
7. Only after verified baseline, propose a distributed coalescing design: exact coalescing key includes appropriate provider/model/version/tenant/options; atomic leadership, expiry, follower behavior, result handoff, error propagation, safe cleanup, stale owner fencing and crash/retry semantics.
8. Implement incrementally with unit, multi-worker concurrency and failure tests; rerun same benchmark and compare.

## Acceptance
- At least 100 identical inference requests exercised concurrently with multiple workers.
- Before/after experiment is repeatable and has raw saved evidence.
- In failure-free coalescing scope, 100 concurrent identical cold misses cause one provider computation (subject to clearly documented test scope).
- Followers receive or retrieve valid results; no indefinite waits; stale leaders cannot corrupt new state.
- Crashes, timeouts, retries and cleanup have explicit tests and documented limitations.
- Metrics, documentation, diagrams and README updated only when verified.

## Agent behavior on first session
First audit and report a short implementation plan. Do not code the entire v2 roadmap. Deliver the smallest testable PR focused on baseline instrumentation and benchmark setup.
