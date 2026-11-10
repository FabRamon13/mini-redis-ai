# Current Milestone — Week 1: Baseline and Cache Stampede Prevention

Status: Repository audit, baseline reconciliation, and the four-worker stampede baseline are complete. Request coalescing is not yet implemented or verified.
Historical target: October 13–19, 2026; current work is continuing beyond that original window. Do not treat historic dates as completion evidence.

## Objective
Demonstrate a reproducible concurrency race with multiple worker processes, then eliminate duplicate provider work for identical in-flight requests within a defined coordination scope, with failure-safe cleanup and measurable evidence.

## Verified source-level observations
- `fastapi_cache/main.py` enqueues a request asynchronously.
- `worker/worker.py:process_inference` embeds and searches cached semantic entries. On miss it sleeps `DEMO_INFERENCE_DELAY_SECONDS` (configurable), calls `generate_response`, and writes a semantic record.
- Redis Clone exposes provider-call, semantic-hit/miss and job counters via `/jobs/metrics`; `GET /metrics` exports Prometheus-style values.
- Local Compose defines one `worker` service and supports running multiple replicas of that service.
- Current implementation has no confirmed distributed in-flight coordination.

## Verified baseline evidence
- Isolated Compose project with one Redis Clone, one API and four workers.
- Fake provider with `DEMO_INFERENCE_DELAY_SECONDS=1` and linear vector search.
- 100 identical requests submitted concurrently with a unique cold prompt.
- 100 completed jobs, 0 failed jobs, 4 provider calls, 4 semantic misses and 96 semantic hits.
- Raw machine-readable results stored under `benchmarks/results/`.

## Next task after review
Propose a distributed coalescing design: the exact key must include appropriate provider/model/version/tenant/options; atomic leadership, expiry, follower behavior, result handoff, error propagation, safe cleanup, stale-owner fencing and crash/retry semantics. Do not implement it until the baseline changes and evidence have been reviewed.

## Acceptance
- At least 100 identical inference requests exercised concurrently with multiple workers.
- Before/after experiment is repeatable and has raw saved evidence.
- In failure-free coalescing scope, 100 concurrent identical cold misses cause one provider computation (subject to clearly documented test scope).
- Followers receive or retrieve valid results; no indefinite waits; stale leaders cannot corrupt new state.
- Crashes, timeouts, retries and cleanup have explicit tests and documented limitations.
- Metrics, documentation, diagrams and README updated only when verified.

## Next development session
Review the baseline evidence, then design request coalescing as a separate incremental change. Do not advance into later v2 milestones.
