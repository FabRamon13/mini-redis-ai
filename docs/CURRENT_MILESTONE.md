# Current Milestone — Week 1: Baseline and Cache Stampede Prevention

Status: **Week 1 accepted and complete.** The baseline is preserved at `c960af3`, the integrated single-flight implementation at `7bec46b`, and the accepted reliability/performance evidence in `docs/WEEK1_STEP3_VERIFICATION.md` plus `benchmarks/results/repeated-20261109/`, `benchmarks/results/faults-20261109.json`, and `benchmarks/results/hol-20261109.json`.
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

## Verified coalescing evidence
- Redis Clone owns flight time, leases and monotonically increasing fencing generations.
- `FLIGHTCOMMIT` atomically publishes the semantic record, updates and prunes the cache index, and resolves the current fenced flight as completed.
- Workers recheck after leadership, renew during provider work, and use bounded jittered follower polling with expiry takeover and failure cooldown.
- An isolated four-worker run completed 100/100 jobs with 1 provider execution, 1 accepted publication, 4 initial semantic misses, 99 returned cache hits, no takeover, timeout, failure or fencing rejection, and a terminal `COMPLETED` flight.
- Raw post-change evidence is stored at `benchmarks/results/stampede-singleflight-20261109.json`; the accepted baseline artifacts are unchanged.

## Acceptance
- At least 100 identical inference requests exercised concurrently with multiple workers.
- Before/after experiment is repeatable and has raw saved evidence.
- In failure-free coalescing scope, 100 concurrent identical cold misses cause one provider computation (subject to clearly documented test scope).
- Followers receive or retrieve valid results; no indefinite waits; stale leaders cannot corrupt new state.
- Crashes, timeouts, retries and cleanup have explicit tests and documented limitations.
- Metrics, documentation, diagrams and README updated only when verified.

## Next development session
Design Week 2 backpressure and reliability work. Prioritize eliminating synchronous-follower head-of-line blocking and adding bounded datastore socket I/O; defer AOF compaction while retaining it as documented technical debt. Preserve Week 1 job IDs, durable queue semantics, claim-token fencing, and single-flight guarantees.
