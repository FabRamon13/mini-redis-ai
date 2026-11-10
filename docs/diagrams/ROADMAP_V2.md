# Mini-Redis AI v2 — Final Development Roadmap

Originally planned October 13–December 22, 2026 (10 weeks). Treat these as historical target dates; progress must be tracked by tests and evidence, not calendar assumptions.

## Definition of done
A cache/AI inference infrastructure platform with tested concurrency controls, semantic correctness safeguards, bounded resource usage, observable operations, reproducible load/failure experiments, documented distributed behavior and a publicly accessible interactive engineering showcase. Every feature needs tests, metrics, documentation and a reproducible demonstration.

## Execution plan

### Week 1 (Oct 13–19): Baseline and concurrency — Core
- [ ] Audit inference request path and metrics.
- [ ] Run repeatable cold-cache multi-worker stampede baseline.
- [ ] Implement cross-worker single-flight with atomic coordination, well-scoped keys, leader/follower behavior, error/timeout cleanup and failure tests.
- [ ] Demonstrate provider-call reduction for 100 identical concurrent misses.

### Week 2 (Oct 20–26): Backpressure and reliability — Core
- [ ] Bound active inference and queue depth.
- [ ] Predictable overload rejection.
- [ ] Worker/provider timeouts, cancellations and retry rules.
- [ ] Test resource exhaustion, failures and safe recovery.
- [ ] Benchmark p95/p99 latency, queue depth and rejection rate.

### Week 3 (Oct 27–Nov 2): Semantic correctness — Core
- [ ] Exact-match comparison baseline vs semantic cache.
- [ ] Tenant isolation and complete model-version cache identity.
- [ ] Knowledge-version invalidation.
- [ ] Labeled paraphrase/negative control dataset and threshold experiments.
- [ ] Measure false reuse, hit rate, backend calls, cost estimates and p95.

### Week 4 (Nov 3–9): Memory and eviction — Advanced
- [ ] Configurable memory cap and accounting.
- [ ] LRU and LFU eviction.
- [ ] Same-budget workload comparison and evictions/hit-rate metrics.
- [ ] TinyLFU-inspired admission only if evidence justifies.

### Week 5 (Nov 10–16): Distributed architecture — Advanced
- [ ] Primary/replica transport and ordered replication.
- [ ] Bootstrap/initial sync and reconnection.
- [ ] Define consistency, acknowledgments and loss windows.
- [ ] Test replica convergence and replication lag.

### Week 6 (Nov 17–23): Failover and recovery — Advanced
- [ ] Health checks and controlled failover.
- [ ] Restart, stale state and interrupted replication resync.
- [ ] Address split-brain/fencing risk, document limitations.
- [ ] Test failures and measure recovery/availability/data loss.

### Week 7 (Nov 24–30): Sharding/scaling — Stretch
- [ ] Consistent hashing and deterministic ownership.
- [ ] Membership changes and key movement.
- [ ] Node unavailability behavior.
- [ ] Benchmark 1, 2 and 3 nodes, aggregate/per-node throughput.
- [ ] Automatic failover only if replication/consistency guarantees can be validated.

### Week 8 (Dec 1–7): Observability — Core
- [ ] OpenTelemetry FastAPI instrumentation and asynchronous context propagation.
- [ ] Custom spans around datastore/cache, embeddings, FAISS/linear lookup, provider and worker.
- [ ] Trace backend via Tempo or Jaeger.
- [ ] Grafana dashboards A: cache performance; B: distributed system health; C: AI inference quality.
- [ ] Alerts and Prometheus metric naming/cardinality review.

### Week 9 (Dec 8–14): Performance experiments — Core
- [ ] Grafana k6 workload suite and regression thresholds.
- [ ] Compare no cache/exact/semantic; no single-flight/with single-flight; LRU/LFU; node scaling; healthy vs failure; version aware vs similarity only.
- [ ] Record raw JSON/CSV, environment, parameters, dashboards and real measured results.

### Week 10 (Dec 15–22): Interactive showcase — Core
- [ ] Hosted engineering lab website, architecture diagrams.
- [ ] Interactive cache stampede, semantic cache and fault/recovery examples with genuine measured/recorded results.
- [ ] Clearly distinguish live actions from replayed demos.
- [ ] Reproducible guides, README, deployment docs and source links.
- [ ] Verify live URL and final automated suite.

## Priorities and scope
Core commitments: request coalescing, backpressure, semantic correctness, OpenTelemetry tracing, k6/Grafana, public showcase. Advanced targets: bounded memory/eviction, single-primary replication and recovery. Stretch: sharding, automatic failover and sophisticated admission policies. Reduce scope if needed rather than ship incorrect distributed guarantees. No Kubernetes/Kafka just for buzzword value.

## Gate for each milestone
Tests (unit/integration/concurrency/fault as appropriate), before/after performance on equivalent workloads, useful metrics, documented tradeoffs and reproducible evidence. Use feature branches and descriptive commits. Do not advance before acceptance criteria pass.
