# Week 1 Step 3 — Reliability and Performance Verification

Verified locally on 2026-11-09/10 (America/New_York) on arm64 macOS 26.2,
Python 3.11.9, Docker 29.5.2 and Docker Compose 5.1.3. The implementation
under test is commit `7bec46bb575772559b95948b4edd299ad8b0cfb9`; the unchanged
baseline workload source is `c960af328b06ac93a8cb6ad2a026b1156d2d63b6`.

## Automated verification

- Final default suite: 220 run, 215 passed, 5 skipped, 0 failed. The accepted
  218-test result is preserved in the benchmark-era evidence; two final tests
  verify that fault hooks require explicit benchmark-only arming and reject
  unknown phases.
- The five opt-in client/concurrency/queue integration tests all passed against
  a fresh localhost Redis Clone and temporary AOF.
- Base Compose, benchmark Compose, and benchmark plus fault override all pass
  `docker compose config -q`.
- No Compose projects were left running. Twenty project-scoped benchmark AOF
  volumes are intentionally retained for audit; none reused or flushed an
  existing datastore.

One 30 ms lease-renewal test failed once on a targeted rerun because scheduler
and fsync delay could consume the entire synthetic lease. The test now uses a
100 ms lease and a 250 ms provider delay, retaining the multiple-renewal
assertion without depending on sub-30 ms scheduling. No runtime lease behavior
was changed for this issue.

## Five-run healthy comparison

Each run used 100 identical cold requests, four workers, one shared Redis
Clone, the fake provider, a one-second delay, and a fresh project-scoped volume.
All ten runs completed 100/100 jobs with no failure or timeout.
The five preserved single-flight AOF volumes were replayed and queried using
their exact prompt-derived keys after the retention window; all returned no
record, confirming that none remained `ACTIVE` (the terminal records had been
eligible for normal lazy retirement).

| Measurement | Baseline median (range; population stdev) | Single-flight median (range; population stdev) |
|---|---:|---:|
| Provider executions | 4 (4–4; 0) | 1 (1–1; 0) |
| Accepted publications | 0 | 1 (1–1; 0) |
| Leaders / followers | 0 / 0 | 1 / 3 |
| Total duration | 1,509.22 ms (1,337.10–1,683.41; 113.45) | 1,801.36 ms (1,666.89–1,899.83; 86.45) |
| p50 completion | 1,332.86 ms (1,180.67–1,420.81; 85.89) | 1,626.83 ms (1,454.85–1,715.68; 102.56) |
| p95 completion | 1,468.46 ms (1,285.10–1,634.66; 116.28) | 1,730.24 ms (1,617.84–1,833.53; 89.16) |
| p99 completion | 1,474.62 ms (1,296.28–1,647.17; 114.53) | 1,744.00 ms (1,622.51–1,842.30; 90.18) |
| Total follower wait | 0 | 4,194 ms (4,073–4,310; 75.60) |
| AOF bytes after run | 303,684 (303,681–303,692; 4.03) | 305,548 (305,546–305,549; 1.36) |

The median single-flight duration was 292.15 ms (19.36%) slower. This workload
therefore proves provider-call reduction, not a latency improvement. Multiple
initial semantic misses remain legitimate: every single-flight run recorded
four misses and 99 returned cache hits while still executing the provider once.
The harness could not obtain useful CPU/memory samples during these short
runs. Post-workload point samples from the longer mixed experiment showed
roughly 22.1 MiB per worker, 36.1–37.7 MiB for the API, and 10.5–10.8 MiB for
Redis Clone; CPU values were low and are not workload averages.

Raw results: `benchmarks/results/repeated-20261109/`.

## Real worker-kill verification

Each case used eight identical requests, a two-second flight lease, a five-
second job claim lease and deterministic, disabled-by-default worker pauses.
The actual leader container was sent `SIGKILL`.

| Kill point | Final jobs | Recovery | Provider attempts | Accepted publications | Leaders / takeovers | Timeouts |
|---|---:|---:|---:|---:|---:|---:|
| After acquire, before provider | 8/8 completed | 4.967 s | 1 | 1 | 2 / 1 | 0 |
| During provider boundary | 8/8 completed | 5.500 s | 2 | 1 | 2 / 1 | 0 |
| After commit, before job finish | 8/8 completed | 5.210 s | 1 | 1 | 1 / 0 | 0 |

The post-commit case recovered through the existing job claim lease; followers
read the already-published result and no flight takeover was needed. A separate
live fencing case expired generation 1, acquired generation 2, rejected the
generation-1 commit (`0`), accepted generation 2 (`1`), and resolved to its new
cache key.

This establishes at-most-one accepted fenced publication. It does not provide
exactly-once external execution: killing a leader after entering the provider
boundary caused two counted attempts. Provider-call counters are persisted by
the datastore before provider generation, while publication count is included
in the atomic `FLIGHTCOMMITSTATE`. A simultaneous datastore crash can still
have the repository's documented AOF/OS durability limitations.

Raw result: `benchmarks/results/faults-20261109.json`.

## Head-of-line blocking

Three control/mixed pairs were run with fresh volumes. Control runs submitted
four unrelated cold prompts. Mixed runs first submitted 20 identical cold
prompts, then the same count of unrelated cold prompts. Four synchronous
workers were used throughout.

| Unrelated-job measurement | Control median across runs | Mixed median across runs |
|---|---:|---:|
| p50 queue wait | 37.85 ms | 1,351.56 ms |
| p95 queue wait | 58.17 ms | 1,516.35 ms |
| p50 completion | 1,044.25 ms | 2,364.28 ms |
| p95 completion | 1,068.03 ms | 2,525.31 ms |

Mixed p50 completion was 1,320.03 ms slower, or 2.26 times control. Every run
completed all jobs, and mixed runs used five provider calls (one coordinated
duplicate computation plus four unrelated computations). Synchronous followers
therefore materially occupy worker slots and delay unrelated FIFO queue work.
Possible later remedies include parking follower jobs outside worker slots,
continuation/requeue with bounded delay, or separating coordination waiting
from execution capacity. No queue redesign was made in Week 1.

Raw result: `benchmarks/results/hol-20261109.json`.

## Resource and state growth

- Terminal flight records are retained for 60 seconds and lazily purged at
  most once per second on flight acquire/status traffic. A deterministic test
  now verifies that 100 terminal records are removed in one eligible pass.
- Cleanup is an O(number of live flight records) scan under the datastore lock.
  Memory is bounded by recent terminal throughput while flight traffic
  continues, but very high cardinality can make the scan disruptive.
- Cleanup does not compact the AOF. Flight state, renewals and terminal records
  remain in the append-only history. The healthy single-flight workload added
  a median 1,864 bytes (0.61%) relative to baseline. Long-lived deployments
  still need AOF rewrite/compaction and restart-memory testing.
- Coordination state mutations request fsync, including acquisitions,
  renewals, failures/releases and commits. The end-to-end comparison includes
  this cost, but no isolated datastore coordination-latency histogram exists,
  so fsync overhead cannot be separated from polling and scheduling here.
- Every worker uses one persistent client socket. Follower polling, lease
  renewal, claim heartbeat and job work share that socket under an `RLock`;
  polling did not create connection growth in these tests.
- Lease renewers stop in `_run_flight_leader`'s `finally` block. The client has
  no socket I/O timeout, so a half-open datastore connection can leave a daemon
  renewer blocked and hold the shared client lock past its join timeout. This
  was not reproduced by container-kill tests because the datastore stayed
  healthy and should be addressed in reliability/backpressure work.

## Conclusion and gate

**Decision recorded 2026-11-10: Week 1's scoped single-flight milestone is
accepted.** Head-of-line blocking and socket I/O timeouts move to high-priority
Week 2 work. Durable AOF compaction remains deferred technical debt.

Healthy-scope acceptance is met: one provider execution, one accepted
publication, 100 successful jobs, correct follower results, and no abandoned
active flight in every repeated run. Worker crashes recover without indefinite
follower waits, and stale generations cannot publish. No critical healthy-path
correctness defect was found.

The accepted scope does not claim production readiness. Synchronous-follower
head-of-line blocking and missing socket timeouts remain high-priority Week 2
risks. Unbounded AOF history remains a deferred production resource/recovery
risk. Healthy operation targets one provider execution; arbitrary crashes may
still produce duplicate external attempts, while fencing limits accepted cache
publication to the current generation.
