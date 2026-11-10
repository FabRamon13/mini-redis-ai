# Benchmarks

## Four-worker cache-stampede baseline and single-flight comparison

This experiment measures duplicate backend provider executions when identical,
cold inference requests overlap across four workers. It uses the fake provider
and a one-second inference delay. The preserved `stampede-baseline-*.json`
artifacts predate request coalescing; `stampede-singleflight-*.json` artifacts
exercise the same workload with distributed single-flight enabled.

Use a unique Compose project name for every run. Compose then creates a new,
project-scoped Redis Clone volume without flushing or deleting another stack's
data:

```bash
export COMPOSE_PROJECT_NAME="mini-redis-stampede-$(date -u +%Y%m%d%H%M%S)"
docker compose -f docker-compose.benchmark.yml up -d --build --scale worker=4
docker compose -f docker-compose.benchmark.yml ps

python3 benchmarks/benchmark_stampede.py \
  --base-url http://127.0.0.1:18000 \
  --requests 100 \
  --workers 4
```

The script generates a unique prompt, submits all requests behind a thread
barrier, polls every accepted job, and writes raw JSON to
`benchmarks/results/`. `provider_call_count` is read before and after the run
from `/jobs/metrics`; it is incremented immediately before each actual provider
invocation. Cache, leadership, follower, publication, takeover, fencing and
timeout counters are also measured from backend deltas.

The prompt is deliberately a unique synthetic exact-match key repeated for all
100 requests. This measures coordination correctness for identical requests;
it is not a semantic-quality or realistic-prompt benchmark.

Inspect the generated JSON before stopping the isolated stack. Stop containers
without deleting the fresh volume so the AOF remains available for audit:

```bash
docker compose -f docker-compose.benchmark.yml down
```

The reported latencies are client-observed submission-to-terminal-state times.
They include HTTP submission, queueing, worker execution, and polling delay.
Host scheduling, Docker resources, and the polling interval affect the values.

## Step 3 repeated, failure, and queue-interference verification

The Step 3 harnesses use a unique Compose project for every case. They call
`docker compose down` without `--volumes`, preserving each isolated AOF volume
for inspection and never flushing a development or production datastore.

Run five equivalent baseline and single-flight samples (the baseline source is
an exported tree at commit `c960af3`):

```bash
python3 benchmarks/run_repeated_stampede.py \
  --baseline-source /path/to/exported-c960af3 \
  --singleflight-source . \
  --baseline-revision c960af328b06ac93a8cb6ad2a026b1156d2d63b6 \
  --singleflight-revision 7bec46bb575772559b95948b4edd299ad8b0cfb9 \
  --benchmark-script benchmarks/benchmark_stampede.py \
  --output-dir benchmarks/results/repeated-YYYYMMDD \
  --runs 5 --requests 100
```

Run the deterministic process-kill campaign. The override enables fault pauses
only in the isolated worker containers; the hooks are disabled by default:

```bash
python3 benchmarks/run_singleflight_faults.py \
  --compose-file docker-compose.benchmark.yml \
  --fault-compose-file docker-compose.fault.yml \
  --output benchmarks/results/faults-YYYYMMDD.json
```

Run three control/mixed pairs to measure whether synchronous followers delay
unrelated jobs. Each mixed run queues 20 identical requests before four unique
unrelated requests; each control run contains only the four unrelated jobs:

```bash
python3 benchmarks/benchmark_mixed_workload.py \
  --compose-file docker-compose.benchmark.yml \
  --output benchmarks/results/hol-YYYYMMDD.json \
  --runs 3 --duplicate-requests 20 --unrelated-requests 4
```

These are coordination and queue-behavior workloads, not semantic-quality
evaluations. Docker CPU snapshots are point-in-time observations; short runs
may finish before a useful sample is returned. Provider execution remains
at-least-once across worker crashes even though fenced publication accepts at
most one current generation.
