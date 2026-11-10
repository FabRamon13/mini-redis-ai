# Benchmarks

## Four-worker cache-stampede baseline

This experiment measures duplicate backend provider executions when identical,
cold inference requests overlap across four workers. It uses the fake provider
and a one-second inference delay. No request coalescing is enabled.

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
from `/jobs/metrics`; it is incremented by workers only after actual provider
execution. Cache hits and misses are also measured from backend counter deltas.

Inspect the generated JSON before stopping the isolated stack. Stop containers
without deleting the fresh volume so the AOF remains available for audit:

```bash
docker compose -f docker-compose.benchmark.yml down
```

The reported latencies are client-observed submission-to-terminal-state times.
They include HTTP submission, queueing, worker execution, and polling delay.
Host scheduling, Docker resources, and the polling interval affect the values.
