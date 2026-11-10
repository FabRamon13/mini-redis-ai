# Mini-Redis AI — Project Context

## Why it exists
A backend/AI infrastructure engineering showcase, with potential to become a developer-facing AI caching service. Demonstrate systems design and operational evidence for SWE roles in backend, distributed systems and AI infrastructure. Avoid a shallow feature checklist: explain tradeoffs, correctness guarantees and measurable impact.

## Existing repository (verify against current branch)
Repository: `FabRamon13/mini-redis-ai`.

- `redis_clone/`: own Redis-inspired TCP datastore, RESP handling, GET/SET/DELETE/MGET/MSET/TTL, queue primitives, append-only persistence and replay.
- `fastapi_cache/main.py`: FastAPI endpoints `/inference`, `/jobs/{job_id}`, `/jobs/metrics`, `/metrics`, `/health`, and cache-aside demo endpoints. `/inference` validates/enqueues a job and returns a job ID; inference is not awaited by the HTTP request.
- `worker/worker.py`: background job claiming with claim tokens and leases, heartbeat and stale-claim recovery; `process_inference` embeds prompts, reads semantic records, runs vector search, reuses hits or calls `generate_response` on misses, writes new entries, finishes jobs.
- `worker/semantic_cache.py`: semantic record serialization/model signature logic.
- `worker/faiss_index.py`: FAISS indexes scoped by provider/model identity; indexes are local/rebuildable, not authoritative.
- `ai/`: embedding router, vector store, inference routing.
- `providers/`: fake and Hugging Face response providers.
- `observability/`, `prometheus/`: structured logging, metrics and monitoring.
- `tests/`, `benchmarks/`: validation and benchmark scripts.
- `docker-compose.yml`, `docker-compose.prod.yml`: local and production service definition; worker service can be scaled with `docker compose up -d --build --scale worker=4` in local mode.

## Previous evidence (not v2 outcomes)
README previously reported 170 tests (165 passing, 5 skipped), 56.5% semantic hit rate on a deployed 46-request validation workload, 26 calls avoided, 0 false-positive negative controls, and 46/46 jobs completed. Revalidate before repeating these claims.

## Request path today
`POST /inference` -> enqueue to custom datastore -> independent worker claims -> embed -> fetch semantic cache entries -> FAISS/linear top-k lookup -> if score exceeds threshold return cached response; otherwise call provider and persist cache record -> worker finishes -> client polls job status.

## Why v2 begins with concurrency
With multiple workers, overlapping misses can each call the provider before another worker saves a result. With a single worker, this race is masked by serialization. Baseline first, then introduce an atomic cross-worker single-flight protocol, failure handling and metrics. Do not confuse semantic deduplication of sequential requests with in-flight coalescing.

## Architectural caution
Current code performs semantic search, not necessarily a separate exact-match inference cache. Model identity checks exist, but tenant/knowledge-version isolation is future work. Existing queue leases and retries do not by themselves guarantee exactly-once inference. Distributed single-flight guarantees must specify process and failure scope, and at-least-once execution may still allow duplicate provider calls on crashes without stronger fencing/idempotency mechanisms.
