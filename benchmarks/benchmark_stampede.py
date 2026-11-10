import argparse
import json
import math
import os
import platform
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path


TERMINAL_STATUSES = {"completed", "failed", "dead"}
METRIC_KEYS = (
    "processed_jobs",
    "failed_jobs",
    "dead_jobs",
    "semantic_cache_hits",
    "semantic_cache_misses",
    "provider_call_count",
    "linear_search_count",
    "flight_leaders_acquired",
    "flight_followers_joined",
    "flight_publications",
    "flight_takeovers",
    "flight_failed",
    "flight_fencing_rejections",
    "flight_follower_wait_ms_total",
    "flight_timeouts",
    "flight_cache_recheck_hits",
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def percentile(values, quantile):
    if not values:
        return None
    if not 0.0 <= quantile <= 1.0:
        raise ValueError("quantile must be between 0 and 1")

    ordered = sorted(values)
    rank = (len(ordered) - 1) * quantile
    lower = math.floor(rank)
    upper = math.ceil(rank)

    if lower == upper:
        return ordered[lower]

    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def metric_delta(before, after):
    return {
        key: after.get(key, 0) - before.get(key, 0)
        for key in METRIC_KEYS
    }


def summarize_records(records):
    completed = [record for record in records if record["status"] == "completed"]
    failed = [record for record in records if record["status"] in {"failed", "dead"}]
    timed_out = [record for record in records if record["status"] == "timed_out"]
    poll_errors = [record for record in records if record["status"] == "poll_error"]
    submission_errors = [
        record for record in records if record["status"] == "submission_error"
    ]
    latencies = [record["latency_ms"] for record in completed]

    return {
        "submitted_jobs": len(records) - len(submission_errors),
        "completed_jobs": len(completed),
        "failed_jobs": len(failed),
        "timed_out_jobs": len(timed_out),
        "poll_error_jobs": len(poll_errors),
        "submission_errors": len(submission_errors),
        "cache_hits": sum(record.get("cache") == "hit" for record in completed),
        "cache_misses": sum(record.get("cache") == "miss" for record in completed),
        "latency_ms": {
            "count": len(latencies),
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
        },
    }


class ApiClient:
    def __init__(self, base_url, timeout_seconds):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def request_json(self, path, method="GET", payload=None):
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = urllib.request.Request(
            self.base_url + path,
            data=body,
            method=method,
            headers={"Content-Type": "application/json"},
        )

        try:
            with urllib.request.urlopen(
                request,
                timeout=self.timeout_seconds,
            ) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"{method} {path} failed with HTTP {exc.code}: {detail}"
            ) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"{method} {path} failed: {exc.reason}") from exc

    def get_health(self):
        return self.request_json("/health")

    def get_metrics(self):
        return self.request_json("/jobs/metrics")

    def submit(self, prompt, provider):
        return self.request_json(
            "/inference",
            method="POST",
            payload={"prompt": prompt, "provider": provider},
        )

    def get_job(self, job_id):
        return self.request_json(f"/jobs/{job_id}")


def submit_one(client, prompt, provider, barrier, clock=time.perf_counter):
    barrier.wait()
    started = clock()

    try:
        response = client.submit(prompt, provider)
        return {
            "job_id": response["job_id"],
            "submitted_monotonic": started,
            "status": "submitted",
        }
    except Exception as exc:
        return {
            "job_id": None,
            "submitted_monotonic": started,
            "status": "submission_error",
            "error": str(exc),
        }


def poll_one(
    client,
    submission,
    timeout_seconds,
    poll_seconds,
    clock=time.perf_counter,
    sleeper=time.sleep,
):
    job_id = submission["job_id"]
    started = submission["submitted_monotonic"]
    deadline = clock() + timeout_seconds

    while clock() < deadline:
        try:
            job = client.get_job(job_id)
        except Exception as exc:
            return {
                "job_id": job_id,
                "status": "poll_error",
                "latency_ms": (clock() - started) * 1_000,
                "cache": None,
                "error": str(exc),
            }

        status = job.get("status")
        if status in TERMINAL_STATUSES:
            result = job.get("result") or {}
            return {
                "job_id": job_id,
                "status": status,
                "latency_ms": (clock() - started) * 1_000,
                "cache": result.get("cache"),
                "error": job.get("error"),
            }

        sleeper(poll_seconds)

    return {
        "job_id": job_id,
        "status": "timed_out",
        "latency_ms": (clock() - started) * 1_000,
        "cache": None,
        "error": f"job did not finish within {timeout_seconds} seconds",
    }


def submit_burst(client, prompt, provider, request_count):
    barrier = threading.Barrier(request_count)
    records = []

    with ThreadPoolExecutor(max_workers=request_count) as executor:
        futures = [
            executor.submit(submit_one, client, prompt, provider, barrier)
            for _ in range(request_count)
        ]
        for future in as_completed(futures):
            records.append(future.result())

    return records


def poll_submissions(client, submissions, timeout_seconds, poll_seconds, workers):
    records = [
        {
            "job_id": submission["job_id"],
            "status": "submission_error",
            "latency_ms": None,
            "cache": None,
            "error": submission["error"],
        }
        for submission in submissions
        if submission["status"] == "submission_error"
    ]
    accepted = [
        submission for submission in submissions if submission["status"] == "submitted"
    ]

    with ThreadPoolExecutor(max_workers=min(workers, max(1, len(accepted)))) as executor:
        futures = [
            executor.submit(
                poll_one,
                client,
                submission,
                timeout_seconds,
                poll_seconds,
            )
            for submission in accepted
        ]
        for future in as_completed(futures):
            records.append(future.result())

    return records


def git_commit():
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def git_worktree_dirty():
    try:
        return bool(
            subprocess.run(
                ["git", "status", "--porcelain"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        )
    except (OSError, subprocess.CalledProcessError):
        return None


def command_version(command):
    try:
        return subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def environment_metadata():
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
        "docker_version": command_version(["docker", "--version"]),
        "docker_compose_version": command_version(["docker", "compose", "version"]),
        "compose_project_name": os.getenv("COMPOSE_PROJECT_NAME"),
    }


def write_result(result, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def run_benchmark(args):
    client = ApiClient(args.base_url, args.http_timeout_seconds)
    health = client.get_health()
    if health.get("status") != "healthy":
        raise RuntimeError(f"benchmark API is not healthy: {health}")

    run_id = str(uuid.uuid4())
    prompt = f"stampede-baseline-{run_id}"
    started_at = utc_now()
    experiment_start = time.perf_counter()
    before = client.get_metrics()
    submissions = submit_burst(client, prompt, args.provider, args.requests)
    records = poll_submissions(
        client,
        submissions,
        args.job_timeout_seconds,
        args.poll_seconds,
        args.poll_workers,
    )
    after = client.get_metrics()
    duration_ms = (time.perf_counter() - experiment_start) * 1_000
    finished_at = utc_now()
    deltas = metric_delta(before, after)
    summary = summarize_records(records)
    summary["provider_calls"] = deltas["provider_call_count"]
    summary["semantic_cache_hits"] = deltas["semantic_cache_hits"]
    summary["semantic_cache_misses"] = deltas["semantic_cache_misses"]
    summary["experiment_duration_ms"] = duration_ms

    return {
        "schema_version": 1,
        "run_id": run_id,
        "prompt": prompt,
        "started_at": started_at,
        "finished_at": finished_at,
        "git_commit": git_commit(),
        "git_worktree_dirty": git_worktree_dirty(),
        "environment": environment_metadata(),
        "configuration": {
            "base_url": args.base_url,
            "provider": args.provider,
            "request_count": args.requests,
            "worker_count": args.workers,
            "inference_delay_seconds": args.inference_delay_seconds,
            "queue_capacity": args.queue_capacity,
            "job_timeout_seconds": args.job_timeout_seconds,
            "http_timeout_seconds": args.http_timeout_seconds,
            "poll_seconds": args.poll_seconds,
            "poll_workers": args.poll_workers,
        },
        "metrics_before": before,
        "metrics_after": after,
        "metric_deltas": deltas,
        "summary": summary,
        "jobs": sorted(records, key=lambda item: item.get("job_id") or ""),
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Measure duplicate provider work during a cold-cache stampede.",
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:18000")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--provider", choices=("fake",), default="fake")
    parser.add_argument("--inference-delay-seconds", type=float, default=1.0)
    parser.add_argument("--queue-capacity", type=int, default=200)
    parser.add_argument("--job-timeout-seconds", type=float, default=120.0)
    parser.add_argument("--http-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--poll-seconds", type=float, default=0.05)
    parser.add_argument("--poll-workers", type=int, default=32)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    for name in ("requests", "workers", "queue_capacity", "poll_workers"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    for name in (
        "inference_delay_seconds",
        "job_timeout_seconds",
        "http_timeout_seconds",
        "poll_seconds",
    ):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")

    return args


def main():
    args = parse_args()
    result = run_benchmark(args)
    output_path = args.output
    if output_path is None:
        stamp = result["started_at"].replace(":", "").replace("+00:00", "Z")
        output_path = Path("benchmarks/results") / f"stampede-{stamp}-{result['run_id']}.json"
    write_result(result, output_path)

    print(json.dumps({"output": str(output_path), **result["summary"]}, indent=2))

    summary = result["summary"]
    if any(
        summary[key]
        for key in ("failed_jobs", "timed_out_jobs", "poll_error_jobs", "submission_errors")
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
