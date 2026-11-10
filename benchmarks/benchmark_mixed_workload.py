import argparse
import json
import os
import platform
import statistics
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from benchmark_stampede import ApiClient, metric_delta, percentile, utc_now


TERMINAL = {"completed", "failed", "dead"}


def run(command, env=None, check=True):
    return subprocess.run(
        command,
        check=check,
        capture_output=True,
        text=True,
        env=env,
    )


def compose_command(project, compose_file, *args):
    return [
        "docker", "compose", "--project-name", project,
        "-f", str(compose_file), *args,
    ]


def command_version(command):
    result = run(command, check=False)
    return result.stdout.strip() or None


def environment_metadata():
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
        "docker_version": command_version(["docker", "--version"]),
        "docker_compose_version": command_version(["docker", "compose", "version"]),
        "git_commit": command_version(["git", "rev-parse", "HEAD"]),
    }


def wait_health(client, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if client.get_health().get("status") == "healthy":
                return
        except Exception:
            pass
        time.sleep(0.25)
    raise RuntimeError("mixed-workload API did not become healthy")


def parse_time(value):
    if not value:
        return None
    return datetime.fromisoformat(value)


def job_measurement(job, submitted_at):
    started = parse_time(job.get("started_at"))
    completed = parse_time(job.get("completed_at") or job.get("failed_at"))
    created = parse_time(job.get("created_at")) or submitted_at
    return {
        "job_id": job.get("id"),
        "status": job.get("status"),
        "cache": (job.get("result") or {}).get("cache"),
        "queue_wait_ms": (started - created).total_seconds() * 1000 if started else None,
        "completion_ms": (completed - created).total_seconds() * 1000 if completed else None,
    }


def summarize_jobs(jobs):
    completed = [job for job in jobs if job["status"] == "completed"]
    waits = [job["queue_wait_ms"] for job in completed if job["queue_wait_ms"] is not None]
    latencies = [job["completion_ms"] for job in completed if job["completion_ms"] is not None]
    return {
        "count": len(jobs),
        "completed": len(completed),
        "failed": sum(job["status"] in {"failed", "dead"} for job in jobs),
        "queue_wait_ms": {
            "p50": percentile(waits, 0.50),
            "p95": percentile(waits, 0.95),
            "max": max(waits) if waits else None,
        },
        "completion_ms": {
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "max": max(latencies) if latencies else None,
        },
    }


def poll_jobs(client, submissions, timeout):
    deadline = time.monotonic() + timeout
    latest = {}
    while time.monotonic() < deadline:
        for job_id, submitted_at in submissions.items():
            if latest.get(job_id, {}).get("status") in TERMINAL:
                continue
            latest[job_id] = client.get_job(job_id)
        if len(latest) == len(submissions) and all(
            job.get("status") in TERMINAL for job in latest.values()
        ):
            break
        time.sleep(0.05)
    return [job_measurement(latest.get(job_id, {}), submitted_at) for job_id, submitted_at in submissions.items()]


def submit_prompts(client, prompts):
    barrier = threading.Barrier(len(prompts))

    def submit(prompt):
        barrier.wait()
        response = client.submit(prompt, "fake")
        return response["job_id"], datetime.now().astimezone()

    submissions = {}
    with ThreadPoolExecutor(max_workers=len(prompts)) as executor:
        futures = [executor.submit(submit, prompt) for prompt in prompts]
        for future in as_completed(futures):
            job_id, submitted_at = future.result()
            submissions[job_id] = submitted_at
    return submissions


def resource_snapshot(project, compose_file):
    ids = run(compose_command(project, compose_file, "ps", "-q")).stdout.split()
    if not ids:
        return []
    output = run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids],
        check=False,
    ).stdout
    snapshots = []
    for line in output.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        snapshots.append({
            "name": item.get("Name"),
            "cpu_percent": item.get("CPUPerc"),
            "memory_usage": item.get("MemUsage"),
        })
    return snapshots


def run_scenario(args, scenario, run_number):
    started_at = utc_now()
    project = f"{args.project_prefix}-{scenario}-{run_number:02d}"
    command = compose_command(project, args.compose_file)
    environment = os.environ.copy()
    environment["BENCHMARK_API_PORT"] = str(args.api_port)
    run([*command, "up", "-d", "--build", "--scale", "worker=4"], env=environment)
    client = ApiClient(args.base_url, 10)
    try:
        wait_health(client)
        before = client.get_metrics()
        run_token = uuid.uuid4().hex
        duplicate_submissions = {}
        if scenario == "mixed":
            duplicate_submissions = submit_prompts(
                client,
                [f"hol-duplicate-{run_token}"] * args.duplicate_requests,
            )
        unrelated_submissions = submit_prompts(
            client,
            [f"hol-unrelated-{run_token}-{index}" for index in range(args.unrelated_requests)],
        )
        all_submissions = {**duplicate_submissions, **unrelated_submissions}
        jobs = poll_jobs(client, all_submissions, args.timeout_seconds)
        unrelated_ids = set(unrelated_submissions)
        duplicate_ids = set(duplicate_submissions)
        after = client.get_metrics()
        resources = resource_snapshot(project, args.compose_file)
        return {
            "scenario": scenario,
            "run_number": run_number,
            "started_at": started_at,
            "finished_at": utc_now(),
            "project": project,
            "configuration": {
                "workers": 4,
                "duplicate_requests": len(duplicate_submissions),
                "unrelated_requests": len(unrelated_submissions),
                "inference_delay_seconds": 1,
            },
            "metric_deltas": metric_delta(before, after),
            "unrelated": summarize_jobs([job for job in jobs if job["job_id"] in unrelated_ids]),
            "duplicates": summarize_jobs([job for job in jobs if job["job_id"] in duplicate_ids]),
            "resource_snapshot": resources,
            "jobs": jobs,
        }
    finally:
        run([*command, "down"], env=environment, check=False)


def distribution(values):
    values = [value for value in values if value is not None]
    return {
        "count": len(values),
        "median": statistics.median(values) if values else None,
        "min": min(values) if values else None,
        "max": max(values) if values else None,
        "stdev": statistics.pstdev(values) if values else None,
    }


def aggregate(results):
    by_scenario = {}
    for scenario in ("control", "mixed"):
        selected = [result for result in results if result["scenario"] == scenario]
        by_scenario[scenario] = {
            "unrelated_queue_wait_p50_ms": distribution(
                [result["unrelated"]["queue_wait_ms"]["p50"] for result in selected]
            ),
            "unrelated_completion_p50_ms": distribution(
                [result["unrelated"]["completion_ms"]["p50"] for result in selected]
            ),
            "provider_calls": distribution(
                [result["metric_deltas"]["provider_call_count"] for result in selected]
            ),
        }
    control = by_scenario["control"]["unrelated_completion_p50_ms"]["median"]
    mixed = by_scenario["mixed"]["unrelated_completion_p50_ms"]["median"]
    return {
        "scenarios": by_scenario,
        "unrelated_completion_median_delta_ms": mixed - control,
        "unrelated_completion_median_ratio": mixed / control,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--compose-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--duplicate-requests", type=int, default=20)
    parser.add_argument("--unrelated-requests", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=float, default=60)
    parser.add_argument("--api-port", type=int, default=18000)
    parser.add_argument("--base-url", default="http://127.0.0.1:18000")
    parser.add_argument("--project-prefix", default="mini-redis-hol-20261109")
    args = parser.parse_args()
    results = []
    for run_number in range(1, args.runs + 1):
        for scenario in ("control", "mixed"):
            result = run_scenario(args, scenario, run_number)
            results.append(result)
            print(json.dumps({
                "scenario": scenario,
                "run": run_number,
                "unrelated": result["unrelated"],
            }), flush=True)
    payload = {
        "schema_version": 1,
        "generated_at": utc_now(),
        "environment": environment_metadata(),
        "runs_per_scenario": args.runs,
        "results": results,
        "aggregate": aggregate(results),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(json.dumps(payload["aggregate"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
