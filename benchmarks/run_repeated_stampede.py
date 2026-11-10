import argparse
import json
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path


def run(command, cwd=None, check=True):
    return subprocess.run(
        command,
        cwd=cwd,
        check=check,
        capture_output=True,
        text=True,
    )


def parse_bytes(value):
    number, unit = value.strip().split()[:2]
    factors = {
        "B": 1,
        "kB": 1000,
        "KB": 1000,
        "KiB": 1024,
        "MB": 1000 ** 2,
        "MiB": 1024 ** 2,
        "GB": 1000 ** 3,
        "GiB": 1024 ** 3,
    }
    return float(number) * factors[unit]


def sample_resources(project, compose_file, stop_event, samples):
    while not stop_event.is_set():
        try:
            ids = run(
                [
                    "docker", "compose", "--project-name", project,
                    "-f", str(compose_file), "ps", "-q",
                ]
            ).stdout.split()
            if ids:
                output = run(
                    ["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids]
                ).stdout
                for line in output.splitlines():
                    item = json.loads(line)
                    used = item["MemUsage"].split("/")[0].strip()
                    samples.append({
                        "name": item["Name"],
                        "cpu_percent": float(item["CPUPerc"].rstrip("%")),
                        "memory_bytes": parse_bytes(used),
                    })
        except (KeyError, ValueError, OSError, subprocess.CalledProcessError, json.JSONDecodeError):
            pass
        stop_event.wait(0.1)


def summarize_resources(samples):
    if not samples:
        return {"sample_count": 0, "max_memory_bytes": None, "median_cpu_percent": None}
    return {
        "sample_count": len(samples),
        "max_memory_bytes": max(item["memory_bytes"] for item in samples),
        "median_cpu_percent": statistics.median(
            item["cpu_percent"] for item in samples
        ),
    }


def wait_for_health(base_url, timeout=60):
    import urllib.request

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(base_url + "/health", timeout=2) as response:
                if json.load(response).get("status") == "healthy":
                    return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError(f"API did not become healthy: {base_url}")


def aof_size(project, compose_file):
    result = run(
        [
            "docker", "compose", "--project-name", project,
            "-f", str(compose_file), "exec", "-T", "redis_clone",
            "stat", "-c", "%s", "/app/data/appendonly.aof",
        ],
        check=False,
    )
    try:
        return int(result.stdout.strip())
    except ValueError:
        return None


def run_one(args, variant, source_dir, source_revision, run_number):
    compose_file = source_dir / "docker-compose.benchmark.yml"
    project = f"{args.project_prefix}-{variant}-{run_number:02d}"
    output = args.output_dir / f"{variant}-run-{run_number:02d}.json"
    compose = [
        "docker", "compose", "--project-name", project,
        "-f", str(compose_file),
    ]
    run([*compose, "up", "-d", "--build", "--scale", "worker=4"])
    try:
        wait_for_health(args.base_url)
        samples = []
        stop_event = threading.Event()
        sampler = threading.Thread(
            target=sample_resources,
            args=(project, compose_file, stop_event, samples),
            daemon=True,
        )
        sampler.start()
        try:
            run([
                sys.executable,
                str(args.benchmark_script),
                "--base-url", args.base_url,
                "--requests", str(args.requests),
                "--workers", "4",
                "--output", str(output),
            ])
        finally:
            stop_event.set()
            sampler.join(timeout=5)

        result = json.loads(output.read_text(encoding="utf-8"))
        result["comparison_variant"] = variant
        result["compose_project"] = project
        result["benchmark_harness_git_commit"] = result.pop("git_commit", None)
        result["benchmark_harness_worktree_dirty"] = result.pop(
            "git_worktree_dirty", None
        )
        result["workload_git_commit"] = source_revision
        result["resources"] = summarize_resources(samples)
        result["aof_size_bytes"] = aof_size(project, compose_file)
        output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        return output, result
    finally:
        run([*compose, "down"], check=False)


def distribution(values):
    numeric = [value for value in values if value is not None]
    if not numeric:
        return {"count": 0, "median": None, "min": None, "max": None, "stdev": None}
    return {
        "count": len(numeric),
        "median": statistics.median(numeric),
        "min": min(numeric),
        "max": max(numeric),
        "stdev": statistics.pstdev(numeric),
    }


def aggregate_variant(results):
    paths = {
        "completed_jobs": lambda item: item["summary"]["completed_jobs"],
        "failed_jobs": lambda item: item["summary"]["failed_jobs"],
        "provider_calls": lambda item: item["summary"]["provider_calls"],
        "publications": lambda item: item["metric_deltas"].get("flight_publications", 0),
        "leaders": lambda item: item["metric_deltas"].get("flight_leaders_acquired", 0),
        "followers": lambda item: item["metric_deltas"].get("flight_followers_joined", 0),
        "duration_ms": lambda item: item["summary"]["experiment_duration_ms"],
        "p50_ms": lambda item: item["summary"]["latency_ms"]["p50"],
        "p95_ms": lambda item: item["summary"]["latency_ms"]["p95"],
        "p99_ms": lambda item: item["summary"]["latency_ms"]["p99"],
        "flight_wait_ms": lambda item: item["metric_deltas"].get(
            "flight_follower_wait_ms_total", 0
        ),
        "max_memory_bytes": lambda item: item["resources"]["max_memory_bytes"],
        "median_cpu_percent": lambda item: item["resources"]["median_cpu_percent"],
        "aof_size_bytes": lambda item: item["aof_size_bytes"],
    }
    return {
        name: distribution([getter(item) for item in results])
        for name, getter in paths.items()
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-source", type=Path, required=True)
    parser.add_argument("--singleflight-source", type=Path, required=True)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--singleflight-revision", required=True)
    parser.add_argument("--benchmark-script", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--base-url", default="http://127.0.0.1:18000")
    parser.add_argument("--project-prefix", default="mini-redis-repeat-20261109")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    all_results = {}
    files = {}
    for variant, source, source_revision in (
        ("baseline", args.baseline_source, args.baseline_revision),
        ("singleflight", args.singleflight_source, args.singleflight_revision),
    ):
        all_results[variant] = []
        files[variant] = []
        for run_number in range(1, args.runs + 1):
            output, result = run_one(
                args, variant, source, source_revision, run_number
            )
            files[variant].append(output.name)
            all_results[variant].append(result)
            print(
                f"{variant} run {run_number}: "
                f"duration={result['summary']['experiment_duration_ms']:.2f}ms "
                f"providers={result['summary']['provider_calls']}",
                flush=True,
            )

    aggregate = {
        "schema_version": 1,
        "runs_per_variant": args.runs,
        "request_count": args.requests,
        "worker_count": 4,
        "workload_revisions": {
            "baseline": args.baseline_revision,
            "singleflight": args.singleflight_revision,
        },
        "files": files,
        "variants": {
            variant: aggregate_variant(results)
            for variant, results in all_results.items()
        },
    }
    baseline_duration = aggregate["variants"]["baseline"]["duration_ms"]["median"]
    flight_duration = aggregate["variants"]["singleflight"]["duration_ms"]["median"]
    aggregate["median_duration_delta_ms"] = flight_duration - baseline_duration
    aggregate["median_duration_delta_percent"] = (
        (flight_duration - baseline_duration) / baseline_duration * 100
    )
    aggregate_path = args.output_dir / "aggregate-comparison.json"
    aggregate_path.write_text(json.dumps(aggregate, indent=2, sort_keys=True) + "\n")
    print(json.dumps(aggregate, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
