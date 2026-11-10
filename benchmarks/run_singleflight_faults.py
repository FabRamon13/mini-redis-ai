import argparse
import json
import os
import platform
import re
import subprocess
import time
from pathlib import Path

from benchmark_stampede import ApiClient, metric_delta, submit_burst, utc_now


TERMINAL = {"completed", "failed", "dead"}


def run(command, env=None, check=True):
    return subprocess.run(
        command,
        check=check,
        capture_output=True,
        text=True,
        env=env,
    )


def compose_command(project, compose_files, *args):
    command = ["docker", "compose", "--project-name", project]
    for compose_file in compose_files:
        command.extend(["-f", str(compose_file)])
    command.extend(args)
    return command


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
            time.sleep(0.25)
    raise RuntimeError("fault-test API did not become healthy")


def wait_for_fault_log(project, compose_files, phase, timeout=30):
    deadline = time.monotonic() + timeout
    pattern = re.compile(
        rf"^(worker-\d+)\s+\|.*singleflight_fault_pause.*\"phase\":\"{phase}\"",
        re.MULTILINE,
    )
    while time.monotonic() < deadline:
        logs = run(
            compose_command(project, compose_files, "logs", "--no-color", "worker"),
            check=False,
        ).stdout
        match = pattern.search(logs)
        if match:
            return match.group(1), logs
        time.sleep(0.1)
    raise RuntimeError(f"did not observe fault hook for {phase}")


def wait_jobs(client, job_ids, timeout=45):
    deadline = time.monotonic() + timeout
    latest = {}
    while time.monotonic() < deadline:
        for job_id in job_ids:
            if latest.get(job_id, {}).get("status") in TERMINAL:
                continue
            latest[job_id] = client.get_job(job_id)
        if len(latest) == len(job_ids) and all(
            job.get("status") in TERMINAL for job in latest.values()
        ):
            return latest
        time.sleep(0.05)
    return latest


def run_kill_case(args, phase, case_number):
    started_at = utc_now()
    project = f"{args.project_prefix}-{case_number:02d}-{phase.replace('_', '-')}"
    compose_files = [args.compose_file, args.fault_compose_file]
    environment = os.environ.copy()
    environment["SINGLEFLIGHT_FAULT_PHASE"] = phase
    command = compose_command(project, compose_files)
    run([*command, "up", "-d", "--build", "--scale", "worker=4"], env=environment)
    client = ApiClient(args.base_url, 5)
    try:
        wait_health(client)
        before = client.get_metrics()
        prompt = f"fault-{phase}-{time.time_ns()}"
        submissions = submit_burst(client, prompt, "fake", args.requests)
        job_ids = [item["job_id"] for item in submissions if item["job_id"]]
        short_name, _logs = wait_for_fault_log(project, compose_files, phase)
        container = f"{project}-{short_name}"
        kill_started = time.monotonic()
        run(["docker", "kill", "--signal", "KILL", container])
        jobs = wait_jobs(client, job_ids, timeout=args.job_timeout)
        recovery_seconds = time.monotonic() - kill_started
        after = client.get_metrics()
        deltas = metric_delta(before, after)
        statuses = {}
        for job in jobs.values():
            statuses[job.get("status", "unknown")] = statuses.get(job.get("status", "unknown"), 0) + 1
        return {
            "case": phase,
            "started_at": started_at,
            "finished_at": utc_now(),
            "project": project,
            "killed_container": container,
            "request_count": args.requests,
            "terminal_job_count": sum(value for key, value in statuses.items() if key in TERMINAL),
            "job_statuses": statuses,
            "recovery_seconds": recovery_seconds,
            "provider_calls": deltas.get("provider_call_count", 0),
            "accepted_publications": deltas.get("flight_publications", 0),
            "leaders": deltas.get("flight_leaders_acquired", 0),
            "followers": deltas.get("flight_followers_joined", 0),
            "takeovers": deltas.get("flight_takeovers", 0),
            "timeouts": deltas.get("flight_timeouts", 0),
            "fencing_rejections": deltas.get("flight_fencing_rejections", 0),
            "metric_deltas": deltas,
            "jobs": {
                job_id: {
                    "status": job.get("status"),
                    "attempts": job.get("attempts"),
                    "cache": (job.get("result") or {}).get("cache"),
                    "error": job.get("error"),
                }
                for job_id, job in jobs.items()
            },
        }
    finally:
        run([*command, "down"], env=environment, check=False)


def run_stale_fencing_case(args, case_number):
    project = f"{args.project_prefix}-{case_number:02d}-stale-fencing"
    compose_files = [args.compose_file]
    command = compose_command(project, compose_files)
    run([*command, "up", "-d", "--build", "redis_clone", "api"])
    client = ApiClient(args.base_url, 5)
    try:
        wait_health(client)
        code = """
import json, time
from redis_clone.client import Client
c = Client('redis_clone', 31337)
first = c.flight_acquire('fault:stale', 'old-owner', 250)
time.sleep(0.3)
second = c.flight_acquire('fault:stale', 'new-owner', 2000)
old = c.flight_commit('fault:stale', 'old-owner', first.generation, 'semantic_cache:old', '{\"entry_id\":\"old\"}', 'semantic_cache:index', 10)
new = c.flight_commit('fault:stale', 'new-owner', second.generation, 'semantic_cache:new', '{\"entry_id\":\"new\"}', 'semantic_cache:index', 10)
status = c.flight_status('fault:stale')
print(json.dumps({'first_generation': first.generation, 'second_generation': second.generation, 'old_commit': old, 'new_commit': new, 'state': status.state, 'result_key': status.result_key}))
c.close()
"""
        result = run([*command, "exec", "-T", "api", "python", "-c", code])
        payload = json.loads(result.stdout.strip().splitlines()[-1])
        payload["case"] = "stale_fencing"
        payload["project"] = project
        return payload
    finally:
        run([*command, "down"], check=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--compose-file", type=Path, required=True)
    parser.add_argument("--fault-compose-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--base-url", default="http://127.0.0.1:18000")
    parser.add_argument("--project-prefix", default="mini-redis-fault-20261109")
    parser.add_argument("--requests", type=int, default=8)
    parser.add_argument("--job-timeout", type=float, default=45)
    args = parser.parse_args()

    cases = []
    for number, phase in enumerate(
        (
            "after_acquire_before_provider",
            "during_provider",
            "after_commit_before_finish",
        ),
        start=1,
    ):
        case = run_kill_case(args, phase, number)
        cases.append(case)
        print(json.dumps(case, indent=2), flush=True)
    fencing = run_stale_fencing_case(args, 4)
    cases.append(fencing)
    result = {
        "schema_version": 1,
        "generated_at": utc_now(),
        "environment": environment_metadata(),
        "cases": cases,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
