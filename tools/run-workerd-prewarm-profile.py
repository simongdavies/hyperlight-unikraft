#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.

import argparse
import csv
import hashlib
import json
import math
import os
import shutil
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path


PROFILE_FIELDS = (
    "ready_wait_ms",
    "admission_wait_ms",
    "ready_owner_wait_ms",
    "replenishment_policy_wait_ms",
    "replenishment_wait_ms",
    "replenishment_restore_ms",
    "snapshot_restore_ms",
    "request_setup_ms",
    "guest_execution_ms",
    "response_finish_ms",
    "vm_teardown_ms",
    "total_ms",
)
PERF_EVENTS = (
    "task-clock",
    "cycles",
    "instructions",
    "context-switches",
    "cpu-migrations",
    "page-faults",
    "major-faults",
    "cache-references",
    "cache-misses",
    "dTLB-loads",
    "dTLB-load-misses",
    "iTLB-loads",
    "iTLB-load-misses",
)
EXPECTED_PAYLOAD_BYTES = 9441
EXPECTED_PAYLOAD_SHA256 = "8c8ed454c901390e5a5d53fa280afc878e16e0188821b439d891fdb1efe20a6c"


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact(path):
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
    }


def percentile(values, fraction):
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def distribution(values):
    if not values:
        return None
    return {
        "minimum": min(values),
        "average": sum(values) / len(values),
        "p50": percentile(values, 0.50),
        "p75": percentile(values, 0.75),
        "p90": percentile(values, 0.90),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "maximum": max(values),
    }


def get_json(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def get_payload(url):
    with urllib.request.urlopen(url, timeout=120) as response:
        return response.status, response.read()


def is_quiescent(status):
    if any(
        status.get(field, 0)
        for field in (
            "admitted",
            "active",
            "queued",
            "execution_slots_in_use",
            "restore_slots_in_use",
            "recycle_queue_depth",
            "teardown_in_flight",
            "completion_queue_depth",
            "completion_in_flight",
            "restore_permits_outstanding",
        )
    ):
        return False
    if status["restore_mode"] != "prewarmed":
        return True
    return (
        status["prewarmed_inventory"] == status["prewarmed_ready"]
        and status["prewarmed_ready"] >= status["warm_floor"]
        and status["prewarmed_replenishing"] == 0
        and not status["refill_active"]
        and not status["replenishment_paused"]
    )


def wait_quiescent(url, timeout=900):
    started = time.monotonic()
    while True:
        status = get_json(url)
        if is_quiescent(status):
            return status, time.monotonic() - started
        if time.monotonic() - started > timeout:
            raise RuntimeError(f"pool did not quiesce: {status}")
        time.sleep(0.01)


def process_sample(pid):
    stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()
    lines = Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines()
    values = {}
    for line in lines:
        if ":" in line:
            key, value = line.split(":", 1)
            values[key] = value.strip()
    ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    return {
        "cpu_seconds": (int(stat[13]) + int(stat[14])) / ticks,
        "rss_bytes": int(values["VmRSS"].split()[0]) * 1024,
        "peak_rss_bytes": int(values["VmHWM"].split()[0]) * 1024,
        "threads": int(values["Threads"]),
        "voluntary_context_switches": int(values["voluntary_ctxt_switches"]),
        "involuntary_context_switches": int(values["nonvoluntary_ctxt_switches"]),
        "minor_faults": int(stat[9]),
        "major_faults": int(stat[11]),
        "fd_count": len(list(Path(f"/proc/{pid}/fd").iterdir())),
    }


def host_sample():
    stat = Path("/proc/stat").read_text(encoding="utf-8").splitlines()[0].split()
    values = [int(value) for value in stat[1:]]
    pressure = {}
    for resource in ("cpu", "memory", "io"):
        path = Path("/proc/pressure") / resource
        pressure[resource] = path.read_text(encoding="utf-8").strip() if path.exists() else None
    load = Path("/proc/loadavg").read_text(encoding="utf-8").split()
    return {
        "cpu_ticks": {
            "total": sum(values[:8]),
            "idle": values[3] + values[4],
        },
        "load": {
            "one": float(load[0]),
            "five": float(load[1]),
            "fifteen": float(load[2]),
            "run_queue": load[3],
        },
        "pressure": pressure,
    }


def parse_profiles(path, offset, end):
    profiles = []
    with path.open(encoding="utf-8", errors="replace") as handle:
        handle.seek(offset)
        for line in handle.read(max(0, end - offset)).splitlines():
            if "workerd profile " not in line or ": {" not in line:
                continue
            try:
                profiles.append(json.loads("{" + line.split(": {", 1)[1]))
            except json.JSONDecodeError:
                pass
    return profiles


def start_profiler(command, stdout_path, stderr_path):
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        process = subprocess.Popen(command, stdout=stdout, stderr=stderr)
    return process


def stop_profiler(process):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=10)
    return process.returncode


def profiler_result(name, command, process, stdout_path, stderr_path, outputs):
    result = {
        "name": name,
        "command": command,
        "exit_code": process.returncode,
        "stdout": artifact(stdout_path),
        "stderr": artifact(stderr_path),
        "outputs": [],
    }
    for path in outputs:
        if path.exists():
            result["outputs"].append(artifact(path))
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--patch-sha256", required=True)
    parser.add_argument("--scratch-mib", type=int, default=384)
    parser.add_argument("--restore-mode", choices=("on-demand", "prewarmed"), required=True)
    parser.add_argument("--active", type=int, default=32)
    parser.add_argument("--owners", type=int)
    parser.add_argument("--restores", type=int, default=1)
    parser.add_argument("--warm-floor", type=int, default=1)
    parser.add_argument("--ready-low", type=int, default=16)
    parser.add_argument("--ready-high", type=int, default=32)
    parser.add_argument("--replenish-batch", type=int, default=2)
    parser.add_argument("--diagnostic-wave", type=int)
    parser.add_argument("--queue-capacity", type=int, default=256)
    parser.add_argument("--requests", type=int)
    parser.add_argument("--duration")
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--profile-log-every", type=int, default=1)
    parser.add_argument("--perf-stat", action="store_true")
    parser.add_argument("--deep-profile", action="store_true")
    parser.add_argument("--bind", default="127.0.0.1:8787")
    args = parser.parse_args()
    if (args.requests is None) == (args.duration is None):
        parser.error("exactly one of --requests or --duration is required")
    if args.restore_mode == "prewarmed" and args.owners is None:
        parser.error("--owners is required in prewarmed mode")
    if args.restore_mode == "on-demand" and args.owners is not None:
        parser.error("--owners is prewarmed-only")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    server_log = args.output_dir / "server.log"
    base_url = "http://" + args.bind
    status_url = base_url + "/__hyperlight/pool-status"
    sync_url = base_url + "/sync"
    command = [
        str(args.binary.resolve()),
        "--executor",
        str((args.artifact_dir / "executor").resolve()),
        "--rootfs",
        str((args.artifact_dir / "rootfs.img").resolve()),
        "--bundle",
        str(args.bundle.resolve()),
        "--scratch-mb",
        str(args.scratch_mib),
        "--bind",
        args.bind,
        "--request-timeout-ms",
        "30000",
        "--restore-mode",
        args.restore_mode,
        "--max-concurrent-sandboxes",
        str(args.active),
        "--queue-capacity",
        str(args.queue_capacity),
        "--profile-log-every",
        str(args.profile_log_every),
    ]
    if args.restore_mode == "prewarmed":
        command += [
            "--prewarmed-sandboxes",
            str(args.owners),
            "--max-concurrent-restores",
            str(args.restores),
            "--warm-floor",
            str(args.warm_floor),
            "--ready-low-watermark",
            str(args.ready_low),
            "--ready-high-watermark",
            str(args.ready_high),
            "--max-replenish-batch",
            str(args.replenish_batch),
        ]
        if args.diagnostic_wave:
            command += ["--diagnostic-no-refill-wave", str(args.diagnostic_wave)]

    with server_log.open("w", encoding="utf-8") as log:
        server = subprocess.Popen(command, stdout=log, stderr=log, text=True)
    report = None
    try:
        started = time.monotonic()
        while True:
            if server.poll() is not None:
                raise RuntimeError(server_log.read_text(encoding="utf-8"))
            try:
                initial_status, startup_seconds = wait_quiescent(status_url, timeout=1)
                break
            except (OSError, urllib.error.URLError, RuntimeError):
                if time.monotonic() - started > 900:
                    raise RuntimeError("server did not become ready")
                time.sleep(0.25)
        log_offset = server_log.stat().st_size
        before_process = process_sample(server.pid)
        before_host = host_sample()
        samples = []
        sample_errors = []
        stop = threading.Event()
        run_started = time.monotonic()

        def sample():
            while not stop.wait(0.01):
                row = {"offset_seconds": time.monotonic() - run_started}
                try:
                    row["process"] = process_sample(server.pid)
                    row["host"] = host_sample()
                    row["pool"] = get_json(status_url)
                    samples.append(row)
                except Exception as error:
                    sample_errors.append(repr(error))

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        profilers = []
        if args.perf_stat and shutil.which("perf"):
            output = args.output_dir / "perf-stat.csv"
            stdout = args.output_dir / "perf-stat.stdout"
            stderr = args.output_dir / "perf-stat.stderr"
            perf_command = [
                "perf",
                "stat",
                "-x",
                ",",
                "-e",
                ",".join(PERF_EVENTS),
                "-p",
                str(server.pid),
                "-o",
                str(output),
            ]
            profilers.append(
                ("perf-stat", perf_command, start_profiler(perf_command, stdout, stderr), stdout, stderr, [output])
            )
        if args.deep_profile and shutil.which("perf"):
            for name, event_args, output_name in (
                ("perf-cpu", ["-F", "99"], "perf-cpu.data"),
                ("perf-offcpu", ["-e", "sched:sched_switch"], "perf-offcpu.data"),
            ):
                output = args.output_dir / output_name
                stdout = args.output_dir / f"{name}.stdout"
                stderr = args.output_dir / f"{name}.stderr"
                perf_command = [
                    "perf",
                    "record",
                    *event_args,
                    "-g",
                    "-p",
                    str(server.pid),
                    "-o",
                    str(output),
                ]
                profilers.append(
                    (name, perf_command, start_profiler(perf_command, stdout, stderr), stdout, stderr, [output])
                )
            stdout = args.output_dir / "perf-trace.stdout"
            stderr = args.output_dir / "perf-trace.stderr"
            perf_command = [
                "perf",
                "trace",
                "-s",
                "-p",
                str(server.pid),
                "-e",
                "ioctl,mmap,munmap,madvise,futex",
            ]
            profilers.append(
                (
                    "perf-trace",
                    perf_command,
                    start_profiler(perf_command, stdout, stderr),
                    stdout,
                    stderr,
                    [],
                )
            )

        hey_args = ["hey", "-c", str(args.concurrency), "-o", "csv"]
        if args.requests is not None:
            hey_args += ["-n", str(args.requests)]
        else:
            hey_args += ["-z", args.duration]
        completed = subprocess.run(
            [*hey_args, sync_url], text=True, capture_output=True, check=False
        )
        run_ended = time.monotonic()
        for _, _, process, _, _, _ in profilers:
            stop_profiler(process)
        stop.set()
        sampler.join()
        csv_path = args.output_dir / "requests.csv"
        csv_path.write_text(completed.stdout, encoding="utf-8", newline="\n")
        if completed.returncode != 0:
            raise RuntimeError(f"hey failed: {completed.stderr}")
        final_status, quiescence_seconds = wait_quiescent(status_url)
        after_process = process_sample(server.pid)
        after_host = host_sample()
        measurement_log_end = server_log.stat().st_size
        recovery_status, recovery_payload = get_payload(sync_url)
        recovery_sha256 = hashlib.sha256(recovery_payload).hexdigest()
        if (
            recovery_status != 200
            or len(recovery_payload) != EXPECTED_PAYLOAD_BYTES
            or recovery_sha256 != EXPECTED_PAYLOAD_SHA256
        ):
            raise RuntimeError("post-run identity mismatch")
        recovery_pool_status, recovery_quiescence_seconds = wait_quiescent(status_url)
        rows = list(csv.DictReader(completed.stdout.splitlines()))
        latencies = [float(row["response-time"]) * 1000 for row in rows]
        statuses = Counter(row["status-code"] for row in rows)
        profiles = parse_profiles(server_log, log_offset, measurement_log_end)
        timeseries = args.output_dir / "timeseries.jsonl"
        with timeseries.open("w", encoding="utf-8", newline="\n") as handle:
            for row in samples:
                handle.write(json.dumps(row, sort_keys=True) + "\n")
        elapsed = run_ended - run_started
        report = {
            "schema_version": 1,
            "configuration": {
                "command": command,
                "source_commit": args.source_commit,
                "patch_sha256": args.patch_sha256,
                "runner_sha256": sha256(Path(__file__)),
                "binary": artifact(args.binary.resolve()),
                "executor": artifact(args.artifact_dir / "executor"),
                "rootfs": artifact(args.artifact_dir / "rootfs.img"),
                "bundle": artifact(args.bundle),
                "scratch_mib": args.scratch_mib,
                "load_command": [*hey_args, sync_url],
                "payload_bytes": len(recovery_payload),
                "payload_sha256": recovery_sha256,
            },
            "startup_seconds": startup_seconds,
            "initial_status": initial_status,
            "final_status": final_status,
            "requests": len(rows),
            "errors": sum(count for code, count in statuses.items() if code != "200"),
            "status_codes": dict(sorted(statuses.items())),
            "wall_seconds": elapsed,
            "throughput_requests_per_second": len(rows) / elapsed,
            "latency_ms": distribution(latencies),
            "phase_ms": {
                field: distribution([profile[field] for profile in profiles])
                for field in PROFILE_FIELDS
            },
            "profile_count": len(profiles),
            "quiescence_seconds": quiescence_seconds,
            "recovery_quiescence_seconds": recovery_quiescence_seconds,
            "recovery_pool_status": recovery_pool_status,
            "process": {
                "before": before_process,
                "after": after_process,
                "cpu_seconds": after_process["cpu_seconds"] - before_process["cpu_seconds"],
                "average_cpu_cores": (
                    after_process["cpu_seconds"] - before_process["cpu_seconds"]
                )
                / elapsed,
            },
            "host": {"before": before_host, "after": after_host},
            "sampling": {
                "interval_ms": 10,
                "samples": len(samples),
                "errors": sample_errors,
                "timeseries": artifact(timeseries),
            },
            "profilers": [
                profiler_result(name, profiler_command, process, stdout, stderr, outputs)
                for name, profiler_command, process, stdout, stderr, outputs in profilers
            ],
            "raw_requests": artifact(csv_path),
        }
        report["profiler_limitations"] = [
            {
                "name": profiler["name"],
                "exit_code": profiler["exit_code"],
                "stderr": Path(profiler["stderr"]["path"]).read_text(
                    encoding="utf-8", errors="replace"
                )[:4000],
            }
            for profiler in report["profilers"]
            if profiler["exit_code"] not in (0, -signal.SIGINT, 128 + signal.SIGINT)
        ]
        if (args.perf_stat or args.deep_profile) and not shutil.which("perf"):
            report["profiler_limitations"].append(
                {"name": "perf", "exit_code": None, "stderr": "perf is not installed"}
            )
    finally:
        if server.poll() is None:
            server.send_signal(signal.SIGTERM)
            try:
                server.wait(timeout=60)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
    if report is None:
        raise RuntimeError("run did not produce a report")
    report["shutdown"] = {
        "exit_code": server.returncode,
        "process_gone": not Path(f"/proc/{server.pid}").exists(),
    }
    report["server_log"] = artifact(server_log)
    output = args.output_dir / "result.json"
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    if report["errors"] != 0 or report["status_codes"] != {"200": report["requests"]}:
        raise SystemExit("request correctness failed")


if __name__ == "__main__":
    main()
