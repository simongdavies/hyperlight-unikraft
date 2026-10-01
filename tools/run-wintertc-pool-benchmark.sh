#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Hyperlight Authors.
set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
    echo "usage: $0 ARTIFACT_DIR OUTPUT_DIR [SCRATCH_MIB] [POOL_SIZE]" >&2
    exit 2
fi

artifact_dir="$(realpath "$1")"
output_dir="$(realpath -m "$2")"
scratch_mib="${3:-344}"
pool_size="${4:-32}"
queue_capacity="${WINTERTC_POOL_QUEUE_CAPACITY:-256}"
restore_mode="${WINTERTC_POOL_RESTORE_MODE:-on-demand}"
prewarmed_sandboxes="${WINTERTC_POOL_PREWARMED_SANDBOXES:-$pool_size}"
max_concurrent_restores="${WINTERTC_POOL_MAX_CONCURRENT_RESTORES:-1}"
warm_floor="${WINTERTC_POOL_WARM_FLOOR:-1}"
ready_low_watermark="${WINTERTC_POOL_READY_LOW_WATERMARK:-$((pool_size / 2))}"
ready_high_watermark="${WINTERTC_POOL_READY_HIGH_WATERMARK:-$pool_size}"
max_replenish_batch="${WINTERTC_POOL_MAX_REPLENISH_BATCH:-2}"
diagnostic_no_refill_wave="${WINTERTC_POOL_DIAGNOSTIC_NO_REFILL_WAVE:-}"
profile_log_every="${WINTERTC_POOL_PROFILE_LOG_EVERY:-64}"
bind="${WINTERTC_POOL_BIND:-127.0.0.1:8787}"
base_url="http://$bind"
bundle="examples/workerd-bundles/workerd-pool-benchmark.json"
server_log="$output_dir/workerd-pool-server.log"
result="$output_dir/wintertc-pool-performance.json"

for command in cargo curl hey python3 realpath; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
test -f "$artifact_dir/executor"
test -f "$artifact_dir/rootfs.img"
test -f "$bundle"
mkdir -p "$output_dir"

server_args=(
    --executor "$artifact_dir/executor"
    --rootfs "$artifact_dir/rootfs.img"
    --bundle "$bundle"
    --scratch-mb "$scratch_mib"
    --bind "$bind"
    --request-timeout-ms 30000
    --restore-mode "$restore_mode"
    --max-concurrent-sandboxes "$pool_size"
    --queue-capacity "$queue_capacity"
    --profile-log-every "$profile_log_every"
)
if [[ "$restore_mode" == "prewarmed" ]]; then
    server_args+=(
        --prewarmed-sandboxes "$prewarmed_sandboxes"
        --max-concurrent-restores "$max_concurrent_restores"
        --warm-floor "$warm_floor"
        --ready-low-watermark "$ready_low_watermark"
        --ready-high-watermark "$ready_high_watermark"
        --max-replenish-batch "$max_replenish_batch"
    )
    if [[ -n "$diagnostic_no_refill_wave" ]]; then
        server_args+=(--diagnostic-no-refill-wave "$diagnostic_no_refill_wave")
    fi
fi

cargo run --release --locked --example workerd-demo -- \
    "${server_args[@]}" >"$server_log" 2>&1 &
server_pid=$!
cleanup() {
    if kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid"
        wait "$server_pid" || true
    fi
}
trap cleanup EXIT INT TERM

ready=false
for _ in $(seq 1 600); do
    if curl --silent --show-error --fail "$base_url/__hyperlight/pool-status" \
        >/dev/null 2>&1; then
        ready=true
        break
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
        cat "$server_log" >&2
        exit 1
    fi
    sleep 1
done
test "$ready" = true

python3 - \
    "$base_url" \
    "$server_pid" \
    "$pool_size" \
    "$queue_capacity" \
    "$scratch_mib" \
    "$bundle" \
    "$result" <<'PY'
import csv
import hashlib
import json
import math
import os
import subprocess
import sys
import threading
import time
import urllib.request

(
    base_url,
    server_pid_text,
    pool_size_text,
    queue_capacity_text,
    scratch_mib_text,
    bundle,
    output_path,
) = sys.argv[1:]
server_pid = int(server_pid_text)
pool_size = int(pool_size_text)
queue_capacity = int(queue_capacity_text)
scratch_mib = int(scratch_mib_text)
clock_ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])

BASELINE = {
    "command": "hey -n 320 -c 32 http://127.0.0.1:8787/sync",
    "requests": 320,
    "concurrency": 32,
    "payload_bytes": 9441,
    "throughput_requests_per_second": 291.3588,
    "p50_ms": 107.3,
    "p95_ms": 121.3,
    "p99_ms": 135.4,
    "errors": 0,
}
STRETCH = {
    "throughput_requests_per_second": 500.0,
    "p95_ms": 100.0,
    "p99_ms": 120.0,
}

def get_json(path):
    with urllib.request.urlopen(base_url + path, timeout=5) as response:
        return json.load(response)

def get_sync():
    with urllib.request.urlopen(base_url + "/sync", timeout=30) as response:
        return response.status, response.read()

def process_sample():
    with open(f"/proc/{server_pid}/stat", encoding="utf-8") as handle:
        fields = handle.read().split()
    with open(f"/proc/{server_pid}/status", encoding="utf-8") as handle:
        status = handle.read().splitlines()
    rss_kib = int(next(line.split()[1] for line in status if line.startswith("VmRSS:")))
    cpu_seconds = (int(fields[13]) + int(fields[14])) / clock_ticks
    return {"cpu_seconds": cpu_seconds, "rss_bytes": rss_kib * 1024}

def is_quiescent(pool):
    if any(
        pool[field] != 0
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
        )
    ):
        return False
    if pool["restore_mode"] != "prewarmed":
        return True
    return (
        pool["prewarmed_inventory"] == pool["prewarmed_ready"]
        and pool["prewarmed_ready"] >= pool["warm_floor"]
        and pool["prewarmed_replenishing"] == 0
        and pool["restore_permits_outstanding"] == 0
        and not pool["refill_active"]
        and not pool["replenishment_paused"]
    )

def wait_for_quiescence(label, timeout=120):
    started = time.monotonic()
    while True:
        pool = get_json("/__hyperlight/pool-status")
        if is_quiescent(pool):
            return pool
        if time.monotonic() - started > timeout:
            raise SystemExit(f"{label}: pool did not become quiescent: {pool}")
        time.sleep(0.01)

def percentile(values, fraction):
    if not values:
        return None
    index = max(0, math.ceil(fraction * len(values)) - 1)
    return sorted(values)[index]

def run_hey(label, args):
    stop = threading.Event()
    samples = []
    def sample():
        while not stop.wait(0.01):
            try:
                samples.append(
                    {
                        "offset_seconds": time.monotonic() - started,
                        "pool": get_json("/__hyperlight/pool-status"),
                        "process": process_sample(),
                    }
                )
            except Exception:
                pass

    before_pool = wait_for_quiescence(f"{label} pre-run")
    before = process_sample()
    started = time.monotonic()
    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    completed = subprocess.run(
        ["hey", *args, "-o", "csv", base_url + "/sync"],
        check=False,
        text=True,
        capture_output=True,
    )
    elapsed = time.monotonic() - started
    if completed.returncode != 0:
        stop.set()
        sampler.join()
        raise SystemExit(
            f"{label}: hey failed with {completed.returncode}: {completed.stderr}"
        )
    after_pool = wait_for_quiescence(f"{label} post-run")
    stop.set()
    sampler.join()
    after = process_sample()

    rows = list(csv.DictReader(completed.stdout.splitlines()))
    latencies_ms = [float(row["response-time"]) * 1000 for row in rows]
    errors = sum(int(row["status-code"]) != 200 for row in rows)
    pool_samples = [before_pool] + [sample["pool"] for sample in samples] + [after_pool]
    peak_active = max(sample["active"] for sample in pool_samples)
    peak_admitted = max(sample["admitted"] for sample in pool_samples)
    peak_queued = max(sample["queued"] for sample in pool_samples)
    peak_execution_slots = max(
        sample["execution_slots_in_use"] for sample in pool_samples
    )
    peak_restore_slots = max(sample["restore_slots_in_use"] for sample in pool_samples)
    peak_completion_in_flight = max(
        sample["completion_in_flight"] for sample in pool_samples
    )
    peak_completion_queue = max(
        sample["completion_queue_depth"] for sample in pool_samples
    )
    peak_recycle_queue = max(
        sample["recycle_queue_depth"] for sample in pool_samples
    )
    peak_teardown_in_flight = max(
        sample["teardown_in_flight"] for sample in pool_samples
    )
    minimum_ready = min(sample["prewarmed_ready"] for sample in pool_samples)
    peak_replenishing = max(
        sample["prewarmed_replenishing"] for sample in pool_samples
    )
    restore_attempts = (
        after_pool["restore_attempts"] - before_pool["restore_attempts"]
    )
    completed_restores = (
        after_pool["completed_restores"] - before_pool["completed_restores"]
    )
    failed_restores = (
        after_pool["failed_restores"] - before_pool["failed_restores"]
    )
    completed_policy_waits = (
        after_pool["completed_replenishment_policy_waits"]
        - before_pool["completed_replenishment_policy_waits"]
    )
    policy_wait_total_ms = (
        after_pool["replenishment_policy_wait_total_ms"]
        - before_pool["replenishment_policy_wait_total_ms"]
    )
    restore_wait_total_ms = (
        after_pool["restore_wait_total_ms"] - before_pool["restore_wait_total_ms"]
    )
    restore_total_ms = (
        after_pool["restore_total_ms"] - before_pool["restore_total_ms"]
    )
    completed_completions = (
        after_pool["completed_completions"] - before_pool["completed_completions"]
    )
    completion_total_ms = (
        after_pool["completion_total_ms"] - before_pool["completion_total_ms"]
    )
    completed_teardowns = (
        after_pool["completed_teardowns"] - before_pool["completed_teardowns"]
    )
    teardown_total_ms = (
        after_pool["teardown_total_ms"] - before_pool["teardown_total_ms"]
    )
    peak_rss = max(
        [before["rss_bytes"], after["rss_bytes"]]
        + [sample["process"]["rss_bytes"] for sample in samples]
    )
    cpu_seconds = max(0.0, after["cpu_seconds"] - before["cpu_seconds"])
    return {
        "label": label,
        "command": "hey " + " ".join(args) + " -o csv " + base_url + "/sync",
        "requests": len(rows),
        "errors": errors,
        "wall_seconds": elapsed,
        "throughput_requests_per_second": len(rows) / elapsed,
        "latency_ms": {
            "average": sum(latencies_ms) / len(latencies_ms),
            "minimum": min(latencies_ms),
            "maximum": max(latencies_ms),
            "p50": percentile(latencies_ms, 0.50),
            "p95": percentile(latencies_ms, 0.95),
            "p99": percentile(latencies_ms, 0.99),
        },
        "pool": {
            "configured_pool_size": pool_size,
            "model": (
                "central ready-owner mailbox dispatch with bounded recycle"
                if before_pool["restore_mode"] == "prewarmed"
                else "fixed owner restore-on-acquisition"
            ),
            "queue_capacity": queue_capacity,
            "peak_admitted": peak_admitted,
            "peak_active": peak_active,
            "peak_queued": peak_queued,
            "peak_execution_slots_in_use": peak_execution_slots,
            "peak_restore_slots_in_use": peak_restore_slots,
            "peak_completion_in_flight": peak_completion_in_flight,
            "peak_completion_queue_depth": peak_completion_queue,
            "peak_recycle_queue_depth": peak_recycle_queue,
            "peak_teardown_in_flight": peak_teardown_in_flight,
            "minimum_prewarmed_ready": minimum_ready,
            "peak_prewarmed_replenishing": peak_replenishing,
            "restore_attempts": restore_attempts,
            "completed_restores": completed_restores,
            "failed_restores": failed_restores,
            "completed_replenishment_policy_waits": completed_policy_waits,
            "replenishment_policy_wait_total_ms": policy_wait_total_ms,
            "replenishment_policy_wait_average_ms": (
                policy_wait_total_ms / completed_policy_waits
                if completed_policy_waits
                else None
            ),
            "restore_wait_total_ms": restore_wait_total_ms,
            "restore_wait_average_ms": (
                restore_wait_total_ms / restore_attempts
                if restore_attempts
                else None
            ),
            "restore_total_ms": restore_total_ms,
            "restore_average_ms": (
                restore_total_ms / restore_attempts if restore_attempts else None
            ),
            "completed_completions": completed_completions,
            "completion_total_ms": completion_total_ms,
            "completion_average_ms": (
                completion_total_ms / completed_completions
                if completed_completions
                else None
            ),
            "completed_teardowns": completed_teardowns,
            "teardown_total_ms": teardown_total_ms,
            "teardown_average_ms": (
                teardown_total_ms / completed_teardowns
                if completed_teardowns
                else None
            ),
            "status_before": before_pool,
            "status_after": after_pool,
        },
        "process": {
            "cpu_seconds": cpu_seconds,
            "average_cpu_cores": cpu_seconds / elapsed,
            "peak_rss_bytes": peak_rss,
        },
        "sample_count": len(samples),
    }

wait_for_quiescence("initial prewarm")
status, payload = get_sync()
if status != 200 or len(payload) != BASELINE["payload_bytes"]:
    raise SystemExit(
        f"/sync mismatch: status={status} bytes={len(payload)} "
        f"expected={BASELINE['payload_bytes']}"
    )

baseline_run = run_hey("baseline-n320-c32", ["-n", "320", "-c", "32"])
latency = baseline_run["latency_ms"]
baseline_pass = (
    baseline_run["errors"] == 0
    and baseline_run["throughput_requests_per_second"]
    > BASELINE["throughput_requests_per_second"]
    and latency["p50"] < BASELINE["p50_ms"]
    and latency["p95"] < BASELINE["p95_ms"]
    and latency["p99"] < BASELINE["p99_ms"]
)
stretch_pass = (
    baseline_run["errors"] == 0
    and baseline_run["throughput_requests_per_second"]
    >= STRETCH["throughput_requests_per_second"]
    and latency["p95"] <= STRETCH["p95_ms"]
    and latency["p99"] <= STRETCH["p99_ms"]
)

sustained = []
for concurrency in (32, 64, 128):
    sustained.append(
        run_hey(
            f"sustained-60s-c{concurrency}",
            ["-z", "60s", "-c", str(concurrency)],
        )
    )

refill_started = time.monotonic()
final_pool = wait_for_quiescence("final refill")
refill_seconds = time.monotonic() - refill_started
recovery_status, recovery_payload = get_sync()

report = {
    "schema_version": 1,
    "accepted": baseline_pass,
    "stretch_target_met": stretch_pass,
    "baseline": BASELINE,
    "stretch_target": STRETCH,
    "configuration": {
        "base_url": base_url,
        "bundle": bundle,
        "bundle_sha256": hashlib.sha256(open(bundle, "rb").read()).hexdigest(),
        "scratch_mib": scratch_mib,
        "pool_size": pool_size,
        "pool_model": baseline_run["pool"]["model"],
        "restore_mode": baseline_run["pool"]["status_before"]["restore_mode"],
        "owner_count": baseline_run["pool"]["status_before"]["owner_count"],
        "effective_concurrency": baseline_run["pool"]["status_before"][
            "effective_concurrency"
        ],
        "prewarmed_sandboxes": baseline_run["pool"]["status_before"][
            "prewarmed_sandboxes"
        ],
        "max_concurrent_restores": baseline_run["pool"]["status_before"][
            "max_concurrent_restores"
        ],
        "warm_floor": baseline_run["pool"]["status_before"]["warm_floor"],
        "ready_low_watermark": baseline_run["pool"]["status_before"][
            "ready_low_watermark"
        ],
        "ready_high_watermark": baseline_run["pool"]["status_before"][
            "ready_high_watermark"
        ],
        "max_replenish_batch": baseline_run["pool"]["status_before"][
            "max_replenish_batch"
        ],
        "profile_log_every": baseline_run["pool"]["status_before"][
            "profile_log_every"
        ],
        "queue_capacity": queue_capacity,
        "payload_bytes": len(payload),
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
    },
    "baseline_run": baseline_run,
    "sustained_runs": sustained,
    "refill": {
        "seconds": refill_seconds,
        "final_pool": final_pool,
        "recovery_status": recovery_status,
        "recovery_payload_bytes": len(recovery_payload),
        "passed": (
            recovery_status == 200
            and len(recovery_payload) == BASELINE["payload_bytes"]
        ),
    },
}
with open(output_path, "w", encoding="utf-8") as handle:
    json.dump(report, handle, indent=2, sort_keys=True)
    handle.write("\n")
print(json.dumps(report, indent=2, sort_keys=True))
if not baseline_pass:
    raise SystemExit("performance FAIL: exact baseline was not beaten")
if not report["refill"]["passed"]:
    raise SystemExit("performance FAIL: pool refill/recovery failed")
if any(run["errors"] != 0 for run in sustained):
    raise SystemExit("performance FAIL: sustained run reported errors")
PY

sha256sum "$bundle" "$result" "$server_log" \
    >"$output_dir/wintertc-pool-artifact-sha256.txt"
