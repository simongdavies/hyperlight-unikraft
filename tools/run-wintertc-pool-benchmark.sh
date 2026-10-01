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

cargo run --release --locked --example workerd-demo -- \
    --executor "$artifact_dir/executor" \
    --rootfs "$artifact_dir/rootfs.img" \
    --bundle "$bundle" \
    --scratch-mb "$scratch_mib" \
    --bind "$bind" \
    --request-timeout-ms 30000 \
    --max-concurrent-sandboxes "$pool_size" \
    --queue-capacity "$queue_capacity" \
    >"$server_log" 2>&1 &
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
    stop.set()
    sampler.join()
    after = process_sample()
    if completed.returncode != 0:
        raise SystemExit(
            f"{label}: hey failed with {completed.returncode}: {completed.stderr}"
        )

    rows = list(csv.DictReader(completed.stdout.splitlines()))
    latencies_ms = [float(row["response-time"]) * 1000 for row in rows]
    errors = sum(int(row["status-code"]) != 200 for row in rows)
    peak_active = max((sample["pool"]["active"] for sample in samples), default=0)
    peak_queued = max((sample["pool"]["queued"] for sample in samples), default=0)
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
            "model": "fixed request workers with restore-on-acquisition",
            "queue_capacity": queue_capacity,
            "peak_active": peak_active,
            "peak_queued": peak_queued,
        },
        "process": {
            "cpu_seconds": cpu_seconds,
            "average_cpu_cores": cpu_seconds / elapsed,
            "peak_rss_bytes": peak_rss,
        },
        "sample_count": len(samples),
    }

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
while True:
    final_pool = get_json("/__hyperlight/pool-status")
    if final_pool["active"] == 0 and final_pool["queued"] == 0:
        break
    if time.monotonic() - refill_started > 30:
        raise SystemExit(f"pool did not refill: {final_pool}")
    time.sleep(0.01)
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
        "pool_model": "fixed request workers with restore-on-acquisition",
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
