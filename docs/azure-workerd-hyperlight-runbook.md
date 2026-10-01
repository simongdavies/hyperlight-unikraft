# Run Workerd in Hyperlight-Unikraft on Azure KVM

This walkthrough starts from a clean Azure Linux VM, validates native KVM,
builds and packages a Workerd executor, runs the HTTP demo, exercises
the runnable WinterTC routes, and compares on-demand and adaptive prewarmed
execution.

The commands use 384 MiB of guest scratch memory.

## 1. VM prerequisites

- Existing Ubuntu 24.04 x86-64 Azure VM.
- `Standard_D32s_v5` or equivalent nested-virtualization-capable size.
- 32 vCPUs and approximately 128 GiB RAM for the concurrency examples.
- Nested virtualization with `/dev/kvm`.
- Sufficient ext4 disk space for Rust, Docker, Bazel, Workerd, and build
  outputs.
- Outbound access to package repositories and GitHub.
- SSH access to the VM.
- Access to the selected demo/benchmark port when the client is remote.

## 2. Clone the branch with submodules

After connecting to the VM:

```bash
mkdir -p "$HOME/src" "$HOME/results"
cd "$HOME/src"
git clone --branch simongdavies-adaptive-prewarm-profiling --recurse-submodules https://github.com/simongdavies/hyperlight-unikraft.git && cd hyperlight-unikraft
git status --short
git rev-parse --short HEAD
```

Expected: the checkout is on
`simongdavies-adaptive-prewarm-profiling`, all submodules are initialized, and
`git status --short` prints nothing.

## 3. Validate the host and install prerequisites

Run on the Azure VM:

```bash
uname -a
lscpu
findmnt -no FSTYPE,TARGET /
free -h
df -h /

grep -E -m1 '(^| )vmx( |$)' /proc/cpuinfo
ls -l /dev/kvm

if id -nG "$USER" | tr ' ' '\n' | grep -qx kvm; then
  echo "Current user is in the kvm group."
else
  sudo usermod -aG kvm "$USER"
  echo "KVM group membership added."
  echo "Disconnect and reconnect SSH, then continue with the next block."
fi
```

Stop at this point if the command added group membership. After reconnecting,
run the access and ioctl checks:

```bash
test -c /dev/kvm && echo "/dev/kvm exists"
test -r /dev/kvm && echo "/dev/kvm is readable"
test -w /dev/kvm && echo "/dev/kvm is writable"

python3 - <<'PY'
import fcntl
import os

fd = os.open("/dev/kvm", os.O_RDWR | os.O_CLOEXEC)
try:
    version = fcntl.ioctl(fd, 0xAE00, 0)  # KVM_GET_API_VERSION
    assert version == 12, version
    print({"kvm_api_version": version})
finally:
    os.close(fd)
PY
```

Expected: an ext4 root filesystem, 32 CPUs for `Standard_D32s_v5`, readable
and writable `/dev/kvm`, and `{"kvm_api_version": 12}`.

Install host dependencies:

```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential \
  cpio \
  curl \
  binutils \
  file \
  git \
  jq \
  patch \
  pkg-config \
  python3 \
  python3-venv \
  rsync \
  unzip

if ! command -v docker >/dev/null; then
  sudo apt-get install -y docker.io
fi
sudo usermod -aG docker "$(whoami)"
```

Reconnect after adding the Docker group, then verify:

```bash
docker version
docker run --rm hello-world
```

Install Rust and `just`:

```bash
export HOME="${HOME:-/home/$(whoami)}"
export RUSTUP_HOME="$HOME/.rustup"
export CARGO_HOME="$HOME/.cargo"
mkdir -p "$RUSTUP_HOME" "$CARGO_HOME"
export PATH="$CARGO_HOME/bin:$PATH"

if ! command -v rustup >/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | env RUSTUP_INIT_SKIP_PATH_CHECK=yes sh -s -- -y --profile minimal
fi

if [[ -f "$CARGO_HOME/env" ]]; then
  source "$CARGO_HOME/env"
fi
rustup toolchain install 1.98.0 --profile minimal
rustup show

if ! command -v just >/dev/null; then
  cargo install just
fi
```

Install the load generator:

```bash
sudo apt-get install -y golang-go
go install github.com/rakyll/hey@v0.1.4
export PATH="$HOME/go/bin:$CARGO_HOME/bin:$PATH"
hey -version
```

## 4. Build the Workerd executor

Clone the executor integration branch:

```bash
cd "$HOME/src"
git clone --branch simongdavies-fix-workerd-dev-container --single-branch --recurse-submodules https://github.com/simongdavies/workerd.git
cd workerd
git rev-parse HEAD
```

The current integration commit is
`627b7bcc8f3cb79bccbb53c8d3f60e5b95af4f6e`.
Its `.bazelrc` selects the static host-tool C++ runtime with
`--host_linkopt='-l:libc++.a'`.

Build with a container that provides LLVM 22 and Bazelisk:

```bash
cd "$HOME/src/workerd"
test -f MODULE.bazel

cat > .devcontainer/Dockerfile.hyperlight-executor <<'EOF'
FROM mcr.microsoft.com/vscode/devcontainers/javascript-node:26-bookworm
ARG LLVM_VERSION=22
RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
       curl \
       gnupg \
       lsb-release \
       software-properties-common \
       tcl \
    && curl -fSsL -o /tmp/llvm.sh https://apt.llvm.org/llvm.sh \
    && bash /tmp/llvm.sh ${LLVM_VERSION} \
    && apt-get install -y --no-install-recommends \
       libunwind-${LLVM_VERSION}-dev \
       libc++-${LLVM_VERSION}-dev \
       libc++abi-${LLVM_VERSION}-dev \
       libclang-rt-${LLVM_VERSION}-dev \
       -o DPkg::options::="--force-overwrite" \
    && npm install -g @bazel/bazelisk \
    && rm -rf /var/lib/apt/lists/* /tmp/llvm.sh
ENV PATH="/usr/lib/llvm-${LLVM_VERSION}/bin:${PATH}"
EOF

docker build \
  -t workerd-hyperlight-builder \
  -f .devcontainer/Dockerfile.hyperlight-executor \
  .devcontainer

mkdir -p "$HOME/.cache/workerd-bazel/action-cache"
mkdir -p "$HOME/.cache/workerd-bazel/repository-cache"
mkdir -p "$HOME/artifacts"

docker run --rm \
  --mount type=bind,src="$HOME/src/workerd",dst=/workspace \
  -v "$HOME/.cache/workerd-bazel:/root/.cache/bazel" \
  -v "$HOME/artifacts:/artifacts" \
  -w /workspace \
  workerd-hyperlight-builder \
  bash -lc '
    bazel --output_base=/root/.cache/bazel/workerd-hyperlight-output \
      build //src/workerd/server:workerd-sandbox-executor \
      --config=opt \
      --strip=always \
      --//:io_backend=cxx \
      --jobs="${WORKERD_BAZEL_JOBS:-$(nproc)}" \
      --disk_cache=/root/.cache/bazel/action-cache \
      --repository_cache=/root/.cache/bazel/repository-cache \
      --announce_rc

    executor=bazel-bin/src/workerd/server/workerd-sandbox-executor
    "$executor" --self-test
    /usr/lib/llvm-22/bin/llvm-strip "$executor"
    cp "$executor" /artifacts/workerd-sandbox-executor
    chmod 0755 /artifacts/workerd-sandbox-executor
  '

export WORKERD_EXECUTOR="$HOME/artifacts/workerd-sandbox-executor"
```

Validate the executor:

```bash
"$WORKERD_EXECUTOR" --self-test
file "$WORKERD_EXECUTOR"
readelf -n "$WORKERD_EXECUTOR" | sed -n '/Build ID/p'
readelf -l "$WORKERD_EXECUTOR"
readelf -d "$WORKERD_EXECUTOR" || true
sha256sum "$WORKERD_EXECUTOR"

file "$WORKERD_EXECUTOR" | grep -q 'ELF 64-bit.*x86-64'
file "$WORKERD_EXECUTOR" | grep -qi 'pie executable'
! readelf -l "$WORKERD_EXECUTOR" | grep -q 'INTERP'
! readelf -d "$WORKERD_EXECUTOR" | grep -Eq '(NEEDED|RPATH|RUNPATH)'
```

Expected: executor self-test passes; the artifact is a stripped x86-64 static
PIE with a GNU Build ID and no interpreter or dynamic dependencies.

## 5. Build Hyperlight-Unikraft and package the rootfs

```bash
cd "$HOME/src/hyperlight-unikraft"
export RUSTUP_HOME="$HOME/.rustup"
export CARGO_HOME="$HOME/.cargo"
mkdir -p "$RUSTUP_HOME" "$CARGO_HOME"
export PATH="$CARGO_HOME/bin:$PATH"
if [[ -f "$CARGO_HOME/env" ]]; then
  source "$CARGO_HOME/env"
fi

just build-workerd-kernel

bash examples/workerd-executor/build-rootfs.sh \
  "$WORKERD_EXECUTOR"

cargo build --release --locked --example workerd-demo

sha256sum \
  kernel/workerd_hyperlight-x86_64 \
  build-elfloader/workerd-executor/executor \
  build-elfloader/workerd-executor/rootfs.img \
  target/release/examples/workerd-demo
```

For a static PIE, `executor` and `rootfs.img` are byte-identical. The packaging
script requires an executable x86-64 PIE and records the dependency closure.

Run focused scheduler and mock-fixture tests:

```bash
just guests
cargo test --locked --lib workerd::pool::tests
cargo test --locked --example workerd-demo
```

Expected: all tests pass.

## 6. Launch the demo and verify basic behavior

Start the on-demand server:

```bash
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/on-demand"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/acceptance.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 500 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 2048 \
  --profile-log-every 1 \
  >"$HOME/results/on-demand/server.log" 2>&1 &

SERVER_PID=$!
echo "$SERVER_PID" > "$HOME/results/on-demand/server.pid"

for _ in $(seq 1 600); do
  curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status \
    >"$HOME/results/on-demand/startup-status.json" && break
  kill -0 "$SERVER_PID"
  sleep 1
done

jq . "$HOME/results/on-demand/startup-status.json"
```

Expected: `restore_mode` is `on-demand`, the server process remains alive, and
the status endpoint returns JSON.

Send a normal request:

```bash
curl --fail-with-body -sS \
  -X POST \
  -H 'x-demo: azure' \
  -d 'hello' \
  http://127.0.0.1:8787/hello | jq .
```

Expected:

```json
{"path":"/hello","method":"POST"}
```

Prove timeout recovery:

```bash
curl -i -X POST -d busy http://127.0.0.1:8787/busy
curl --fail-with-body -sS \
  -X POST \
  -d after \
  http://127.0.0.1:8787/after | jq .
```

Expected: `/busy` returns HTTP 504. The following `/after` request returns
HTTP 200 and `{"path":"/after","method":"POST"}`.

Test bounded overload by restarting with one active slot and one queued slot:

```bash
kill -TERM "$SERVER_PID"
wait "$SERVER_PID"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/acceptance.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 500 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 1 \
  --queue-capacity 1 \
  >"$HOME/results/on-demand/overload-server.log" 2>&1 &
SERVER_PID=$!

until curl --silent --fail \
  http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; do
  kill -0 "$SERVER_PID"
  sleep 1
done

hey -n 20 -c 20 -m POST -d busy http://127.0.0.1:8787/busy
```

Expected: one request executes, one waits, and excess admissions receive HTTP
503. HTTP 504s are expected for the admitted `/busy` requests.

Stop the server cleanly:

```bash
kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
```

### Optional host reboot check

This command intentionally disconnects the SSH session. Record the hashes
first:

```bash
sha256sum \
  build-elfloader/workerd-executor/executor \
  build-elfloader/workerd-executor/rootfs.img \
  target/release/examples/workerd-demo \
  | tee "$HOME/results/pre-reboot-sha256.txt"
```

Run the reboot command separately:

```bash
sudo reboot
```

After reconnecting:

```bash
cd "$HOME/src/hyperlight-unikraft"
sha256sum -c "$HOME/results/pre-reboot-sha256.txt"
```

## 7. Run every WinterTC capability demo

Start a deterministic loopback upstream that accepts the Workerd demo's POST
upload:

```bash
mkdir -p "$HOME/results/upstream"
cat > "$HOME/results/upstream/server.py" <<'PY'
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BODY = b"loopback-upstream\n"

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("content-length", "0"))
        self.rfile.read(length)
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(BODY)))
        self.end_headers()
        self.wfile.write(BODY)

    def log_message(self, format, *args):
        print(format % args, flush=True)

ThreadingHTTPServer(("127.0.0.1", 18080), Handler).serve_forever()
PY

python3 "$HOME/results/upstream/server.py" \
  >"$HOME/results/upstream/server.log" 2>&1 &
UPSTREAM_PID=$!
```

Start the human-runnable WinterTC demo bundle with only that loopback port
allowed for outbound fetch:

```bash
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/wintertc-demo"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-wintertc-demo.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 5000 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 8 \
  --queue-capacity 128 \
  --fetch-loopback-port 18080 \
  --profile-log-every 1 \
  >"$HOME/results/wintertc-demo/server.log" 2>&1 &
SERVER_PID=$!

until curl --silent --fail \
  http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; do
  kill -0 "$SERVER_PID"
  sleep 1
done
```

Run each independent route:

```bash
for route in \
  core \
  timers \
  global-handlers \
  byob \
  byte-stream-tee \
  core-wasm
do
  curl --fail-with-body -sS \
    "http://127.0.0.1:8787/evidence/$route" \
    | tee "$HOME/results/wintertc-demo/$route.json"
  printf '\n'
done

curl --fail-with-body -sS \
  'http://127.0.0.1:8787/evidence/state?token=azure-kvm-demo' \
  | tee "$HOME/results/wintertc-demo/state-first.json"
printf '\n'

curl --fail-with-body -sS \
  'http://127.0.0.1:8787/evidence/state?token=azure-kvm-demo' \
  | tee "$HOME/results/wintertc-demo/state-second.json"
printf '\n'

curl --fail-with-body -sS \
  'http://127.0.0.1:8787/evidence/fetch?upstream=http://127.0.0.1:18080/' \
  | tee "$HOME/results/wintertc-demo/fetch.json"
printf '\n'
```

Exercise every MessagePort stage:

```bash
for stage in \
  construct \
  listener-registration \
  start \
  post-message \
  queued-delivery \
  close \
  transfer-reentanglement \
  clone-failure
do
  curl --fail-with-body -sS \
    "http://127.0.0.1:8787/evidence/messageport?stage=$stage" \
    | tee "$HOME/results/wintertc-demo/messageport-$stage.json"
  printf '\n'
done
```

Success means every command returns HTTP 200 and the route JSON reports its
behavior as passing. The two state requests must both show fresh request state;
mutable module state must not carry from one request VM to the next.

Stop both processes:

```bash
kill -TERM "$SERVER_PID" "$UPSTREAM_PID"
wait "$SERVER_PID"
wait "$UPSTREAM_PID"
```

## 8. Run a simple parallel `hey` benchmark

Use the 9,441-byte `/sync` response from
`examples/workerd-bundles/workerd-pool-benchmark.json`.

Start a fresh on-demand server:

```bash
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/hey-on-demand"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-pool-benchmark.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 30000 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 256 \
  --profile-log-every 1 \
  >"$HOME/results/hey-on-demand/server.log" 2>&1 &
SERVER_PID=$!

until curl --silent --fail \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  >"$HOME/results/hey-on-demand/startup-status.json"; do
  kill -0 "$SERVER_PID"
  sleep 1
done
```

Verify response identity before load:

```bash
curl --fail-with-body -sS \
  http://127.0.0.1:8787/sync \
  -o "$HOME/results/hey-on-demand/sync.bin"

test "$(wc -c < "$HOME/results/hey-on-demand/sync.bin")" -eq 9441
sha256sum "$HOME/results/hey-on-demand/sync.bin"
```

Run a single request, one exact 32-client wave, and a 60-second sustained run:

```bash
hey -n 1 -c 1 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-on-demand/single.txt"

hey -n 320 -c 32 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-on-demand/wave-c32.txt"

hey -z 60s -c 32 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-on-demand/sustained-c32.txt"

hey -z 60s -c 64 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-on-demand/sustained-c64.txt"
```

Expected:

- the `Status code distribution` contains only `[200]`;
- `Error distribution` is absent or zero;
- the server remains alive;
- the status endpoint returns to quiescence after load.

`hey` latency is client-observed end-to-end latency. Compare requests/second
and p50/p95/p99 only across matched server settings, payloads, request counts,
client concurrency, host size, and instrumentation.

Inspect status and stop cleanly:

```bash
curl --fail-with-body -sS \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  | tee "$HOME/results/hey-on-demand/final-status.json" \
  | jq .

kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
```

## 9. Compare on-demand and adaptive prewarmed execution

The prewarmed pool preserves one-request-per-VM isolation. Every fixed owner
restores a VM on its own thread, advertises a mailbox through the central ready
queue, executes at most one request, destroys that VM, and later restores a
replacement when the scheduler grants a permit.

Start the adaptive configuration:

```bash
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/hey-adaptive-o48-r1"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-pool-benchmark.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 30000 \
  --restore-mode prewarmed \
  --prewarmed-sandboxes 48 \
  --max-concurrent-restores 1 \
  --warm-floor 1 \
  --ready-low-watermark 16 \
  --ready-high-watermark 32 \
  --max-replenish-batch 2 \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 256 \
  --profile-log-every 1 \
  >"$HOME/results/hey-adaptive-o48-r1/server.log" 2>&1 &
SERVER_PID=$!

until curl --silent --fail \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  | tee "$HOME/results/hey-adaptive-o48-r1/startup-status.json" \
  | jq -e '
      .restore_mode == "prewarmed" and
      .prewarmed_inventory == .prewarmed_ready and
      .prewarmed_ready >= .warm_floor and
      .prewarmed_replenishing == 0
    ' >/dev/null; do
  kill -0 "$SERVER_PID"
  sleep 1
done
```

Run the same matched load:

```bash
hey -n 320 -c 32 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-adaptive-o48-r1/wave-c32.txt"

hey -z 60s -c 32 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-adaptive-o48-r1/sustained-c32.txt"

curl --fail-with-body -sS \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  | tee "$HOME/results/hey-adaptive-o48-r1/final-status.json" \
  | jq .

kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
```

The warm floor is fully restored but non-dispatchable. Stopping the server
destroys the final warm VM. Replenishment begins below the low watermark and
refills toward the high watermark in bounded batches.

Run both modes with matched settings. Concurrent replacement restore can
increase guest execution time, so compare throughput and phase timing rather
than ready misses alone.

### Exact no-refill diagnostic wave

Start 64 owners, prefill all of them, and suppress normal replacement restore
until one exact 32-request wave completes:

```bash
mkdir -p "$HOME/results/hey-no-refill-o64"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-pool-benchmark.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 30000 \
  --restore-mode prewarmed \
  --prewarmed-sandboxes 64 \
  --max-concurrent-restores 1 \
  --warm-floor 1 \
  --ready-low-watermark 16 \
  --ready-high-watermark 32 \
  --max-replenish-batch 2 \
  --diagnostic-no-refill-wave 32 \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 256 \
  --profile-log-every 1 \
  >"$HOME/results/hey-no-refill-o64/server.log" 2>&1 &
SERVER_PID=$!

until curl --silent --fail \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  | tee "$HOME/results/hey-no-refill-o64/startup-status.json" \
  | jq -e '.prewarmed_ready == 64' >/dev/null; do
  kill -0 "$SERVER_PID"
  sleep 1
done

hey -n 32 -c 32 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-no-refill-o64/wave-c32.txt"

curl --fail-with-body -sS \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  | tee "$HOME/results/hey-no-refill-o64/post-wave-status.json" \
  | jq .

```

Expected: all 32 responses are HTTP 200, the diagnostic completion count
reaches 32, replacement restore is paused until the wave ends, and the warm
floor is never violated.

## 10. Inspect pool status and request timing

Query live configuration and pressure:

```bash
curl --fail-with-body -sS \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  | jq '{
      restore_mode,
      max_concurrent_sandboxes,
      effective_concurrency,
      prewarmed_sandboxes,
      max_concurrent_restores,
      warm_floor,
      ready_low_watermark,
      ready_high_watermark,
      max_replenish_batch,
      replenishment_paused,
      replenishment_pause_reason,
      refill_active,
      restore_permits_outstanding,
      prewarmed_inventory,
      prewarmed_ready,
      prewarmed_replenishing,
      execution_slots_in_use,
      restore_slots_in_use,
      recycle_queue_depth,
      teardown_in_flight,
      completion_queue_depth,
      completion_in_flight,
      admitted,
      active,
      queued,
      prewarmed_hits,
      prewarmed_misses
    }'
```

Per-request JSON profiles are written to the server log:

- `admission_wait_ms`: bounded admission/active-capacity delay;
- `ready_owner_wait_ms`: wait after active capacity exists but no dispatchable
  ready owner exists;
- `replenishment_policy_wait_ms`: owner wait for an adaptive restore permit;
- `replenishment_wait_ms`: wait for a restore slot;
- `replenishment_restore_ms`: actual replacement restore, excluding startup
  prefill;
- `snapshot_restore_ms`, `request_setup_ms`, `guest_execution_ms`,
  `response_finish_ms`, `vm_teardown_ms`, and `total_ms`: request phases.

The pool is quiescent when admitted, active, queued, execution/restore slots,
recycle, teardown, completion, and outstanding restore permits are all zero.
In prewarmed mode, inventory must equal ready depth, ready depth must remain at
least the warm floor, and refill/pause state must be inactive.

Stop the no-refill server:

```bash
kill -TERM "$SERVER_PID"
wait "$SERVER_PID"
```

## 11. Use the repository benchmark wrapper

`tools/run-wintertc-pool-benchmark.sh` automates the same `/sync` workload,
server lifecycle, status sampling, identity checks, and JSON reporting. It
requires `cargo`, `curl`, `hey`, and Python 3.

On-demand:

```bash
WINTERTC_POOL_RESTORE_MODE=on-demand \
WINTERTC_POOL_PROFILE_LOG_EVERY=1 \
bash tools/run-wintertc-pool-benchmark.sh \
  build-elfloader/workerd-executor \
  "$HOME/results/wrapper-on-demand" \
  384 \
  32
```

Adaptive:

```bash
WINTERTC_POOL_RESTORE_MODE=prewarmed \
WINTERTC_POOL_PREWARMED_SANDBOXES=48 \
WINTERTC_POOL_MAX_CONCURRENT_RESTORES=1 \
WINTERTC_POOL_WARM_FLOOR=1 \
WINTERTC_POOL_READY_LOW_WATERMARK=16 \
WINTERTC_POOL_READY_HIGH_WATERMARK=32 \
WINTERTC_POOL_MAX_REPLENISH_BATCH=2 \
WINTERTC_POOL_PROFILE_LOG_EVERY=1 \
bash tools/run-wintertc-pool-benchmark.sh \
  build-elfloader/workerd-executor \
  "$HOME/results/wrapper-adaptive-o48-r1" \
  384 \
  32
```

The wrapper writes its JSON result and server log to the output directory.

## 12. Capture 10 ms telemetry and optional `perf` profiles

Build the release binary and set source/diff identifiers:

```bash
cd "$HOME/src/hyperlight-unikraft"
cargo build --release --locked --example workerd-demo

SOURCE_COMMIT="$(git rev-parse HEAD)"
TREE_DIFF_SHA256="$(git diff --binary HEAD | sha256sum | awk '{print $1}')"
BINARY=target/release/examples/workerd-demo
ARTIFACT_DIR=build-elfloader/workerd-executor
BUNDLE=examples/workerd-bundles/workerd-pool-benchmark.json
```

Matched on-demand control:

```bash
python3 tools/run-workerd-prewarm-profile.py \
  --binary "$BINARY" \
  --artifact-dir "$ARTIFACT_DIR" \
  --bundle "$BUNDLE" \
  --output-dir "$HOME/results/profile-on-demand" \
  --source-commit "$SOURCE_COMMIT" \
  --patch-sha256 "$TREE_DIFF_SHA256" \
  --scratch-mib 384 \
  --restore-mode on-demand \
  --active 32 \
  --requests 320 \
  --concurrency 32 \
  --profile-log-every 1 \
  --perf-stat
```

Exact no-refill wave:

```bash
python3 tools/run-workerd-prewarm-profile.py \
  --binary "$BINARY" \
  --artifact-dir "$ARTIFACT_DIR" \
  --bundle "$BUNDLE" \
  --output-dir "$HOME/results/profile-no-refill-o64" \
  --source-commit "$SOURCE_COMMIT" \
  --patch-sha256 "$TREE_DIFF_SHA256" \
  --scratch-mib 384 \
  --restore-mode prewarmed \
  --active 32 \
  --owners 64 \
  --restores 1 \
  --warm-floor 1 \
  --ready-low 16 \
  --ready-high 32 \
  --replenish-batch 2 \
  --diagnostic-wave 32 \
  --requests 32 \
  --concurrency 32 \
  --profile-log-every 1 \
  --perf-stat
```

Sustained adaptive row:

```bash
python3 tools/run-workerd-prewarm-profile.py \
  --binary "$BINARY" \
  --artifact-dir "$ARTIFACT_DIR" \
  --bundle "$BUNDLE" \
  --output-dir "$HOME/results/profile-adaptive-o48-r1" \
  --source-commit "$SOURCE_COMMIT" \
  --patch-sha256 "$TREE_DIFF_SHA256" \
  --scratch-mib 384 \
  --restore-mode prewarmed \
  --active 32 \
  --owners 48 \
  --restores 1 \
  --warm-floor 1 \
  --ready-low 16 \
  --ready-high 32 \
  --replenish-batch 2 \
  --duration 60s \
  --concurrency 32 \
  --profile-log-every 1 \
  --perf-stat \
  --deep-profile
```

The runner writes:

- `result.json` with configuration, artifact hashes, correctness, throughput,
  latency, phase distributions, process/host deltas, and server return code;
- `timeseries.jsonl` with 10 ms process, host load/PSI, and pool samples;
- raw `hey` CSV and the server log;
- `perf stat`, CPU, off-CPU, and syscall-trace outputs when supported.

If hardware counters or call stacks are unavailable, the result records them
in `profiler_limitations`.

## 13. Stop application processes

Every foreground walkthrough section stops the process it starts, and the two
repository runners stop their child server before returning. Confirm that no
demo or loopback process remains:

```bash
pgrep -af 'target/release/examples/workerd-demo|results/upstream/server.py' || true
```

Copy the required result files off the VM, then delete the Azure resources
created for the VM using the same Azure workflow that created them.
