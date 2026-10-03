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
export RUSTUP_HOME="$HOME/.rustup"
export CARGO_HOME="$HOME/.cargo"
export PATH="$HOME/go/bin:$CARGO_HOME/bin:$PATH"

grep -Fqx 'export PATH="$HOME/go/bin:$HOME/.cargo/bin:$PATH"' "$HOME/.profile" \
  || printf '%s\n' 'export PATH="$HOME/go/bin:$HOME/.cargo/bin:$PATH"' \
    >> "$HOME/.profile"

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

Install and verify the load generator before building or starting any
benchmark server:

```bash
sudo apt-get install -y golang-go
go install github.com/rakyll/hey@v0.1.4
export PATH="$HOME/go/bin:$HOME/.cargo/bin:$PATH"
command -v hey
hey -version
```

## 4. Build the Workerd executor

Clone the executor integration branch:

```bash
cd "$HOME/src"
git clone --branch simongdavies-workerd-compliance-integration --single-branch --recurse-submodules https://github.com/simongdavies/workerd.git
cd workerd
git rev-parse HEAD
test "$(git rev-parse HEAD)" = \
  bbc51f926e293bdaaf2d8185f78220ca722993c6
```

The current integration commit is
`bbc51f926e293bdaaf2d8185f78220ca722993c6`. It contains the complete WinterTC
executor implementation.

Apply and verify the validated static host-tool C++ linker correction:

```bash
grep -Fqx \
  "build:linux --host_linkopt='-lc++' --host_linkopt='-lm'" \
  .bazelrc

sed -i \
  "s/build:linux --host_linkopt='-lc++' --host_linkopt='-lm'/build:linux --host_linkopt='-l:libc++.a' --host_linkopt='-lm'/" \
  .bazelrc

grep -Fqx \
  "build:linux --host_linkopt='-l:libc++.a' --host_linkopt='-lm'" \
  .bazelrc
```

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
  bash -c '
    export PATH=/usr/lib/llvm-22/bin:$PATH &&
    export CC=/usr/lib/llvm-22/bin/clang &&
    export CXX=/usr/lib/llvm-22/bin/clang++ &&
    executor=bazel-bin/src/workerd/server/workerd-sandbox-executor &&
    rm -f "$executor" &&
    bazel --output_base=/root/.cache/bazel/workerd-hyperlight-output \
      build //src/workerd/server:workerd-sandbox-executor \
      --config=opt \
      --strip=always \
      --//:io_backend=cxx \
      --jobs="${WORKERD_BAZEL_JOBS:-$(nproc)}" \
      --disk_cache=/root/.cache/bazel/action-cache \
      --repository_cache=/root/.cache/bazel/repository-cache \
      --repo_env=CC=/usr/lib/llvm-22/bin/clang \
      --repo_env=CXX=/usr/lib/llvm-22/bin/clang++ \
      --announce_rc &&
    test -x "$executor" &&
    "$executor" --self-test &&
    /usr/lib/llvm-22/bin/llvm-strip "$executor" &&
    cp "$executor" /artifacts/workerd-sandbox-executor &&
    chmod 0755 /artifacts/workerd-sandbox-executor
  '

export WORKERD_EXECUTOR="$HOME/artifacts/workerd-sandbox-executor"
```

Every Workerd Bazel command in this qualification must reuse this same
pinned-container cache lane: output base
`/root/.cache/bazel/workerd-hyperlight-output`, repository cache
`/root/.cache/bazel/repository-cache`, disk cache
`/root/.cache/bazel/action-cache`, and the mounted host cache
`$HOME/.cache/workerd-bazel`. Do not create per-session cache roots or duplicate
an already completed build. Consume a handed-off verified executor whenever
possible; rebuild only when Workerd source inputs changed.

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

If replacing an executor built from an earlier Workerd checkout, rebuild the
executor above and rerun `examples/workerd-executor/build-rootfs.sh` below.
The Workerd-specific kernel and `workerd-demo` host binary do not need to be
rebuilt solely for this source correction.

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

if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
fi
SERVER_PID=
SERVER_LOG="$HOME/results/on-demand/server.log"

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
  >"$SERVER_LOG" 2>&1 &

SERVER_PID=$!
echo "$SERVER_PID" > "$HOME/results/on-demand/server.pid"

SERVER_READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status \
    >"$HOME/results/on-demand/startup-status.json"; then
    SERVER_READY=true
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    echo "On-demand server stopped before readiness. Recent log output:"
    tail -n 100 "$SERVER_LOG"
    break
  fi
  sleep 1
done

if [[ "$SERVER_READY" == true ]]; then
  jq . "$HOME/results/on-demand/startup-status.json"
else
  echo "On-demand server is not ready; do not continue this section."
fi
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
if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
fi
SERVER_PID=
SERVER_LOG="$HOME/results/on-demand/overload-server.log"

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
  >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

SERVER_READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
    SERVER_READY=true
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    echo "Overload server stopped before readiness. Recent log output:"
    tail -n 100 "$SERVER_LOG"
    break
  fi
  sleep 1
done

if [[ "$SERVER_READY" == true ]]; then
  printf '=== bounded-overload-c20 ===\n'
  printf '%s\n' \
    'One active slot plus one queued slot proves bounded admission: admitted busy requests time out while excess requests are rejected.'
  printf 'Endpoint: http://127.0.0.1:8787/busy\n'
  printf 'Load: hey -n 20 -c 20 -m POST -d busy\n'
  hey -n 20 -c 20 -m POST -d busy \
    http://127.0.0.1:8787/busy \
    | tee "$HOME/results/on-demand/overload-c20.txt"
  statuses=("${PIPESTATUS[@]}")
  if (( statuses[0] == 0 && statuses[1] == 0 )); then
    printf 'PASS bounded-overload-c20\n\n'
  else
    printf 'FAIL bounded-overload-c20: hey=%s tee=%s\n\n' \
      "${statuses[0]}" "${statuses[1]}" >&2
  fi
else
  echo "Overload server is not ready; benchmark skipped."
fi
```

Expected: one request executes and one waits in the bounded queue. Both
admitted `/busy` requests time out with HTTP 504; excess admissions receive
HTTP 503. No 2xx response is expected. Inspect the status-code distribution
and error section in `overload-c20.txt`.

Stop the server cleanly:

```bash
if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
fi
if [[ -n "${SERVER_PID:-}" ]]; then
  wait "$SERVER_PID" 2>/dev/null || true
fi
SERVER_PID=
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
    def read_exactly(self, length):
        data = self.rfile.read(length)
        if len(data) != length:
            raise ValueError("request body ended early")

    def drain_request_body(self):
        transfer_encoding = self.headers.get("transfer-encoding")
        content_length = self.headers.get("content-length")
        if transfer_encoding is not None:
            if transfer_encoding.lower() != "chunked":
                raise ValueError("unsupported Transfer-Encoding")
            while True:
                line = self.rfile.readline()
                if not line.endswith(b"\r\n"):
                    raise ValueError("malformed chunk size")
                size_text = line[:-2].split(b";", 1)[0].strip()
                size = int(size_text, 16)
                if size == 0:
                    while True:
                        trailer = self.rfile.readline()
                        if trailer == b"\r\n":
                            return
                        if not trailer or not trailer.endswith(b"\r\n"):
                            raise ValueError("malformed chunk trailer")
                self.read_exactly(size)
                if self.rfile.read(2) != b"\r\n":
                    raise ValueError("malformed chunk terminator")
        if content_length is None:
            return
        length = int(content_length, 10)
        if length < 0:
            raise ValueError("negative Content-Length")
        self.read_exactly(length)

    def do_POST(self):
        try:
            self.drain_request_body()
        except (ValueError, OverflowError) as error:
            self.send_error(400, str(error))
            return
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

if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
fi
SERVER_PID=
SERVER_LOG="$HOME/results/wintertc-demo/server.log"

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
  >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

SERVER_READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
    SERVER_READY=true
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    echo "WinterTC server stopped before readiness. Recent log output:"
    tail -n 100 "$SERVER_LOG"
    break
  fi
  sleep 1
done
if [[ "$SERVER_READY" != true ]]; then
  echo "WinterTC server is not ready; do not continue this section."
fi
```

Use the reusable evidence runner against the running demo. It prints the
endpoint and a short explanation before each check, preserves raw JSON under
`$HOME/results/wintertc-demo`, displays colorized pretty JSON, continues after
failures, and returns nonzero if any requested check fails.

List the stable check names and descriptions:

```bash
bash tools/run-wintertc-demo.sh --list
```

Run the complete suite:

```bash
bash tools/run-wintertc-demo.sh all
```

Pause for Enter between checks when running interactively:

```bash
bash tools/run-wintertc-demo.sh --pause all
```

Run only the streaming fetch evidence:

```bash
bash tools/run-wintertc-demo.sh fetch
```

Run one named MessagePort stage:

```bash
bash tools/run-wintertc-demo.sh messageport-queued-delivery
```

`state` makes two requests and requires both responses to report
`requestCount: 1` and `previousStateToken: null`, proving mutable module state
does not carry from one request VM to the next. `messageport` runs all eight
MessagePort stages. Use `--base-url` and `--output-dir` to override the running
demo URL and raw JSON directory. The runner does not start or stop either
server.

The successful summary is deliberately scoped. `16/16 WinterTC demo checks
passed` means eight core checks (`core`, timers, global handlers, BYOB,
byte-stream tee, core Wasm, fresh-VM state isolation, and host-brokered fetch)
plus eight MessagePort lifecycle stages; it is not a claim of complete
Workerd or Cloudflare Workers parity.

| Capability | State | Evidence or boundary |
|---|---|---|
| V8 isolate in a Hyperlight VM | Proven | Real Workerd/JSG/V8 runs in disposable Hyperlight-Unikraft VMs with one request per VM. |
| Host-brokered fetch | Proven | Deny-by-default lifecycle, policy, streaming, limit, cancellation, and redirect evidence passed. |
| Selected WinterTC evidence | Proven | The named runner passes 16/16; this is not complete WinterTC or standalone Workerd conformance. |
| Core WebAssembly | Proven | Core Wasm executes in the selected real-executor evidence. |
| Workerd VFS `/bundle` | Proven | Focused executor evidence and the final real-Hyperlight storage/VFS 18/18 run prove the owned module directory is readable and read-only. |
| Workerd VFS `/tmp` | Proven | Existing packaged-executor evidence proves read/write and fresh-request reset. |
| Workerd `/dev/null`, `/dev/zero`, `/dev/random` | Proven | Existing packaged-executor evidence passes each deterministic device check. |
| Named host-backed storage | Proven | Final real-Hyperlight storage/VFS evidence passes 18/18, including named read, RO/RW behavior, traversal and unlisted denial, and fresh-request reset. |
| Storage RO/RW policy | Proven | Host-selected named paths, confinement, deny default, snapshot policy binding, and the `ExecutorInit` JSON object with `protocol_version: 2` are covered by final evidence. |
| Storage accounting quotas | Proven | Per-sandbox operation/read/write budgets and native/Node/Web quota mappings pass final evidence; these remain accounting budgets, not capacity or inode quotas. |
| Configured upstream Node tests | Proven | The final source query discovers 412 tests. Three variants whose upstream oracle requires `cloudflare.com:80` are explicitly excluded; all 409 selected tests pass in 211.07 seconds with no failures or skips. The broader query discovers 522 generated targets, excludes four hosted-network variants, and proves all 518 selected targets after exact uncached retries. |
| Packaged Node workloads | Proven | Targeted CommonJS recovery passed 15/15, then the final complete five-workload real-KVM regression passed all 25 assertions in fresh one-request VMs. |
| MessagePort | Proven for named stages | Eight deterministic lifecycle and transfer stages pass; broader parity follows authoritative upstream tests. |
| Bounded broker protocols/runtime | Source integrated; unrun | Deny-by-default network and logical-service contracts, versioned wire handling, runtime registration, fresh-VM reset, actor seam, and std-only fixtures are present in Hyperlight source. They have not been run in Hyperlight/KVM, do not enable ambient network access, and are not complete Workerd integration or Azure evidence. |
| WebSocket and non-service event entrypoints | Not started | Bounded WebSockets are a post-baseline broker phase; scheduled and queue ingress follow later. Cloudflare-hosted variants remain excluded. |
| Constrained TCP/TLS and UDP | Next / Not started | Ambient sockets remain prohibited. Add separate host-owned destination/DNS policy and protocol-specific quotas after baseline signing. Fetch grants no implicit raw network access. |
| Generic service and identity bindings | Later / Not started | Add explicit logical binding identities, host-owned credentials, policy identity, reset semantics, and deterministic evidence without exposing secrets to guests. |
| Cloudflare product plumbing | Out of scope | R2, AI, Vectorize, Hyperdrive, hosted email/tail/trace, and other Cloudflare-managed backends are excluded. |
| Persistent KV/SQL brokers | Next / Not started | Implement host-owned KV and SQLite/D1-style brokers in the trusted host; MySQL/PostgreSQL remain external services. Never mount live database files into request VMs. |
| Cache API backend | Next / Not started | Add a bounded host-owned cache namespace with explicit identity, capacity/operation limits, lifecycle, and evidence. |
| Durable Objects | Later / Separate architecture | Requires placement, single-writer ownership, durable state, alarms, migration, and failure-recovery design; do not treat it as a storage mount or ordinary KV binding. |
| Broker process separation | Deferred | Current brokers are trusted in-process Rust objects; a sidecar or Mesh architecture is not a dependency. |
| WASI Component Model | Deferred post-proof | A self-contained evaluation fixture, including generated/test/lock evidence, is retained under `experiments/workerd-component-model/`; it is not a promoted runtime capability. Revisit Workerd/V8 plus pinned `jco`/WASI shims only after the signed baseline and clean Azure proof. Core WebAssembly is already separate. |

Only the **Now** baseline is implemented in this change: Workerd VFS,
deny-by-default named RO/RW host storage, host-side accounting quotas,
snapshot policy identity, and conformance/evidence integration. The patched
executor is packaged and passes the final real-Hyperlight storage/VFS and
packaged-workload evidence. Signing and clean Azure qualification remain.

| Phase | Deliverable | Status | Required next action |
|---|---|---|---|
| Now | VFS `/bundle`, `/tmp`, `/dev`; named RO/RW storage; operation/read/write budgets; configured and broader Node tests | Implemented, proven, and locally validated | Preserve the frozen unstaged tree and prepared commit boundaries until fresh just-in-time YubiKey confirmation is available. |
| Next | Bounded broker protocols/runtime | Source integrated; std-only fixtures unrun in Hyperlight/KVM | Complete the real Workerd/Unikraft adapters, policy identity, lifecycle/cancellation integration, deterministic Hyperlight tests, load tests, and clean Azure KVM qualification before changing any capability state. |
| Next | Host-owned KV broker | Not started | Define logical namespaces, CRUD/list/batch ABI, authorization, operation/byte/capacity quotas, snapshot policy identity, fresh-request reset, deterministic tests, load tests, and Azure KVM evidence. |
| Next | Host-owned SQLite/D1-style broker | Not started | Keep database/WAL files and connections in the trusted host; define prepared statements, transactions, result limits, concurrency/time quotas, namespace identity, recovery, evidence, and Azure proof. |
| Next | Cache API backend | Not started | Implement explicit cache namespaces, key/value bounds, TTL/eviction, capacity and operation quotas, reset/isolation rules, deterministic evidence, load tests, and Azure proof. |
| Next | Bounded WebSockets | Not started | Broker handshake and frames through host-owned origin/subprotocol policy with connection, frame, byte, time, concurrency, close, cancellation, and backpressure limits; prove deny/allow/quota/load behavior in Azure. |
| Next | Constrained TCP/TLS | Not started | Prohibit ambient networking; add protocol/host/port and resolved-address allowlists, DNS rebinding checks, connect/read/write/close and TLS controls, socket/operation/byte/time/concurrency quotas, no guest credentials, policy identity, fresh reset, deterministic evidence, load tests, and Azure proof. |
| Next | Constrained UDP | Not started | Prohibit ambient datagrams; add destination/DNS allowlists, send/receive operations, socket/datagram/byte/time/rate/concurrency bounds, lifecycle/cancellation/reset, policy identity, deterministic evidence, load tests, and Azure proof. |
| Later | Scheduled and queue ingress | Not started | Define host-owned authenticated ingress, retry/deduplication, deadlines, payload limits, ordering semantics, fresh-VM dispatch, deterministic replay evidence, and Azure qualification. |
| Later | Generic service and identity bindings | Not started | Define logical names, least-privilege credentials held only by the host, rotation, per-binding quotas, policy/snapshot identity, audit evidence, and failure/reset behavior. |
| Later | Durable Objects | Separate architecture required | Design placement and routing, exclusive ownership, durable transactions, alarms, migration, failover, backpressure, quotas, and multi-host evidence before implementation. |
| Deferred | Component Model interoperability | Deferred post-proof; fixture retained | Keep `experiments/workerd-component-model/` as evaluation-only generated/test/lock evidence. Evaluate pinned `jco`/WASI shims only after baseline signing and clean Azure qualification; do not block or promote the current V8 path. |
| Out of scope | Cloudflare-managed product plumbing | Excluded | Do not implement R2, AI, Vectorize, Hyperdrive, hosted email/tail/trace, or provider control-plane emulation in this roadmap. |

#### Complete mission ledger

Every row remains in status reports even when unchanged. Estimates describe
remaining active work, or state plainly when a later phase is intentionally
unscheduled.

| Work item | Current state | Evidence or blocker | Next action | Estimated remaining time |
|---|---|---|---|---|
| Adaptive prewarm scheduling | Implementation complete; clean Azure proof pending | Warm floor, ready low/high watermarks, bounded refill, diagnostic no-refill wave, and policy metrics are implemented and locally tested. | Run the final 384 MiB Azure matrix after exact signed revisions exist. | 4–8 hours after signed commits |
| Host-brokered fetch | Completed | Deny-by-default policy, explicit allow rules, streaming limits, cancellation, redirects, and deterministic evidence are proven. | Preserve behavior through signing and clean Azure proof. | 0 minutes for implementation |
| Named storage and Workerd virtual filesystem | Completed | Final real-Hyperlight evidence passes 18/18 with exact binary, source, diff, result, and provenance hashes. | Preserve the frozen identities through commit preparation. | 0 minutes for implementation |
| Packaged Node workloads | Completed, timing table update active | Five workloads pass all 25 assertions in fresh one-request real-KVM sandboxes. | Add the exact final per-workload timings from `result.json`. | 10–20 minutes after the timing artifact is available |
| Configured upstream Node tests | Completed | 412 tests discovered; three hosted-oracle variants excluded; 409 selected tests passed in 211.07 seconds with zero failures and skips. | Record the exact three excluded names beside the result. | 5–10 minutes after the list is available |
| Broader upstream Node tests | Completed | 522 generated targets discovered; four hosted-network variants excluded; the initial 518-target run passed 498 and timed out 20 under 48-way contention in 293.700305743 seconds. Exact uncached retries passed 20/20 in 75.724202257 seconds, proving 518/518 with zero assertion/runtime failures in 369.424508 seconds total execution time. | Preserve the exact exclusions and contention/retry distinction recorded below. | Done |
| Repository non-Node tests | Completed; not promoted | 2,138 selected, 2,128 executed, 2,044 passed, 94 failed, zero skipped, and 10 not executed. Exact retries preserved all 94 failures; none are waived. Initial/retry/total durations are 5,301.316543806 / 3,622.221737482 / 8,923.538281288 seconds. | Use the preserved failures to define later compatibility work; do not represent this as a full repository pass. | Done |
| Hyperlight formatting, build, and static analysis | Completed | `just fmt-apply`, `just build`, native `just clippy`, and Ubuntu-24.04 WSL `just clippyw` pass. | Rerun only if the final source changes. | 0 minutes unless source changes |
| Hyperlight full tests | Completed | After rebuilding the stale Workerd mock fixture with `just guests`, the standard complete `just test` run passes. The final run includes `.NET` HTTP and OOM plus the real-guest storage policy test. Earlier external HTTP timeouts reproduced on pristine and current source and cleared without source changes. The 22,647-byte passing log SHA-256 is `a3849220836664f22b056a555442991e51538c92bcb03190abed11fa9b5120e1`. | Preserve the passing log and rerun only if source or guest inputs change. | Done |
| Final source review | Completed | Review found three issues: poisoned broker mutex reuse, duplicate executor-handle accounting, and incomplete `ExecutorInit` documentation. All are fixed; focused broker tests pass 31/31. | Run final diff and documentation checks after evidence updates. | 5–10 minutes |
| Hyperlight commit preparation | Ready but not staged | Nothing is staged or committed. Four independent commit messages are prepared outside the repository. `just guests`, `just fmt-apply`, `just build`, `just clippy`, WSL `just clippyw`, complete `just test`, and `git diff --check` pass on the frozen tree. | Stage one approved boundary at a time, then request fresh YubiKey readiness immediately before each `git commit -s -S`. | 15–25 minutes plus user-interactive signing |
| Workerd commit preparation | Ready in the Workerd checkout | Final source, evidence identities, compatibility boundaries, and separate remediation plan are frozen. No repository failure belongs directly to the four approved commits. | Preserve the prepared independent boundaries and request YubiKey confirmation separately for each commit. | 0 minutes before interactive staging/signing |
| Signed commits | Blocked only on user presence | Every commit requires `git commit -s -S` and fresh just-in-time YubiKey confirmation. All fallible validation is complete. | Stage and review one approved boundary, prepare its exact command, then ask immediately before running it. | Approximately 15–25 minutes staging/review plus 2–5 user-interactive minutes per commit |
| Push | Not started | Push is intentionally deferred until signed commits are verified. | Push only after explicit user direction. | 5–10 minutes after commits |
| Clean Azure scaling and profiling proof | Not started | Exact signed Hyperlight and Workerd revisions do not yet exist. No resource is active for this phase. | Prove zero stale resources, create the leased tagged group, run the 384 MiB matrix, and capture profiler/time-series evidence. | 4–8 hours after push |
| Node qualification evidence and Azure cleanup | Completed | Explicit-allowlist archive SHA-256 `89965d622c9b6cfd8fe410a25b0192032d8c509ad75863a9a54edff60551ebcb`; 134/134 payload files independently verified. Dedicated RG deletion reached `az group exists == false` at 2026-10-02T22:02:37Z; final tagged and matching group/resource queries are empty. | Preserve the archive and cleanup proof; this does not replace the later clean scaling/profiling campaign. | Done |
| Host-owned key-value storage | Not started | No KV host functions or Workerd adapter are registered. | Design explicit namespaces and finite operation, byte, key, and concurrency limits. | Separate phase; no estimate yet |
| Cache API | Not started | No host-owned cache binding exists. | Define bounded keys, values, expiry, eviction, capacity, isolation, and evidence. | Separate phase; no estimate yet |
| SQLite/D1-style storage | Not started | Named filesystem mounts are not a transactional database broker. | Implement host-owned namespaces, statements, transactions, result limits, quotas, and recovery without mounting live databases into request sandboxes. | Separate phase; no estimate yet |
| WebSockets | Not started | Fetch grants no WebSocket capability; source-only contracts are not runtime proof. | Implement and qualify origin/subprotocol policy, message limits, deadlines, close behavior, backpressure, and cleanup. | Separate phase; no estimate yet |
| Brokered TCP/TLS | Next; not started | Ambient sockets remain prohibited. | Add a separately enabled host-owned destination, DNS, credential, connection, byte, operation, concurrency, and timeout policy. | Separate phase; no estimate yet |
| Brokered UDP | Not started | No qualified UDP host function is registered for Workerd. | Add destination and DNS rules plus datagram count, size, byte, rate, concurrency, lifecycle, and timeout limits. | Separate phase; no estimate yet |
| Scheduled and queue events | Not started | Only request/fetch dispatch is qualified. | Define trusted ingress, payload limits, retry identity, ordering, deadlines, and one-event-per-sandbox evidence. | Separate phase; no estimate yet |
| Service and identity bindings | Not started | No generic logical binding or host-owned credential delivery is qualified. | Define explicit binding identities, non-leaking credentials, rotation, policy identity, audit records, quotas, and reset behavior. | Separate phase; no estimate yet |
| Durable Objects-style actors | Deferred; separate architecture | Request sandboxes cannot serve as durable actor state. The source actor seam is not runtime proof. | Design host-owned routing, exclusive activation, persistence, transactions, alarms, migration, recovery, and quotas. | Deferred; no estimate yet |
| Wasm Component Model | Deferred experiment | The pinned lowering fixture is deterministic and policy-locked but is not production parity evidence. | Revisit only after size, cold-start, restore, policy, and toolchain gates justify adoption. | Deferred; no estimate yet |
| Workerd Rust guest refactor | Deferred until after qualification | Current final evidence covers the existing C++ bridge. A refactor could change parsing, wrappers, or state. | Retain a minimal C++ Workerd object bridge, move substantive guest logic into existing Rust+cxx infrastructure, then rerun complete equivalence evidence. | Post-qualification phase; no estimate yet |
| Upstream Workerd candidates | Not started | The compatibility-preserving EDQUOT infrastructure and narrow TLS/BYOB and Node filesystem fixes are proven locally but not prepared as upstream submissions. | Split minimal patches, document necessity and compatibility, and submit only after the signed baseline. | Separate phase; no estimate yet |
| Cloudflare-hosted product services | Out of scope | R2, AI, Vectorize, Hyperdrive, hosted email/tail/trace, and provider control-plane behavior require external Cloudflare services. | No action in this mission. | No work planned |

#### Broker lifecycle and trust boundary

The current fetch, timer, and hostfs brokers are Rust objects plus registered
host-call closures inside the single trusted `workerd-demo` host process.
Hyperlight does not start a separate broker daemon. The Rust host broker is
part of the trusted computing base.

The newly integrated generic broker protocol/runtime source is a separate,
default-off foundation. `SandboxBuilder::broker_runtime()` registers no broker
function unless the host supplies an explicit network adapter or logical
service, and `BrokerRuntime::deny_all()` grants neither. Its std-only fixtures
and unit surfaces are source-integrated but have not been exercised in
Hyperlight/KVM. Complete Workerd integration and clean Azure qualification
remain required; this does not extend the proven capability set above.

At process startup the launcher parses immutable policies, creates shared
broker/backend state, initializes the Worker version and snapshot template,
then starts the VM pool. Every initialized or restored sandbox receives its
own fetch session, timer session, and hostfs registration. Fetch operation
maps/deadlines and storage counters are per sandbox/request; immutable policy
and process-level concurrency/backend state may be shared through `Arc`.

Prewarmed owners create these per-sandbox sessions and mounts during restore,
before request assignment. On-demand owners create them while restoring the VM
for that request. The sessions, registrations, and counters are discarded with
the one-request VM.

Future SQLite KV/SQL should remain embedded/shared in this trusted host
process: one process-level durable backend/connection pool plus a
per-sandbox capability session and quotas, not one broker or SQLite process
per VM. MySQL/PostgreSQL remain external services reached only by the trusted
host broker. A separate broker sidecar is an optional future
defense-in-depth deployment, not the current architecture.

Stop both processes:

```bash
kill -TERM "$SERVER_PID" "$UPSTREAM_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
wait "$UPSTREAM_PID" 2>/dev/null || true
SERVER_PID=
UPSTREAM_PID=
```

### Workerd in-memory VFS evidence

This section exercises upstream Workerd's own virtual filesystem from Worker
JavaScript in real V8/KVM. It is separate from Hyperlight hostfs and the named
host-backed policy in the following section. The pinned fork requires
`nodejs_compat` plus `enable_nodejs_fs_module`; the checked-in bundle declares
both flags.

Start one-request-per-VM on-demand execution with the VFS evidence bundle:

```bash
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/workerd-vfs"

if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
fi
SERVER_PID=
SERVER_LOG="$HOME/results/workerd-vfs/server.log"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-vfs-evidence.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 10000 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 1 \
  --queue-capacity 8 \
  >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

SERVER_READY=false
for _ in $(seq 1 120); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
    SERVER_READY=true
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    tail -n 100 "$SERVER_LOG"
    break
  fi
  sleep 1
done
if [[ "$SERVER_READY" != true ]]; then
  echo "Workerd VFS server is not ready; do not continue this section."
fi
```

List the stable checks, run all evidence, pause interactively, or run only the
fresh-request `/tmp` check:

```bash
bash tools/run-workerd-vfs-demo.sh --list
bash tools/run-workerd-vfs-demo.sh all
bash tools/run-workerd-vfs-demo.sh --pause all
bash tools/run-workerd-vfs-demo.sh tmp-reset
```

Success proves Worker JavaScript can read the `/bundle` directory while it
remains immutable; `/tmp` supports read/write but starts empty in both
independent request VMs; `/dev/null` discards writes and returns EOF;
`/dev/zero` returns zero bytes; and `/dev/random` returns two nonzero, distinct
samples. Raw JSON is preserved under `$HOME/results/workerd-vfs`.

```bash
if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
fi
wait "${SERVER_PID:-}" 2>/dev/null || true
SERVER_PID=
```

### Policy-constrained outbound fetch evidence

This focused demo proves that outbound authority belongs to the Rust host.
The Worker accepts a target URL, but cannot add hosts, schemes, ports, or
address classes to the broker policy. Redirects are returned to the Worker and
are not followed. Loopback, private, and metadata destinations are separately
gated.

Start a deterministic local upstream with one success route and one redirect:

```bash
mkdir -p "$HOME/results/fetch-policy/upstream"
cat >"$HOME/results/fetch-policy/upstream/server.py" <<'PY'
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BODY = b"allowed-upstream\n"

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/ok":
            self.send_response(200)
            self.send_header("content-type", "text/plain")
            self.send_header("content-length", str(len(BODY)))
            self.end_headers()
            self.wfile.write(BODY)
            return
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("location", "/ok")
            self.send_header("content-length", "0")
            self.end_headers()
            return
        self.send_error(404)

    def log_message(self, format, *args):
        print(format % args, flush=True)

ThreadingHTTPServer(("127.0.0.1", 18080), Handler).serve_forever()
PY

if [[ -n "${UPSTREAM_PID:-}" ]] && kill -0 "$UPSTREAM_PID" 2>/dev/null; then
  kill -TERM "$UPSTREAM_PID" 2>/dev/null || true
  wait "$UPSTREAM_PID" 2>/dev/null || true
fi
UPSTREAM_PID=
python3 "$HOME/results/fetch-policy/upstream/server.py" \
  >"$HOME/results/fetch-policy/upstream/server.log" 2>&1 &
UPSTREAM_PID=$!
```

Start the dedicated Worker with an explicit general policy. Only HTTP to
`localhost:18080` is listed, and loopback is the only address-class opt-in:

```bash
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/fetch-policy"

if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
fi
SERVER_PID=
SERVER_LOG="$HOME/results/fetch-policy/server.log"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/fetch-policy-demo.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 5000 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 4 \
  --queue-capacity 32 \
  --fetch-allow-host localhost \
  --fetch-allow-scheme http \
  --fetch-allow-port 18080 \
  --fetch-allow-loopback \
  --fetch-max-request-bytes 1048576 \
  --fetch-max-response-bytes 4194304 \
  --fetch-max-concurrent-requests 8 \
  --fetch-timeout-ms 5000 \
  >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

SERVER_READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
    SERVER_READY=true
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    echo "Fetch-policy server stopped before readiness. Recent log output:"
    tail -n 100 "$SERVER_LOG"
    break
  fi
  sleep 1
done
if [[ "$SERVER_READY" != true ]]; then
  echo "Fetch-policy server is not ready; do not continue this section."
fi
```

List the stable checks and the policy boundary each one demonstrates:

```bash
bash tools/run-workerd-fetch-policy-demo.sh --list
```

Run all policy checks and preserve their raw JSON:

```bash
bash tools/run-workerd-fetch-policy-demo.sh all
```

Pause between checks in an interactive walkthrough:

```bash
bash tools/run-workerd-fetch-policy-demo.sh --pause all
```

Run one denial check:

```bash
bash tools/run-workerd-fetch-policy-demo.sh metadata-denied
```

The complete run requires the allowed endpoint to succeed; wrong port,
`example.com`, HTTPS, `169.254.169.254`, and `10.0.0.1` to be rejected by
policy before connection; and the redirect route to return HTTP 302 with its
`Location` header instead of following it. Raw JSON is stored under
`$HOME/results/workerd-fetch-policy`.

Stop both processes:

```bash
if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
fi
if [[ -n "${UPSTREAM_PID:-}" ]] && kill -0 "$UPSTREAM_PID" 2>/dev/null; then
  kill -TERM "$UPSTREAM_PID" 2>/dev/null || true
fi
wait "${SERVER_PID:-}" 2>/dev/null || true
wait "${UPSTREAM_PID:-}" 2>/dev/null || true
SERVER_PID=
UPSTREAM_PID=
```

### Restricted named storage evidence

Workerd's `node:fs` API operates on Workerd's virtual filesystem. Hyperlight
hostfs is a separate guest-kernel facility. The Rust launcher owns the mapping
from logical binding names to canonical host directories and mounts only
`/mnt/workerd-storage/NAME`; Worker input never chooses a host path. The real
Workerd executor's `init` guest call receives an `ExecutorInit` JSON object
with `protocol_version: 2` and only sorted logical `{name,mode}` entries. The
executor maps the corresponding fixed guest directories to `/storage/NAME`;
it never receives host paths. Policies contain at most eight
bindings, with lowercase names that start with a letter and contain only
letters, digits, or `-`. The native executor fixture below directly exercises
the same guest mount, confinement, mode, quota, snapshot-restore, and
fresh-owner boundary; treat it as executor-level evidence until the patched
real executor is built and run.

Build the changed fixture and create deterministic host directories:

```bash
cd "$HOME/src/hyperlight-unikraft"
just guests

STORAGE_ROOT="$HOME/results/workerd-storage-policy/host"
rm -rf -- "$STORAGE_ROOT"
mkdir -p "$STORAGE_ROOT/readonly" "$STORAGE_ROOT/scratch" "$STORAGE_ROOT/outside"
printf 'fixture-read-ok\n' > "$STORAGE_ROOT/readonly/message.txt"
printf 'must-not-read\n' > "$STORAGE_ROOT/outside/secret.txt"
ln -s -- "$STORAGE_ROOT/outside" "$STORAGE_ROOT/readonly/escape"
```

Start the fixture with one read-only and one read-write binding. The 16-byte
write budget is intentionally small so the quota check is deterministic:

```bash
if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
fi
SERVER_PID=
SERVER_LOG="$HOME/results/workerd-storage-policy/server.log"
mkdir -p "$(dirname "$SERVER_LOG")"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor-fixture/executor \
  --rootfs build-elfloader/workerd-executor-fixture/rootfs.img \
  --bundle examples/workerd-bundles/helloworld_esm.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 10000 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 1 \
  --queue-capacity 8 \
  --storage-ro "readonly=$STORAGE_ROOT/readonly" \
  --storage-rw "scratch=$STORAGE_ROOT/scratch" \
  --storage-max-operations readonly=128 \
  --storage-max-read-bytes readonly=1048576 \
  --storage-max-operations scratch=128 \
  --storage-max-read-bytes scratch=1048576 \
  --storage-max-write-bytes scratch=16 \
  >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

SERVER_READY=false
for _ in $(seq 1 120); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
    SERVER_READY=true
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    tail -n 100 "$SERVER_LOG"
    break
  fi
  sleep 1
done
if [[ "$SERVER_READY" != true ]]; then
  echo "Storage-policy fixture is not ready; do not continue this section."
fi
```

List and run the stable checks:

```bash
bash tools/run-workerd-storage-policy-demo.sh --list
bash tools/run-workerd-storage-policy-demo.sh all
bash tools/run-workerd-storage-policy-demo.sh --pause all
bash tools/run-workerd-storage-policy-demo.sh quota-denied
```

The six checks require an allowed read, `EROFS` for a read-only write, a
bounded read-write update, denial of a host symlink escape, absence of an
unlisted logical binding, and `EDQUOT` when a write exceeds the host-side byte
budget. Raw JSON is under `$HOME/results/workerd-storage-policy`.

Operation/read/write counters are per mount and per restored sandbox. Every
one-request VM receives fresh counters and the identical mount table bound
into the snapshot identity. Valid-mount calls consume operations; reads charge
bytes returned; accepted write payloads are charged before I/O; and extending
a file with `truncate` charges the requested growth. This does not claim
race-free aggregate storage, maximum-file-size, or file-count quotas: external
host changes and the current stateless `fs_*` host functions make those
capacity claims unsound.

```bash
if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
fi
wait "${SERVER_PID:-}" 2>/dev/null || true
SERVER_PID=
```

Repeat the same six checks through Worker JavaScript and the patched real
Workerd executor. This is the acceptance proof that `/storage/NAME` is the
Worker-visible VFS adapter over the host-owned `/mnt/workerd-storage/NAME`
mounts:

```bash
SERVER_LOG="$HOME/results/workerd-storage-policy/workerd-server.log"

RUST_LOG=info \
target/release/examples/workerd-demo \
  --executor build-elfloader/workerd-executor/executor \
  --rootfs build-elfloader/workerd-executor/rootfs.img \
  --bundle examples/workerd-bundles/workerd-vfs-evidence.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 10000 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 1 \
  --queue-capacity 8 \
  --storage-ro "readonly=$STORAGE_ROOT/readonly" \
  --storage-rw "scratch=$STORAGE_ROOT/scratch" \
  --storage-max-operations readonly=128 \
  --storage-max-read-bytes readonly=1048576 \
  --storage-max-operations scratch=128 \
  --storage-max-read-bytes scratch=1048576 \
  --storage-max-write-bytes scratch=16 \
  >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

SERVER_READY=false
for _ in $(seq 1 120); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status >/dev/null; then
    SERVER_READY=true
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    tail -n 100 "$SERVER_LOG"
    break
  fi
  sleep 1
done
if [[ "$SERVER_READY" != true ]]; then
  echo "Real Workerd storage server is not ready; do not continue."
fi

bash tools/run-workerd-storage-policy-demo.sh all
```

The real-Workerd run must also report `PASS: 6`, and the server startup log
must show the `ExecutorInit` JSON object with `protocol_version: 2` and only
logical names/modes. Neither that object nor Worker responses may contain host
paths.

Storage and Worker-JS VFS are final approved proof. The optimized stripped
executor/rootfs SHA-256 is
`cbc6ac02d041d552e871a58c99a7c62e09969d6b8e53540692d7b03b89892bd3`.
The reviewed Workerd source is signed at commit
`1d7a127908d4aca098bdad2e0caf904b9f1ffe82`.
Its 42-file source manifest SHA-256 is
`4f2a3cba603e4f3a84747151380e6437f7f1d4aa761abf2f6277958d2e798029`,
and the tracked Workerd diff SHA-256 is
`86bb53721d00a739ff4cf1303ae3666bfa1b0814410e620851ea3c8f7693f658`.
The complete real-Hyperlight storage/VFS suite passes 18/18 with result
SHA-256
`67350ac547973f671bb8c51b5b085a2894b5c3fdacb71b281d89ebe40b818a3e`.
The same artifact passes the complete packaged-workload regression 25/25
with result SHA-256
`5406c1b30a80234e7b02b1eb0898b2129cc0e38ec6a623224b2252aeedf00447`.
Combined provenance SHA-256:
`a38a617d38a57e05c3a5959c346ebe9c7dff3025df62562dc8e051583dbcecd9`.

The evidence proves the `ExecutorInit` JSON object with `protocol_version: 2`,
sorted logical-only storage descriptors, and no host paths, together with VFS
bundle/tmp/dev behavior and named
read/RO/RW/traversal/unlisted behavior, fresh-request reset,
operation/stat/read/write `EDQUOT`, Node `EDQUOT`, and Web
`QuotaExceededError`. Generic `EDQUOT` propagation is compatibility-preserving
default-adapter infrastructure: it introduces no source/API/ABI break, no
required configuration, and no behavior change for existing backends.
TLS/BYOB and Node filesystem parity corrections are independent bug fixes
limited to proven incorrect, oracle-covered behavior. No unrelated Workerd
semantics, APIs, ABIs, or defaults are changed. The final source query
discovers 412 configured Node tests; three variants requiring
`cloudflare.com:80` are excluded, and all 409 selected tests pass in 211.07
seconds with no failures or skips.

Keep the two interfaces distinct in evidence and design reviews. The Rust
launcher serializes the Workerd-specific `ExecutorInit` JSON object with
`protocol_version: 2` and passes it as the argument to the Hyperlight `init`
guest call. That object carries the Worker bundle, modules, and sorted
named-storage descriptors. Individual storage operations instead invoke the
registered Hyperlight host functions `fs_stat`, `fs_read_bytes`,
`fs_write_bytes`, and the other `fs_*` functions. Those host-function calls are
not `ExecutorInit` fields or messages.

Post-qualification, evaluate a Rust guest refactor without changing the proven
contract: retain only a minimal C++ Workerd object bridge and move substantive
guest parsing, wrappers, and state into the existing Workerd Rust+cxx
infrastructure. Require complete behavioral and compatibility checks, the
configured and broader Node test selections, storage/VFS 18/18, and
packaged-workload 25/25 requalification before adopting that refactor.

```bash
if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
fi
wait "${SERVER_PID:-}" 2>/dev/null || true
SERVER_PID=
```

### Complete Node suite and packaged workload acceptance

The standalone Node compatibility gate is the complete upstream suite at the
exact pinned Workerd revision, not a selected route list:

```bash
bazel test //src/workerd/api/node/tests/... --test_output=errors
```

Record the exact total, pass, fail, and skip counts. Exclude only tests that
upstream already skips or that the output proves are platform-inapplicable,
and record every exclusion with its exact upstream/platform reason.

For the final source, the configured query discovers 412 tests. Three variants
whose upstream oracle requires `cloudflare.com:80` are explicitly excluded.
All 409 selected tests execute and pass in 211.07 seconds, with zero failures
and zero skips.

The broader query discovers 522 generated targets and excludes these four
hosted-network variants because each executes the HTTP fixture that opens a
connection to external `cloudflare.com:80`:

- `//src/workerd/api/node/tests:http-nodejs-test@`
- `//src/workerd/api/node/tests:http-nodejs-test@all-autogates`
- `//src/workerd/api/node/tests:http-nodejs-test@all-compat-flags`
- `//src/workerd/api/node/tests:http-nodejs-test@gc-stress`

The `@eslint` variant remains selected because it performs static linting
rather than the hosted network operation. The initial 518-target run under
48-way contention completes in 293.700305743 seconds with 498 passes, 20
timeouts, zero skips, and no assertion or runtime failures. Its
`initial.bep.json` SHA-256 is
`5f54d35a1654a8d9551013a8d4510f57501e258dd6b538629a4235d29e14993a`.
Exact individual uncached retries complete in 75.724202257 seconds and pass
all 20 timed-out targets, proving 518/518 selected targets. The
`retry.bep.json` SHA-256 is
`7023fd459306f8c6c970a9fd2aa2c887c5a035a56dda2d3ed1ab8e492df44bbc`.
Total execution time across the initial run and retries is 369.424508 seconds
(6 minutes 9.42 seconds).

The same leased Azure host also executes the repository-wide deterministic
non-Node selection without promoting a partial result. It selects 2,138
targets, executes 2,128, passes 2,044, fails 94, skips zero, and leaves 10 not
executed. Exact retries preserve all 94 failures; none are waived. The initial
phase takes 5,301.316543806 seconds, retries take 3,622.221737482 seconds, and
combined execution takes 8,923.538281288 seconds. The result SHA-256 is
`2e8536306a807b319ee8003ca1da183f781933028819399ae6442a4c984d5357`;
the initial and retry BEP SHA-256 values are
`1838cb48edf173ba7974e8423c9358768cad6027c06c1bd3eeb796e1267bd162`
and
`7ca89ac012bc4d5dbfe613743cb4d349fea009d2cdee19fada733c9552b769c4`.
The 104 manifest records cover 94 unique labels because the 10 not-executed
labels are already included among the failures. Classification is:

- 80 source regressions: 42 streams conformance/oracle, eight BYOB
  pending-read, four BYOB tee invalidation/error-type, 10 GC-stress-only
  runtime/assertion, six Encoding WPT, six C++ compile, three other
  deterministic runtime/oracle, and one ESLint;
- nine bounded timeouts: five WPT at 1,800 seconds and four container-client
  variants at 3,600 seconds;
- four selection/query artifacts for requested host-platform-incompatible
  targets; and
- one external container dependency where container-shutdown lacks
  `cloudflare/proxy-everything:main`.

The 10 linked non-executions are six source compile failures and four platform
incompatibilities. There are zero environment, tooling, or setup failures.
The classification JSON SHA-256 is
`0f76a3561a37d18cf6f0694ec0142b8072ca8daf780f6350e518e678f607f262`;
the CSV SHA-256 is
`e9c00a8acf64a5a315866261b463cbc482145788d7701107dee5ea0dc7e919ff`.
All 80 source labels require code or oracle fixes, and all 94 affected labels
require an approved rerun after remediation.

The approved remediation plan is intentionally separate from the four frozen
commits. Its SHA-256 is
`476982376c33e2943009f37f6009dcaf58fbb873e86b940cbcd76b3279b0ebd1`.
None of the 94 failures belongs directly to the approved EDQUOT, Node
TLS/BYOB adapter, Node filesystem, or executor commit boundaries. Four compile
failures belong to explicitly excluded logical-broker/sandbox-state work. The
remaining source work requires new approval boundaries: 54 Workerd
streams/BYOB labels, 10 GC-stress labels, six Encoding WPT labels, and six
benchmark/runtime/lint-maintenance labels. The other 14 labels are non-source:
nine WPT/container timeouts, four platform-selection artifacts, and one
missing external image. Keep the four approved commits frozen and use the
separate remediation plan for any later fixes and reruns.

The explicit-allowlist archive is 5,779,921 bytes with SHA-256
`89965d622c9b6cfd8fe410a25b0192032d8c509ad75863a9a54edff60551ebcb`.
Independent extraction verifies all 134 payload files with zero errors. The
dedicated `workerd-node-522-20261002-cf07-rg` group is deleted, `az group
exists` becomes `false` at 2026-10-02T22:02:37Z, and subscription-wide tagged
and matching group/resource queries return empty arrays. The cleanup proof
SHA-256 is
`1d4c31f721c4064940abf9886bf14bdce4de719bec532eaf1dfb711c1b8231ef`.
The machine-readable dossier containing these source, test, classification,
archive, cleanup, and validation identities is
`docs/workerd-qualification-evidence.json`.

The exact three previously failing `tls-nodejs-test` variants pass after
`src/node/internal/streams_readable.js` uses a high-water-mark-only strategy
for `createTypeBytes`.

The representative real-Hyperlight workloads use immutable package metadata
and assertions checked into:

```bash
PINS=examples/workerd-executor/workerd-node-workload-pins.json
ACCEPTANCE=examples/workerd-executor/workerd-node-workload-acceptance.json

jq -e '
  .schema_version == 1 and
  ([.packages[].name] | sort | unique | length) == 5 and
  all(.packages[];
    (.version | length) > 0 and
    (.integrity | startswith("sha512-")) and
    (.sha256 | test("^[0-9a-f]{64}$")))
' "$PINS"

jq -e '
  .schema_version == 1 and
  .global_requirements.fresh_vm_per_request == true and
  .global_requirements.raw_network_access == false and
  (.workloads | length) == 5 and
  all(.workloads[]; (.assertions | length) == 5)
' "$ACCEPTANCE"

sha256sum "$PINS" "$ACCEPTANCE"
```

The canonical timestamp-free pins manifest SHA-256 is
`12d2f36b7bc3280302abcec19cfa7f206361d4cbf594e0c52b8d2cf562aca819`.
The five mirrored tarballs were independently matched byte-for-byte to their
published npm `dist.shasum` values; retain the checked-in SRI and SHA-256
values as the package provenance record.

The five workloads are `graceful-fs`, deterministic streamed ZIP creation,
streams/zlib/crypto with backpressure, CommonJS/ESM package resolution, and
`node:http`/TLS through explicit host policy. Each run must use the packaged
executor inside a real one-request Hyperlight VM and record the revisions,
executor/bundle/manifest hashes, command, exit status, response, wall time,
and peak memory named in the acceptance manifest. Run the stateful workloads
twice to prove the second fresh VM cannot observe the first VM's `/tmp`.

#### Measured final workload timings

These are six qualification requests, not benchmark statistics. Fresh-VM
request latency starts immediately before the client sends the request, after
`workerd-demo` readiness and snapshot creation, and ends after the complete
response body is decoded. It includes fresh VM restore, per-request setup,
Worker execution, registered host-function activity, teardown, HTTP response,
and response-body reading. It excludes process launch, initial executor and
Workerd initialization, snapshot creation, readiness polling, and final process
termination.

Host process peak resident memory is Linux `VmHWM` for the top-level
`workerd-demo` process over its complete lifetime. It includes the Rust host
runtime and VM mappings. It is not guest-only memory, scratch allocation,
cgroup memory, or total host memory.

| Workload | Timed requests | Fresh-VM request latency | Host process peak resident memory | What passing proves |
|---|---:|---|---|---|
| `graceful-fs` | 2 | 107.252 ms; 100.777 ms | 415,956,992 bytes (396.7 MiB), covering both requests | The packaged module loads, `/bundle` is read-only, `/tmp` works, and the second fresh VM cannot see the first VM's `/tmp` data. |
| Streamed ZIP creation | 1 | 125.211 ms | 416,407,552 bytes (397.1 MiB) | The archive has the exact expected names, bytes, CRC values, and SHA-256 values verified outside the guest. |
| Streams, zlib, and crypto | 1 | 167.146 ms | 413,474,816 bytes (394.3 MiB) | All packaged stream, compression, backpressure, and cryptographic assertions pass. |
| CommonJS and ES modules | 1 | 96.359 ms | 416,907,264 bytes (397.6 MiB) | The registered packaged dependency graph resolves and executes the expected result. |
| HTTP and TLS policy | 1 | 751.406 ms | 413,913,088 bytes (394.7 MiB) | Allow, deny, redirect, response-size, and intentional 500 ms timeout checks pass without ambient guest networking. |

The authoritative schema-version-3 result JSON is identified by SHA-256
`5406c1b30a80234e7b02b1eb0898b2129cc0e38ec6a623224b2252aeedf00447`.
Keep the raw JSON with the sealed evidence archive so each number can be
independently recalculated.

For HTTP/TLS, allow only the deterministic test upstream, disable automatic
redirects, record request/response/concurrency/timeout limits, and grant no raw
TCP or UDP access. An allowed request must pass; unlisted host/port, denied
redirect target, timeout, and oversized response must fail with the documented
policy error. Store detailed request/response and hash artifacts beside the
other evidence. Do not mark packaged workloads proven until these real-KVM
assertions pass.

The initial fastbuild executor/rootfs with SHA-256 beginning `a843` is
explicitly rejected. Its ELF `PT_LOAD` span was approximately 128.76 MiB,
exceeding the 128 MiB loader gate; real-KVM boot failed before Worker
initialization, ending with diagnostic binding `ENOMEM`. Zero workload
assertions ran, so this attempt is artifact-preflight evidence only and must
not appear as workload execution. Build an optimized stripped executor, run
its verifier and self-test, repackage the rootfs, verify byte identity and
load-span limits, then start again with the `graceful-fs` fresh on-demand VM.

The optimized recovery artifacts passed verifier, self-test, static-PIE, and
load-span checks and reached all five workloads in real KVM. The first
intermediate result was 13/25 because dependency `.js` entries were serialized
as `text`, causing `createRequire()` to return strings instead of CommonJS
exports. After adding `commonJsModule`, the frozen third KVM batch reached
15/25: `graceful-fs`, `streams-zlib-crypto`, and `node-http-tls` passed 5/5,
while ZIP and semver/UUID failed during Worker initialization because
extensionless relative and bare-package CommonJS lookup were incomplete.

The resolver recovery is deliberately bounded to registered bundle modules and
their `package.json` maps. It canonicalizes valid extensionless relative and
bare-package edges to explicit registered specifiers, preserves Workerd's CJS
caching/cycles/errors, and rejects traversal or unregistered relatives.
Focused self-tests cover semver's `../classes/range`, yazl's bare
`buffer-crc32`, exports, traversal denial, and missing-module denial. The
optimized resolver artifact was repackaged and the targeted CommonJS recovery
passed 15/15 with result SHA-256
`960e73057a37fc86a3bfdb0e0e938d7ffccc29e1c9bb7d61791da9700211c58c`.
The final complete five-workload regression passes all 25 assertions in fresh
one-request real-KVM VMs. Result SHA-256
`5406c1b30a80234e7b02b1eb0898b2129cc0e38ec6a623224b2252aeedf00447`
identifies the exact workload outcomes. Executor/rootfs SHA-256
`cbc6ac02d041d552e871a58c99a7c62e09969d6b8e53540692d7b03b89892bd3`
identifies the exact guest binary bytes used for every workload. Source
manifest SHA-256
`4f2a3cba603e4f3a84747151380e6437f7f1d4aa761abf2f6277958d2e798029`
binds the 42 source files used to build those bytes, and tracked-diff SHA-256
`86bb53721d00a739ff4cf1303ae3666bfa1b0814410e620851ea3c8f7693f658`
binds the reviewed Workerd changes. The same artifact also passes the final
storage/VFS 18/18 suite recorded above.

## 8. Run a simple parallel `hey` benchmark

Use the 9,441-byte `/sync` response from
`examples/workerd-bundles/workerd-pool-benchmark.json`.

Start a fresh on-demand server:

```bash
cd "$HOME/src/hyperlight-unikraft"
mkdir -p "$HOME/results/hey-on-demand"

if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
fi
SERVER_PID=
SERVER_LOG="$HOME/results/hey-on-demand/server.log"

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
  >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

SERVER_READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status \
    >"$HOME/results/hey-on-demand/startup-status.json"; then
    SERVER_READY=true
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    echo "Benchmark server stopped before readiness. Recent log output:"
    tail -n 100 "$SERVER_LOG"
    break
  fi
  sleep 1
done
if [[ "$SERVER_READY" != true ]]; then
  echo "Benchmark server is not ready; do not continue this section."
fi
```

Verify response identity before load:

```bash
printf '=== response identity ===\n'
printf '%s\n' \
  'The exact 9,441-byte size is the correctness gate; SHA-256 records this response identity for later comparisons.'

curl --fail-with-body -sS \
  http://127.0.0.1:8787/sync \
  -o "$HOME/results/hey-on-demand/sync.bin"

test "$(wc -c < "$HOME/results/hey-on-demand/sync.bin")" -eq 9441
sha256sum "$HOME/results/hey-on-demand/sync.bin"
```

Run a single request, one exact 32-client wave, and a 60-second sustained run:

```bash
run_hey() {
  label="$1"
  description="$2"
  output="$3"
  shift 3
  endpoint="http://127.0.0.1:8787/sync"

  printf '=== %s ===\n%s\n' "$label" "$description"
  printf 'Endpoint: %s\nLoad: hey %s\n' "$endpoint" "$*"
  hey "$@" "$endpoint" | tee "$output"
  statuses=("${PIPESTATUS[@]}")
  if (( statuses[0] == 0 && statuses[1] == 0 )); then
    printf 'PASS %s\n\n' "$label"
    return 0
  fi
  printf 'FAIL %s: hey=%s tee=%s\n\n' \
    "$label" "${statuses[0]}" "${statuses[1]}" >&2
  if (( statuses[0] != 0 )); then
    return "${statuses[0]}"
  fi
  return "${statuses[1]}"
}

run_hey \
  single \
  'One-request smoke test proving the end-to-end request/VM path and expected HTTP 200, 9,441-byte response.' \
  "$HOME/results/hey-on-demand/single.txt" \
  -n 1 -c 1

run_hey \
  wave-c32 \
  'Finite 320-request concurrency-32 wave matching configured sandbox parallelism and showing completion and latency distribution.' \
  "$HOME/results/hey-on-demand/wave-c32.txt" \
  -n 320 -c 32

run_hey \
  sustained-c32 \
  '60-second steady-state run at configured max concurrency; the primary throughput and latency baseline.' \
  "$HOME/results/hey-on-demand/sustained-c32.txt" \
  -z 60s -c 32

run_hey \
  sustained-c64 \
  '60-second pressure run at twice configured max concurrency, exercising queue/backpressure for comparison with c32.' \
  "$HOME/results/hey-on-demand/sustained-c64.txt" \
  -z 60s -c 64
```

Interpret `Requests/sec` as achieved throughput. `Average` and `Slowest`
summarize client-observed end-to-end latency, while the latency distribution
shows its percentiles. `Status code distribution` must contain only HTTP 200,
and the error distribution must be absent or zero. Compare c64 with c32 to see
how queue pressure changes throughput, latency, and errors. The server must
remain alive and return to quiescence after load.

Inspect status and stop cleanly:

```bash
curl --fail-with-body -sS \
  http://127.0.0.1:8787/__hyperlight/pool-status \
  | tee "$HOME/results/hey-on-demand/final-status.json" \
  | jq .

kill -TERM "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=
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

if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
fi
SERVER_PID=
SERVER_LOG="$HOME/results/hey-adaptive-o48-r1/server.log"

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
  >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

SERVER_READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status \
    | tee "$HOME/results/hey-adaptive-o48-r1/startup-status.json" \
    | jq -e '
      .restore_mode == "prewarmed" and
      .prewarmed_inventory == .prewarmed_ready and
      .prewarmed_ready >= .warm_floor and
      .prewarmed_replenishing == 0
    ' >/dev/null; then
    SERVER_READY=true
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    echo "Adaptive server stopped before readiness. Recent log output:"
    tail -n 100 "$SERVER_LOG"
    break
  fi
  sleep 1
done
if [[ "$SERVER_READY" != true ]]; then
  echo "Adaptive server is not ready; do not continue this section."
fi
```

Run the same matched load:

```bash
run_hey \
  adaptive-wave-c32 \
  'Finite 320-request concurrency-32 wave matched to the on-demand wave; compare completion and latency distribution.' \
  "$HOME/results/hey-adaptive-o48-r1/wave-c32.txt" \
  -n 320 -c 32

run_hey \
  adaptive-sustained-c32 \
  '60-second adaptive steady-state row matched to the primary on-demand c32 throughput and latency baseline.' \
  "$HOME/results/hey-adaptive-o48-r1/sustained-c32.txt" \
  -z 60s -c 32

ADAPTIVE_QUIESCENT=false
for _ in $(seq 1 9000); do
  if curl --fail-with-body -sS \
    http://127.0.0.1:8787/__hyperlight/pool-status \
    >"$HOME/results/hey-adaptive-o48-r1/final-status.json" \
    && jq -e '
      .admitted == 0 and .active == 0 and .queued == 0 and
      .execution_slots_in_use == 0 and .restore_slots_in_use == 0 and
      .recycle_queue_depth == 0 and .teardown_in_flight == 0 and
      .completion_queue_depth == 0 and .completion_in_flight == 0 and
      .restore_permits_outstanding == 0 and
      .prewarmed_inventory == .prewarmed_ready and
      .prewarmed_ready >= .warm_floor and
      .prewarmed_replenishing == 0 and
      (.refill_active | not) and (.replenishment_paused | not)
    ' "$HOME/results/hey-adaptive-o48-r1/final-status.json" >/dev/null; then
    ADAPTIVE_QUIESCENT=true
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    tail -n 100 "$SERVER_LOG"
    break
  fi
  sleep 0.01
done

jq '{
      ready: .prewarmed_ready,
      inventory: .prewarmed_inventory,
      warm_floor,
      replenishing: .prewarmed_replenishing,
      refill_active,
      replenishment_paused,
      restore_permits_outstanding,
      admitted,
      active,
      queued,
      execution_slots_in_use,
      restore_slots_in_use,
      recycle_queue_depth,
      teardown_in_flight,
      completion_queue_depth,
      completion_in_flight
    }' "$HOME/results/hey-adaptive-o48-r1/final-status.json"

if [[ "$ADAPTIVE_QUIESCENT" == true ]]; then
  echo 'PASS adaptive quiescence/readiness'
else
  echo 'FAIL adaptive quiescence/readiness' >&2
fi

kill -TERM "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=
```

The warm floor is fully restored but non-dispatchable. Stopping the server
destroys the final warm VM. Replenishment begins below the low watermark and
refills toward the high watermark in bounded batches.

Run both modes with matched settings. Concurrent replacement restore can
increase guest execution time, so compare throughput and phase timing rather
than ready misses alone. Compare `Requests/sec`, `Average`, `Slowest`, tail
latency, status codes, and errors between each adaptive row and its on-demand
counterpart.

### Exact no-refill diagnostic wave

Start 64 owners, prefill all of them, and suppress normal replacement restore
until one exact 32-request wave completes:

```bash
mkdir -p "$HOME/results/hey-no-refill-o64"

if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
  kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
fi
SERVER_PID=
SERVER_LOG="$HOME/results/hey-no-refill-o64/server.log"

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
  >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

SERVER_READY=false
for _ in $(seq 1 600); do
  if curl --silent --fail \
    http://127.0.0.1:8787/__hyperlight/pool-status \
    | tee "$HOME/results/hey-no-refill-o64/startup-status.json" \
    | jq -e '.prewarmed_ready == 64' >/dev/null; then
    SERVER_READY=true
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    echo "No-refill server stopped before readiness. Recent log output:"
    tail -n 100 "$SERVER_LOG"
    break
  fi
  sleep 1
done
if [[ "$SERVER_READY" != true ]]; then
  echo "No-refill server is not ready; do not continue this section."
fi

printf '=== no-refill-wave-c32 ===\n'
printf '%s\n' \
  'Prefilling 64 ready owners and dispatching exactly 32 concurrent requests leaves 32 ready owners, isolating guest execution without concurrent replacement refill.'
printf 'Endpoint: http://127.0.0.1:8787/sync\n'
printf 'Load: hey -n 32 -c 32\n'
hey -n 32 -c 32 \
  http://127.0.0.1:8787/sync \
  | tee "$HOME/results/hey-no-refill-o64/wave-c32.txt"
statuses=("${PIPESTATUS[@]}")
if (( statuses[0] == 0 && statuses[1] == 0 )); then
  printf 'PASS no-refill-wave-c32\n\n'
else
  printf 'FAIL no-refill-wave-c32: hey=%s tee=%s\n\n' \
    "${statuses[0]}" "${statuses[1]}" >&2
fi

NO_REFILL_COMPLETE=false
for _ in $(seq 1 9000); do
  if curl --fail-with-body -sS \
    http://127.0.0.1:8787/__hyperlight/pool-status \
    >"$HOME/results/hey-no-refill-o64/post-wave-status.json" \
    && jq -e '
      .diagnostic_wave_dispatched == 32 and
      .diagnostic_wave_completed == 32 and
      (.replenishment_paused | not) and
      (.refill_active | not) and
      .restore_permits_outstanding == 0 and
      .prewarmed_inventory == 32 and
      .prewarmed_ready == 32 and
      .prewarmed_replenishing == 0
    ' "$HOME/results/hey-no-refill-o64/post-wave-status.json" >/dev/null; then
    NO_REFILL_COMPLETE=true
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    wait "$SERVER_PID" 2>/dev/null || true
    tail -n 100 "$SERVER_LOG"
    break
  fi
  sleep 0.01
done

jq '{
      diagnostic_wave_dispatched,
      diagnostic_wave_completed,
      replenishment_paused,
      replenishment_pause_reason,
      refill_active,
      restore_permits_outstanding,
      warm_floor,
      prewarmed_inventory,
      prewarmed_ready,
      prewarmed_replenishing
    }' "$HOME/results/hey-no-refill-o64/post-wave-status.json"

if [[ "$NO_REFILL_COMPLETE" == true ]]; then
  echo 'PASS no-refill diagnostic invariants'
else
  echo 'FAIL no-refill diagnostic invariants' >&2
fi
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
kill -TERM "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=
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
Print matched summaries:

```bash
for result in \
  "$HOME/results/wrapper-on-demand/wintertc-pool-performance.json" \
  "$HOME/results/wrapper-adaptive-o48-r1/wintertc-pool-performance.json"
do
  printf '=== %s ===\n' "$(dirname "$result")"
  jq '{
      accepted,
      stretch_target_met,
      restore_mode: .configuration.restore_mode,
      owners: .configuration.owner_count,
      effective_concurrency: .configuration.effective_concurrency,
      baseline: {
        requests: .baseline_run.requests,
        errors: .baseline_run.errors,
        requests_per_second: .baseline_run.throughput_requests_per_second,
        latency_ms: {
          average: .baseline_run.latency_ms.average,
          slowest: .baseline_run.latency_ms.maximum,
          p95: .baseline_run.latency_ms.p95,
          p99: .baseline_run.latency_ms.p99
        }
      },
      sustained: [
        .sustained_runs[] |
        {
          label,
          requests,
          errors,
          requests_per_second: .throughput_requests_per_second,
          latency_ms: {
            average: .latency_ms.average,
            slowest: .latency_ms.maximum,
            p95: .latency_ms.p95,
            p99: .latency_ms.p99
          }
        }
      ],
      refill: {
        seconds: .refill.seconds,
        passed: .refill.passed
      }
    }' "$result"
  printf '\n'
done
```

The on-demand and adaptive rows provide matched throughput, latency, error,
and refill/recovery comparisons. Detailed pool samples and the complete server
log remain in each wrapper output directory.

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

Print the focused phase and profiler summary for each row:

```bash
for result in \
  "$HOME/results/profile-on-demand/result.json" \
  "$HOME/results/profile-no-refill-o64/result.json" \
  "$HOME/results/profile-adaptive-o48-r1/result.json"
do
  printf '=== %s ===\n' "$(dirname "$result")"
  jq '{
      source_commit: .configuration.source_commit,
      restore_mode: .initial_status.restore_mode,
      requests,
      errors,
      status_codes,
      requests_per_second: .throughput_requests_per_second,
      latency_ms: {
        p50: .latency_ms.p50,
        p95: .latency_ms.p95,
        p99: .latency_ms.p99
      },
      phases_ms: {
        admission_wait: {
          average: .phase_ms.admission_wait_ms.average,
          p95: .phase_ms.admission_wait_ms.p95
        },
        ready_owner_wait: {
          average: .phase_ms.ready_owner_wait_ms.average,
          p95: .phase_ms.ready_owner_wait_ms.p95
        },
        replenishment_policy_wait: {
          average: .phase_ms.replenishment_policy_wait_ms.average,
          p95: .phase_ms.replenishment_policy_wait_ms.p95
        },
        replenishment_restore: {
          average: .phase_ms.replenishment_restore_ms.average,
          p95: .phase_ms.replenishment_restore_ms.p95
        },
        guest_execution: {
          average: .phase_ms.guest_execution_ms.average,
          p95: .phase_ms.guest_execution_ms.p95
        },
        total: {
          average: .phase_ms.total_ms.average,
          p95: .phase_ms.total_ms.p95
        }
      },
      profile_count,
      sampling: {
        interval_ms: .sampling.interval_ms,
        samples: .sampling.samples,
        errors: .sampling.errors
      },
      profiler_limitations
    }' "$result"
  printf '\n'
done
```

These rows distinguish admission and ready-owner waiting from replacement
restore and guest execution. Full 10 ms telemetry, raw request CSV, server
logs, and supported perf outputs remain beside each `result.json`.

## 13. Stop application processes

Every foreground walkthrough section stops the process it starts, and the two
repository runners stop their child server before returning. Confirm that no
demo or loopback process remains:

```bash
pgrep -af 'target/release/examples/workerd-demo|results/upstream/server.py' || true
```

Copy the required result files off the VM, then delete the Azure resources
created for the VM using the same Azure workflow that created them.
