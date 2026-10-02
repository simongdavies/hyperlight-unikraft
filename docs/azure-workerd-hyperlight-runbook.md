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

Stop both processes:

```bash
kill -TERM "$SERVER_PID" "$UPSTREAM_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
wait "$UPSTREAM_PID" 2>/dev/null || true
SERVER_PID=
UPSTREAM_PID=
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
