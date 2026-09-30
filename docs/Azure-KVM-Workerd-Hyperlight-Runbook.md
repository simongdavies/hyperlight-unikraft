# Run Workerd in Hyperlight-Unikraft on Azure KVM

This runbook builds the real Workerd/JSG/V8 executor, packages it for
Hyperlight-Unikraft, runs it with KVM, and applies concurrent load.

## 1. Azure VM requirements

- Use a native x86-64 Linux VM with nested virtualization.
- The user running the demo must have write permission to `/dev/kvm`.
- Keep the repositories and build outputs on the VM's native ext4 disk.
- Do not use Azure Files, SMB, or a Windows-backed mount.

If required, add the current user to the `kvm` group:

```bash
sudo usermod -aG kvm "$(whoami)"
```

Reconnect the SSH session after changing group membership.

## 2. Install prerequisites

Docker CE is already installed if `docker --version` succeeds. Do not install
Ubuntu's `docker.io` package over an existing Docker CE/containerd.io
installation.

```bash
docker --version
```

Docker is used only to build the Workerd executor with its pinned LLVM 22 and
Bazelisk toolchain. It is not used by the Hyperlight/KVM runtime.

If Docker reports permission denied for `/var/run/docker.sock`, run:

```bash
sudo usermod -aG docker "$(whoami)"
newgrp docker
docker ps
```

Install the remaining prerequisites:

```bash
sudo apt-get update
sudo apt-get install -y \
  git \
  curl \
  build-essential \
  pkg-config \
  python3 \
  cpio \
  rsync \
  jq \
  golang-go
```

Install Rust if it is not already available:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```

Install `just`:

```bash
cargo install just
```

## 3. Clone the pushed branches

```bash
mkdir -p "$HOME/src" "$HOME/artifacts"
cd "$HOME/src"

git clone --recurse-submodules \
  --branch simongdavies-workerd-isolate-boundary \
  https://github.com/simongdavies/workerd.git

git clone --recurse-submodules \
  --branch simongdavies-parallel-workerd-sandboxes \
  https://github.com/simongdavies/hyperlight-unikraft.git
```

## 4. Build the Workerd executor

The Workerd development container supplies LLVM 22 and Bazelisk.

```bash
cd "$HOME/src/workerd"
```

The current Microsoft Node dev-container base resolves to Debian Trixie.
Replace the obsolete package list and make host tools use the same LLVM 22
libc++ archive as target binaries:

```bash
cat > .devcontainer/Dockerfile <<'EOF'
FROM mcr.microsoft.com/vscode/devcontainers/javascript-node:26

# Install dependencies, including clang through the LLVM APT repository.
ARG LLVM_VERSION=22
RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get -y install --no-install-recommends tcl \
    && curl -fSsL -o /tmp/llvm.sh https://apt.llvm.org/llvm.sh \
    && bash /tmp/llvm.sh ${LLVM_VERSION} \
    && apt-get -y install --no-install-recommends libunwind-${LLVM_VERSION}-dev libc++-${LLVM_VERSION}-dev libc++abi-${LLVM_VERSION}-dev libclang-rt-${LLVM_VERSION}-dev -o DPkg::options::="--force-overwrite" \
    && rm -f /tmp/llvm.sh \
    && rm -rf /var/lib/apt/lists/*
ENV PATH="/usr/lib/llvm-${LLVM_VERSION}/bin:${PATH}"

# Install Bazel (via Bazelisk)
RUN npm install -g @bazel/bazelisk

# Install Just
RUN npm install -g rust-just
EOF

sed -i "s/--host_linkopt='-lc++' --host_linkopt='-lm'/--host_linkopt='-l:libc++.a' --host_linkopt='-lm'/" .bazelrc
```

Build, self-test, strip, and inspect the executor:

```bash
docker build -t workerd-dev -f .devcontainer/Dockerfile .devcontainer
mkdir -p "$HOME/.cache/workerd-bazel" "$HOME/workerd-artifacts"

docker run --rm \
  -v "$PWD:/workspace" \
  -v "$HOME/.cache/workerd-bazel:/root/.cache/bazel" \
  -v "$HOME/workerd-artifacts:/artifacts" \
  -w /workspace \
  workerd-dev \
  bash -lc '
    set -eux
    bazel build //src/workerd/server:workerd-sandbox-executor \
      --//:io_backend=cxx \
      --workspace_status_command=/bin/true \
      --jobs="$(nproc)" \
      --repo_env=CC=/usr/lib/llvm-22/bin/clang \
      --repo_env=AR=/usr/lib/llvm-22/bin/llvm-ar \
      --linkopt=--ld-path=/usr/lib/llvm-22/bin/ld.lld \
      --host_linkopt=--ld-path=/usr/lib/llvm-22/bin/ld.lld

    executor=bazel-bin/src/workerd/server/workerd-sandbox-executor
    "$executor" --self-test
    /usr/lib/llvm-22/bin/llvm-strip "$executor"
    file "$executor"
    if readelf -l "$executor" | grep -q INTERP; then
      echo "unexpected dynamic interpreter" >&2
      exit 1
    fi
    ldd "$executor" || true
    cp "$executor" /artifacts/workerd-sandbox-executor
    chmod 0755 /artifacts/workerd-sandbox-executor
    sha256sum /artifacts/workerd-sandbox-executor
  '

file "$HOME/workerd-artifacts/workerd-sandbox-executor"
sha256sum "$HOME/workerd-artifacts/workerd-sandbox-executor"
```

Expected properties:

- x86-64 stripped static PIE
- no `PT_INTERP`
- `ldd` reports `statically linked`
- `--self-test` passes

## 5. Package the executor for Hyperlight-Unikraft

```bash
cd "$HOME/src/hyperlight-unikraft"
git submodule update --init --recursive

export RUSTUP_HOME="$HOME/.rustup"
export CARGO_HOME="$HOME/.cargo"
export CARGO_TARGET_DIR="$HOME/.cache/hluk-v014-target"
mkdir -p "$RUSTUP_HOME" "$CARGO_HOME" "$CARGO_TARGET_DIR"
rustup toolchain install 1.98.0 --profile minimal

just build-workerd-kernel

bash examples/workerd-executor/build-rootfs.sh \
  "$HOME/workerd-artifacts/workerd-sandbox-executor"
```

Record artifact hashes:

```bash
sha256sum \
  kernel/workerd_hyperlight-x86_64 \
  build-elfloader/workerd-executor/executor \
  build-elfloader/workerd-executor/rootfs.img
```

For this direct-initrd static-PIE layout, `executor` and `rootfs.img` should
have the same SHA-256.

## 6. Run the real Workerd KVM probe

```bash
RUST_LOG=hyperlight_unikraft=debug \
cargo run --release --locked \
  --example workerd-bundle-probe -- \
  examples/workerd-bundles/helloworld_esm.json \
  https://example.test/ \
  512
```

```bash
cargo run --release --locked \
  --example workerd-bundle-probe -- \
  examples/workerd-bundles/web-streams.json \
  https://example.test/sync \
  512
```

The `workerd_sandbox` integration test uses a separate GCC-built mock executor;
run it only after the real Workerd probe succeeds.

```bash
cd "$HOME/src/hyperlight-unikraft"
export RUSTUP_HOME="$HOME/.rustup"
export CARGO_HOME="$HOME/.cargo"
export CARGO_TARGET_DIR="$HOME/.cache/hluk-v014-target"
just workerd-api-probe 512
```

Interpretation:

- HTTP `200` means the real Workerd Worker completed successfully.
- Every API field whose value is `true` passed the smoke test: URL,
  URLPattern, Request, Response, Headers, FormData, Blob, text codecs,
  SHA-256, secure random bytes, readable/transform streams, gzip
  compression/decompression, and performance timing.
- WebAssembly reached the runtime but is blocked by the executor embedder
  policy.
- The executor declares its timer channel disabled, but this smoke does not
  schedule or await a timer, so that declaration is not a runtime behavior
  test.
- The executor declares outbound fetch, WebSocket, raw connect, bindings, and
  actors unavailable because the Hyperlight sandbox grants none of those host
  capabilities. This smoke does not attempt those operations; prove denial
  separately with bounded negative tests before treating the declarations as
  enforcement evidence.
- This is a minimum-common API smoke test, not WinterTC conformance.

Run the timeout and recovery probe:

```bash
RUST_LOG=hyperlight_unikraft=debug \
cargo run --release --locked \
  --example workerd-memory-probe -- \
  512 \
  --bundle examples/workerd-bundles/acceptance.json \
  2>build-elfloader/workerd-memory-512-azure.stderr \
  | tee build-elfloader/workerd-memory-512-azure.json
```

## 7. Start the parallel HTTP demo

Example for your 32-vCPU machine:

```bash
RUST_LOG=info \
cargo run --release --locked \
  --example workerd-demo -- \
  --bundle examples/workerd-bundles/acceptance.json \
  --bind 0.0.0.0:8787 \
  --scratch-mb 512 \
  --request-timeout-ms 500 \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 2048 \
  2>build-elfloader/workerd-demo-profiles.jsonl
```

Behavior with these settings:

- The Worker is initialized and snapshotted once when the server starts.
- Up to 32 requests execute simultaneously.
- Every request executes in a separate VM restored from that one immutable
  initialized snapshot; the server does not create a new snapshot per request.
- The next 2048 requests wait in the FIFO queue.
- Further admissions receive HTTP 503.
- A request VM is never reused.
- VM creation, `KVM_RUN`, response handling, teardown, and destruction stay
  on one fixed owning thread.

Tune `--max-concurrent-sandboxes` for the machine's CPU and memory. The value
`4` is only the program's conservative default, not a technical limit.

Restoring on every request is the deliberately strongest isolation mode: guest
memory mutations, poisoned runtime state, and a timed-out/killed VM cannot
survive into the next request. A production optimization can keep a bounded
pool of VMs restored in advance and consume each VM for one request, replacing
it asynchronously afterward. That preserves one-request-per-VM isolation while
moving most restore cost off the request latency path, at the cost of reserved
memory. Reusing a VM for multiple requests is faster but is a weaker isolation
model and is not what this demo measures.

The implementation already loads the immutable snapshot once and shares its
backing data through memory mapping/copy-on-write rather than copying the full
guest memory image for every request. The measured restore phase still includes
creating a KVM VM and vCPU, registering guest memory and saved CPU state,
rebuilding host-function bindings, and servicing demand-page faults.

The safest next optimization is a **ready-VM pool**:

1. Each fixed KVM owner thread restores one VM before advertising itself as
   ready.
2. A request is dispatched only to a thread with a ready VM.
3. The VM serves exactly one request and is destroyed.
4. That same owner thread restores its replacement before rejoining the ready
   pool.

This removes restore from the latency of bursts up to the ready-pool size while
preserving vCPU thread ownership and one-request-per-VM isolation. Sustained
throughput still includes replenishment cost.

Background replenishment must have its own concurrency limit. Restoring a VM
creates KVM VM/vCPU state and consumes an owner thread, CPU time, memory, and
file descriptors. Starting 32 restores while 32 vCPUs are executing would
oversubscribe a 32-core host and can make latency worse. Budget active execution
and restore work together; for example, start A/B testing a 32-core machine
with 28 active request VMs and at most 4 concurrent restores. Idle owner threads
must block rather than spin. Measure runnable threads, CPU saturation, RSS,
restore latency, and request latency before increasing either limit.

Further experiments should measure prefaulting the snapshot's hot pages,
`madvise()` policies, huge pages, and KVM memory-slot setup separately; none
should be adopted without proving that snapshot pages remain immutable/shared
and request writes remain private copy-on-write pages.

## 8. Verify correctness and timeout recovery

From another terminal:

```bash
curl -i \
  -X POST \
  -H 'x-demo: azure' \
  -d 'hello' \
  http://127.0.0.1:8787/hello
```

Expected: HTTP 200 with `{"path":"/hello","method":"POST","count":1}`.

```bash
curl -i \
  -X POST \
  -d 'busy' \
  http://127.0.0.1:8787/busy
```

Expected: HTTP 504.

```bash
curl -i \
  -X POST \
  -d 'after' \
  http://127.0.0.1:8787/after
```

Expected: HTTP 200 with `{"path":"/after","method":"POST","count":1}`,
proving recovery with reset Workerd module state.

## 9. Install and run `hey`

```bash
go install github.com/rakyll/hey@v0.1.4
mkdir -p "$HOME/bin"
cat > "$HOME/bin/run-hey" <<'EOF'
#!/usr/bin/env bash
set -u
output="$(mktemp)"
"$HOME/go/bin/hey" "$@" >"$output" &
pid=$!
while kill -0 "$pid" 2>/dev/null; do
  printf '.'
  sleep 1
done
wait "$pid"
status=$?
printf '\n'
cat "$output"
rm -f "$output"
exit "$status"
EOF
chmod +x "$HOME/bin/run-hey"
export PATH="$HOME/bin:$HOME/go/bin:$PATH"
```

Single-request baseline:

```bash
run-hey \
  -n 100 \
  -c 1 \
  -m POST \
  -H 'Content-Type: text/plain' \
  -d 'load-body' \
  http://127.0.0.1:8787/hello
```

Run one client per active sandbox:

```bash
run-hey \
  -n 1000 \
  -c 32 \
  -m POST \
  -H 'Content-Type: text/plain' \
  -d 'load-body' \
  http://127.0.0.1:8787/hello
```

Exercise queueing:

```bash
run-hey \
  -n 1000 \
  -c 64 \
  -m POST \
  -H 'Content-Type: text/plain' \
  -d 'load-body' \
  http://127.0.0.1:8787/hello
```

Drive it harder:

```bash
run-hey \
  -n 1000 \
  -c 128 \
  -m POST \
  -H 'Content-Type: text/plain' \
  -d 'load-body' \
  http://127.0.0.1:8787/hello
```

Do not include `/busy` in ordinary throughput measurements. It deliberately
spins until the request timeout.

## 10. Test bounded overload explicitly

Restart the demo with a deliberately tiny active limit and queue:

```bash
RUST_LOG=info \
cargo run --release --locked \
  --example workerd-demo -- \
  --bundle examples/workerd-bundles/acceptance.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 512 \
  --request-timeout-ms 500 \
  --max-concurrent-sandboxes 1 \
  --queue-capacity 1
```

Then overload it:

```bash
hey \
  -n 20 \
  -c 20 \
  -m POST \
  -d 'busy' \
  http://127.0.0.1:8787/busy
```

One request runs, one queues, and excess admissions receive HTTP 503.

## 11. Run the multi-module streams Worker

This Worker loads two ES modules. The main module imports `streams-util`; for
each `/sync` request it generates 20 chunks of random lorem-style text, passes
them through a `TransformStream` that uppercases the text using
`TextDecoder`/`TextEncoder`, and returns the streamed response. Each request
still runs in a fresh Hyperlight VM, so this exercises module linking, V8,
ReadableStream, TransformStream, response streaming, restore, and teardown.

Restart the demo with:

```bash
RUST_LOG=info \
cargo run --release --locked \
  --example workerd-demo -- \
  --bundle examples/workerd-bundles/web-streams.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 512 \
  --request-timeout-ms 500 \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 2048
```

From the second terminal, capture and inspect one complete response:

```bash
curl -sS \
  -D "$HOME/streams-response.headers" \
  -o "$HOME/streams-response.body" \
  http://127.0.0.1:8787/sync

cat "$HOME/streams-response.headers"
head -c 2000 "$HOME/streams-response.body"
printf '\n\nResponse bytes: '
wc -c < "$HOME/streams-response.body"
```

Apply load:

```bash
run-hey \
  -n 320 \
  -c 32 \
  http://127.0.0.1:8787/sync
```

## 12. Prove Linux KVM VMs are being created

Count the Linux KVM API calls made by the running demo. This is a separate
evidence run: do not use its performance numbers because tracing adds overhead.

```bash
sudo apt-get update
sudo apt-get install -y strace

demo_pid="$(pgrep -n workerd-demo)"
rm -f "$HOME"/kvm-ioctl.*

sudo strace \
  -ff \
  -qq \
  -yy \
  -e trace=ioctl,close \
  -p "$demo_pid" \
  -o "$HOME/kvm-ioctl" &
trace_pid=$!

sleep 1
"$HOME/go/bin/hey" \
  -n 320 \
  -c 32 \
  http://127.0.0.1:8787/sync

sudo kill -INT "$trace_pid"
wait "$trace_pid" || true

vm_creates="$(grep -h -c 'KVM_CREATE_VM' "$HOME"/kvm-ioctl.* | awk '{ total += $1 } END { print total + 0 }')"
vcpu_creates="$(grep -h -c 'KVM_CREATE_VCPU' "$HOME"/kvm-ioctl.* | awk '{ total += $1 } END { print total + 0 }')"
vm_closes="$(grep -h -cE 'close\(.*kvm-vm' "$HOME"/kvm-ioctl.* | awk '{ total += $1 } END { print total + 0 }')"
vcpu_closes="$(grep -h -cE 'close\(.*kvm-vcpu' "$HOME"/kvm-ioctl.* | awk '{ total += $1 } END { print total + 0 }')"

printf '\nKVM proof\n'
printf '  VM creates:    %s\n' "$vm_creates"
printf '  vCPU creates:  %s\n' "$vcpu_creates"
printf '  VM closes:     %s\n' "$vm_closes"
printf '  vCPU closes:   %s\n' "$vcpu_closes"
```

For 320 successful requests, the expected result is 320 VM creates and 320
vCPU creates, followed by 320 VM closes and 320 vCPU closes. The
`kvm-ioctl.*` files are the raw kernel-call evidence.

## 13. What the evidence proves

Treat each claim separately and retain the evidence:

| Claim | Required evidence |
|---|---|
| The real Workerd fork runs in the guest | Executor `--self-test`, the real bundle probe, and the executor/kernel/rootfs hashes |
| Each successful request creates a KVM VM | `KVM_CREATE_VM` and `KVM_CREATE_VCPU` counts from Section 12 equal the successful request count for that traced run |
| Request VMs are destroyed | KVM VM and vCPU `close()` counts from Section 12 match the create counts for that traced run |
| Mutable Workerd module state does not persist between requests | A state-isolation Worker whose mutable module-level counter returns `1` for every sequential and concurrent request |
| A killed Worker does not poison later requests | `/busy` returns 504 and a later `/after` request returns 200 |
| Admission is bounded | The one-active/one-queued overload test produces HTTP 503 for excess requests |
| Host capabilities are absent | Negative tests for outbound fetch, WebSocket, raw connect, bindings, actors, host filesystem, and host sockets |
| Artifacts did not change between build and run | Recorded SHA-256 values for the executor, kernel, packaged executor, and rootfs |

The KVM trace count should match the number of successful requests. A mismatch,
request error, missing close, or reused state is a failed isolation run, not a
result to explain away.

The `hey` latency is client-observed end-to-end latency. It includes any queue
wait, fresh VM restore/creation, request setup, guest V8 execution, response
validation, VM teardown, and HTTP delivery. Break down the server-owned phases
from the demo profile log:

```bash
sed -n 's/^workerd request [^:]*: //p' \
  build-elfloader/workerd-demo-profiles.jsonl |
jq -s '
  def percentile($p):
    sort as $values |
    $values[((($values | length) - 1) * $p | floor)];
  def stats($field):
    map(.[$field]) |
    {
      average: (add / length),
      p50: percentile(0.50),
      p95: percentile(0.95),
      p99: percentile(0.99)
    };
  {
    requests: length,
    snapshot_restore_ms: stats("snapshot_restore_ms"),
    request_setup_ms: stats("request_setup_ms"),
    guest_execution_ms: stats("guest_execution_ms"),
    response_finish_ms: stats("response_finish_ms"),
    vm_teardown_ms: stats("vm_teardown_ms"),
    profiled_total_ms: stats("total_ms")
  }
'
```

`profiled_total_ms` excludes time waiting in the bounded queue and client/TCP
overhead. The difference between it and `hey` latency is therefore expected,
especially when client concurrency exceeds the active sandbox limit.

This evidence does **not** prove that Hyperlight, Unikraft, KVM, V8, or Workerd
is free of exploitable bugs. It proves the intended process/VM lifecycle and
capability configuration for this build. Defense-in-depth still requires
patching, host hardening, resource limits, artifact provenance, and adversarial
testing.

## 14. WinterTC / ECMA-429 status and path to conformance

The current `api-smoke.json` is a useful functional smoke test, but it is not a
WinterTC conformance suite. The authoritative 2025 Minimum Common Web API is
ECMA-429:

- <https://github.com/WinterTC55/proposal-minimum-common-api>
- <https://min-common-api.proposal.wintertc.org/>

The current Hyperlight executor profile is **not ECMA-429 conformant**:

- the executor declares timers disabled, but the smoke does not behavior-test
  timer scheduling or cancellation;
- the executor declares outbound `fetch()` unavailable, but the smoke does not
  behavior-test a denied fetch;
- WebAssembly is blocked by embedder policy;
- many required Event, MessageChannel, File, writable/BYOB stream, encoding
  stream, error-reporting, and global APIs are not yet tested through this
  executor;
- the hand-written smoke checks only selected successful paths and do not prove
  Web-standard behavior.

Move toward a defensible conformance claim in this order:

1. **Pin the target.** Record the exact ECMA-429 edition and a pinned commit of
   the specification and Web Platform Tests (WPT).
2. **Create a machine-readable coverage matrix.** List every required
   interface, global method/property, normative source specification, test
   status, intentional deviation, and capability dependency.
3. **Run official behavior tests.** Build a guest-side adapter for the
   applicable WPT `testharness.js` subset. Test semantics and error behavior,
   not merely whether names exist on `globalThis`.
4. **Differential-test the embedder.** Run the same selected tests in the
   Workerd fork outside Hyperlight and through the Hyperlight executor. Any
   difference is an embedding/ABI regression until explained.
5. **Restore required APIs safely.** Timers need a bounded timer channel.
   `fetch()` needs a capability broker with explicit destinations, DNS policy,
   byte/time limits, and deterministic local test endpoints. Production may
   intentionally deny capabilities, but that profile must not be described as
   ECMA-429 conformant.
6. **Resolve WebAssembly policy.** Either enable the required WebAssembly API
   with resource controls or document that the profile is intentionally
   non-conformant.
7. **Add negative and adversarial coverage.** Test malformed ABI frames,
   oversized bodies/headers, cancellation, timer storms, stream backpressure,
   decompression limits, promise rejection handling, snapshot corruption,
   timeout recovery, and capability-denial paths.
8. **Publish results from CI.** Produce a versioned JSON and human-readable
   report with pass/fail/unsupported/deviation totals. Do not use the word
   “conformant” until every required item passes or ECMA-429 explicitly permits
   the documented behavior.

## 15. Record the results

Capture:

- Azure VM size and CPU model
- Linux kernel version
- Workerd executor, kernel, executor-package, and rootfs SHA-256 values
- active-sandbox and queue limits
- `hey` request count and concurrency
- throughput and p50/p95/p99 latency
- HTTP errors and 503 count
- `workerd-demo` phase profiles
- host resident and peak memory
