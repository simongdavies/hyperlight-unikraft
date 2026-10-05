# Run Workerd on Hyperlight

This walkthrough builds the signed public Workerd and Hyperlight sources on an
existing x86-64 Linux machine with KVM, then serves a Worker from fresh
Hyperlight micro-VMs. It does not provision cloud resources or require private
build inputs.

## Prerequisites

You need:

- x86-64 Linux with readable and writable `/dev/kvm`
- Git, Docker, curl, GPG, Rustup, and common C/C++ build tools
- enough disk space for Workerd, Bazel, and Docker build caches

Clone the public Hyperlight branch containing this walkthrough:

```bash
git clone \
  --branch simongdavies-standalone-workerd-runbook \
  https://github.com/simongdavies/hyperlight-unikraft.git
cd hyperlight-unikraft
export PATH="$HOME/go/bin:$HOME/.cargo/bin:$PATH"
```

Check KVM before starting the build:

```bash
test -c /dev/kvm && test -r /dev/kvm && test -w /dev/kvm
```

## Build

The setup script verifies that the runtime is based on signed Hyperlight commit
`c0564669d7cc7cfd42f33d28e4a0f69261f3dca6`, checks out signed Workerd commit
`621cb07e7d2cf0cb0f49872129d4408f6319acef`, initializes submodules, builds the
executor, packages its root filesystem, and builds `workerd-demo`.

```bash
tools/setup-workerd-demo.sh --install-deps
```

Omit `--install-deps` when the host already has the required packages. The
script also installs pinned `just`, `hey`, and `wasm-tools` commands when they
are missing, and rebuilds the checked-in Component Model fixture. Run
`tools/setup-workerd-demo.sh --help` for paths and build-concurrency settings.

Expected final output starts with `Setup complete` and lists the demo binary
and packaged executor.

## Run

Start the HTTP bridge:

```bash
target/release/examples/workerd-demo \
  --bundle examples/workerd-bundles/helloworld_esm.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --restore-mode prewarmed \
  --prewarmed-sandboxes 8 \
  --max-concurrent-sandboxes 4 \
  --max-concurrent-restores 1 \
  --warm-floor 1 \
  --ready-low-watermark 4 \
  --ready-high-watermark 8 \
  --max-replenish-batch 2
```

In another terminal, check the Worker and pool:

```bash
curl --fail-with-body http://127.0.0.1:8787/
curl --fail-with-body http://127.0.0.1:8787/__hyperlight/pool-status | jq
```

The first command returns `Hello World!`. The status response reports
`"restore_mode": "prewarmed"`, `"warm_floor": 1`, and a ready VM inventory.
Each request runs in one fresh VM; the owner thread destroys it and restores a
replacement without moving VMs between threads.

Press `Ctrl-C` in the server terminal to stop cleanly.

## Capability demos

The following demos reuse the built executor and checked-in fixtures. Run one
server at a time on port 8787, and stop it with `Ctrl-C` before starting the
next.

### Isolation, timeout recovery, and bounded admission

Start the acceptance Worker:

```bash
target/release/examples/workerd-demo \
  --bundle examples/workerd-bundles/acceptance.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 500 \
  --max-concurrent-sandboxes 1 \
  --queue-capacity 1
```

From another terminal:

```bash
curl --fail-with-body -X POST -d hello http://127.0.0.1:8787/hello
curl -i -X POST -d busy http://127.0.0.1:8787/busy
curl --fail-with-body -X POST -d after http://127.0.0.1:8787/after
```

The normal requests return their path and method. `/busy` returns HTTP 504,
and the following request succeeds in a fresh VM. A concurrent wave such as
`hey -n 20 -c 20 -m POST -d busy http://127.0.0.1:8787/busy` also shows HTTP
503 for work beyond the single execution and queue slots.

### Host policy, data bindings, scheduled events, and queues

These focused tests print the host-owned identity, quota, reset, audit, typed
KV, Cache, D1, Durable Object, scheduled, and queue behavior:

```bash
cargo +1.98.0 test --locked --lib data:: -- --nocapture
build-elfloader/workerd-executor/executor --self-test
```

Expected: tests pass; the executor self-test exits zero. Undeclared bindings,
unknown operations, invalid envelopes, stale identities, and exceeded quotas
fail closed. Durable host data survives replacement of the request VM.

### Virtual filesystems, named storage, and Node modules

Create the two explicit host directories, then start the VFS Worker:

```bash
mkdir -p demo-storage/readonly demo-storage/scratch
printf 'fixture-read-ok\n' >demo-storage/readonly/message.txt

target/release/examples/workerd-demo \
  --bundle examples/workerd-bundles/workerd-vfs-evidence.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --storage-ro readonly="$PWD/demo-storage/readonly" \
  --storage-rw scratch="$PWD/demo-storage/scratch" \
  --storage-max-operations scratch=16 \
  --storage-max-read-bytes scratch=1048576 \
  --storage-max-write-bytes scratch=16
```

In another terminal:

```bash
tools/run-workerd-vfs-demo.sh all
tools/run-workerd-storage-policy-demo.sh all
```

Expected: `/bundle`, fresh `/tmp`, and the supported `/dev` devices pass.
Named reads and bounded writes succeed; read-only writes, traversal,
undeclared mounts, and quota overflow are denied. This also exercises the
declared `node:fs`, CommonJS, and ESM boundaries without ambient packages.

### WinterTC, Web APIs, core Wasm, streams, and MessagePort

Start the Worker:

```bash
target/release/examples/workerd-demo \
  --bundle examples/workerd-bundles/workerd-wintertc-demo.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384
```

List or run its stable checks from another terminal:

```bash
tools/run-wintertc-demo.sh --list
tools/run-wintertc-demo.sh all
```

Expected: core APIs, timers, global handlers, byte streams, core Wasm,
MessagePort stages, and fresh-VM state isolation pass. The `fetch` check needs
the explicitly allowed loopback upstream described in the next section.

### Host-constrained fetch

Start a deterministic loopback upstream:

```bash
python3 - <<'PY' &
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
BODY = b"allowed-upstream\n"
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("location", "/ok")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("content-length", str(len(BODY)))
        self.end_headers()
        self.wfile.write(BODY)
    def log_message(self, *_):
        pass
ThreadingHTTPServer(("127.0.0.1", 18080), Handler).serve_forever()
PY
UPSTREAM_PID=$!
```

Start the fetch-policy Worker in another terminal:

```bash
target/release/examples/workerd-demo \
  --bundle examples/workerd-bundles/fetch-policy-demo.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --fetch-loopback-port 18080
```

Run the checks:

```bash
tools/run-workerd-fetch-policy-demo.sh all
```

The listed loopback endpoint succeeds. Wrong ports, unlisted hosts,
disallowed schemes, metadata, and private addresses are denied; redirects are
returned without being followed. Stop the upstream with
`kill "$UPSTREAM_PID"` after stopping the Worker.

To include fetch in the full WinterTC demo, start
`workerd-wintertc-demo.json` with the same `--fetch-loopback-port 18080`, then
run `tools/run-wintertc-demo.sh fetch`.

### WASI Preview 2 and Preview 3

```bash
cargo +1.98.0 test --locked --lib wasi_preview2::proof::tests -- --nocapture
cargo +1.98.0 test --locked --test wasi_p3 -- --nocapture
```

Expected: typed HTTP, bounded streams, deterministic clock/random, futures,
backpressure, cancellation, deadlines, and resource release pass. Undeclared
interfaces and ambient CLI, listener, and raw-device authority remain denied.

### TCP/TLS, UDP, and WebSocket capabilities

```bash
cargo +1.98.0 test --locked \
  --test broker_tcp_tls \
  --test broker_udp \
  --test broker_websocket \
  -- --nocapture
```

Expected: explicit host-owned destination and protocol policy passes, while
undeclared destinations, oversized messages, and ambient guest sockets fail
before host I/O.

### Component Model fixture

Rebuild and verify the checked-in, pinned `jco` lowering:

```bash
(
  cd experiments/workerd-component-model
  npm install \
    --ignore-scripts \
    --no-audit \
    --no-fund \
    --package-lock=false
  npm run build:component
  npm run transpile
  npm run lock
  npm test
)
cargo +1.98.0 test --locked --test workerd_component -- --nocapture
```

Expected: the Component binary validates, lowering is deterministic, the
canonical bundle and core Wasm identities match, and invalid Wasm is rejected.

The signed Workerd executor limits an individual module to 32 KiB, while this
fixture's generated JavaScript is 45,194 bytes. Therefore the full
`workerd-component-proof` guest command needs a separately reviewed Workerd
source change and is not silently patched by this signed-source walkthrough.

## Pool and throughput illustration

Use the same bundle and 32-way request shape for both modes. First start the
on-demand control:

```bash
target/release/examples/workerd-demo \
  --bundle examples/workerd-bundles/workerd-pool-benchmark.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 30000 \
  --restore-mode on-demand \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 256 \
  --profile-log-every 64
```

From another terminal:

```bash
hey -n 320 -c 32 http://127.0.0.1:8787/sync
curl --fail-with-body http://127.0.0.1:8787/__hyperlight/pool-status | jq
```

`hey` prints cold/on-demand latency and requests/second. After the wave, pool
status is quiescent when admitted, active, queued, execution, teardown, and
completion gauges are zero.

Stop that server, then start the matched adaptive prewarm run:

```bash
target/release/examples/workerd-demo \
  --bundle examples/workerd-bundles/workerd-pool-benchmark.json \
  --bind 127.0.0.1:8787 \
  --scratch-mb 384 \
  --request-timeout-ms 30000 \
  --restore-mode prewarmed \
  --prewarmed-sandboxes 48 \
  --max-concurrent-sandboxes 32 \
  --queue-capacity 256 \
  --max-concurrent-restores 1 \
  --warm-floor 1 \
  --ready-low-watermark 16 \
  --ready-high-watermark 32 \
  --max-replenish-batch 2 \
  --profile-log-every 64
```

Run the same client command:

```bash
hey -n 320 -c 32 http://127.0.0.1:8787/sync
curl --fail-with-body http://127.0.0.1:8787/__hyperlight/pool-status | jq
```

Compare the two `hey` summaries on the same machine and request shape. This is
a local demonstration, not an acceptance threshold. Prewarmed pool status
shows ready depth, warm floor, watermarks, active and restore slots,
replenishment state, teardown/completion pressure, hits/misses, and cumulative
phase timings. It is quiescent only after those current gauges are zero, all
inventory is ready, no restore is in progress, and refill is inactive.

## Troubleshooting

- **`/dev/kvm` permission denied:** add the user to the host's `kvm` group,
  start a new login session, and repeat the KVM check.
- **Docker permission denied:** configure Docker for the current user or run
  the setup through an approved rootful Docker workflow.
- **Workerd checkout is dirty:** remove local changes in `.workerd-src`, or set
  `WORKERD_DIR` to a clean checkout path.
- **Build terminated for lack of memory:** lower `WORKERD_BAZEL_JOBS` and
  `CARGO_BUILD_JOBS`.
- **Port 8787 is busy:** pass another loopback address with `--bind`.

## Local cleanup

Stop the server before removing generated files:

```bash
rm -rf \
  "${XDG_CACHE_HOME:-$HOME/.cache}/hyperlight-workerd" \
  build-elfloader/workerd-executor \
  experiments/workerd-component-model/node_modules \
  target
docker image rm workerd-hyperlight-builder
```
