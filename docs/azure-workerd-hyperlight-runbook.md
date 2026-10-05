# Run Workerd on Hyperlight

This walkthrough starts from an existing x86-64 Linux machine with KVM. It
builds the signed public Workerd and Hyperlight sources, then presents every
Workerd-on-Hyperlight feature as an interactive, individually runnable demo.
The historical URL is retained for existing links; no cloud tooling is needed.

## 1. Clone and check KVM

```bash
git clone \
  --branch simongdavies-adaptive-prewarm-profiling \
  https://github.com/simongdavies/hyperlight-unikraft.git
cd hyperlight-unikraft
test -c /dev/kvm && test -r /dev/kvm && test -w /dev/kvm
```

You also need Docker, Git, curl, GPG, Rustup, Node.js, npm, Go, Python, and
common C/C++ build tools. The setup script can install the Ubuntu/Debian host
packages, but it does not configure KVM or Docker permissions.

## 2. Build the signed pair

```bash
tools/setup-workerd-demo.sh --install-deps
```

Omit `--install-deps` when dependencies are already installed. The script:

- verifies signed Hyperlight commit
  `c0564669d7cc7cfd42f33d28e4a0f69261f3dca6`;
- checks out and verifies signed Workerd commit
  `621cb07e7d2cf0cb0f49872129d4408f6319acef`;
- initializes submodules and builds the Workerd executor;
- packages the executor with `examples/workerd-executor/build-rootfs.sh`;
- builds `workerd-demo` and the reproducible Component fixture.

Run `tools/setup-workerd-demo.sh --help` for checkout and concurrency options.
Successful setup ends with `Setup complete` and the built binary paths.

## 3. Choose a demo

List the presenter menu:

```bash
tools/hyperlight-demo --list
```

Preview the complete audience-facing narration without KVM or built artifacts:

```bash
tools/hyperlight-demo --preview
tools/hyperlight-demo --describe d1
```

Run the guided interactive journey:

```bash
tools/hyperlight-demo
```

Run every demo unattended, or select one:

```bash
tools/hyperlight-demo --all
tools/hyperlight-demo --demo kv
tools/hyperlight-demo --demo benchmark-prewarmed
```

Each step prints its purpose, action, key result, and takeaway. Ordinary logs
and JSON output go to `demo-output/`; override that location with
`--output-dir DIR` or `HYPERLIGHT_DEMO_OUTPUT`. Raw command output stays in
those logs so the default presenter view remains concise. Add `--verbose` when
you want to stream it during a run.

## Demo index

| Demo | What it shows | Expected result |
|---|---|---|
| `isolation` | Timeout disposal and fresh-VM recovery | HTTP 504 is followed by a successful clean request |
| `policy` | Who is running, permissions, usage limits, activity log, and reset | The host applies the correct Worker's rules; the Worker cannot change them |
| `ingress` | Scheduled events and message batches | A host passes an event or batch through the VM and receives completion, acknowledge, or retry decisions |
| `kv` | KV key/value data | Stored data remains after the temporary VM is replaced |
| `cache` | Cache API | Cached state survives a fresh VM reset |
| `d1` | D1 / SQL | A transactional batch commits atomically and persists across reset |
| `durable-objects` | Durable Objects | Each object's data stays separate and survives VM replacement |
| `storage` | Packaged files and named folders | Listed folders work; folder escapes, read-only writes, and excess use are denied |
| `node` | Node-style modules and files | Supported modules work without opening the whole host machine |
| `core-wasm` | Core WebAssembly | The packaged Wasm export runs in disposable request VMs |
| `component` | Public Component fixture | Lowering and identities verify; the signed runtime size limit is reported |
| `wasi-p2` | WASI Preview 2 | Portable HTTP, streams, clock, random, cleanup, and allowed-access checks pass |
| `wasi-p3` | WASI Preview 3 | Ordering, backpressure, deadlines, cancellation, and cleanup pass |
| `tcp-tls` | TCP/TLS broker | Declared destinations work and undeclared destinations fail before I/O |
| `udp` | Controlled UDP messages | Bounded messages work only for host-approved destinations |
| `websocket` | Controlled WebSocket connections | Configured endpoint, message-size, lifetime, reset, and denial checks pass |
| `web-apis` | WinterTC and Web APIs | Timers, streams, handlers, MessagePort, and state reset pass |
| `fetch` | Constrained outbound fetch | One declared loopback service works; other routes and redirects stay bounded |
| `benchmark-on-demand` | Cold pool latency/throughput | 320 HTTP 200 responses at concurrency 32 plus quiescent pool status |
| `benchmark-prewarmed` | Adaptive warm-pool latency/throughput | Matched load succeeds and reports ready/refill/pressure state |

The presenter reuses the repository's existing VFS, named-storage, WinterTC,
and fetch scripts and their checked-in Worker bundles. Use their `--list`
options when presenting the lower-level route checks.

## Compatibility at a glance

Workerd runs JavaScript applications built for the Workers platform.
Hyperlight runs each request inside a small temporary virtual machine. This
project connects them; it does not replace Workerd's features, but controls
what crosses the VM boundary.

These tables describe the signed self-hosted Workerd package and the ways this
project connects it to temporary VMs. They distinguish what the demos prove
from complete compatibility that is not claimed.

### WinterTC and Web APIs

WinterTC is a shared checklist of browser-style JavaScript APIs that are useful
outside a browser, such as requests, responses, URLs, streams, timers, files,
and cryptography.

| Status | Surface | What the demo proves or what remains |
|---|---|---|
| Working | URL and request APIs, FormData, Blob/File, text codecs, crypto digest/random, timers, streams/transforms/compression, MessagePort, byte streams with caller-provided buffers, core Wasm, and fresh-state reset | `web-apis` and `core-wasm` run the listed APIs in temporary VMs. These APIs are working, not unsupported. |
| Working with host checks | Outbound fetch, TCP/TLS, UDP, WebSocket, named folders, and saved data | The matching demos prove that only listed destinations, folders, and operations are available. |
| Different storage lifetime | KV, Cache, D1, and Durable Objects | Data is kept by the host so it survives replacement of the temporary VM. VM-local counters and request state reset. |
| Remaining gap | Complete WinterTC and browser-style API coverage | The listed APIs are demonstrated, but complete compatibility with every WinterTC or browser API check has not been established. |
| Unavailable by design | Unrestricted dynamic code and direct host access | `eval()`/`new Function()`, arbitrary host paths, raw sockets/listeners, and arbitrary host extensions are not provided. |

### Node compatibility

The pinned Workerd fork supports many Node-style JavaScript APIs, but it is not
a complete Node.js runtime. Prior selected runs passed the configured suite
**409/409** and the broader selected suite **518/518**. Those results
demonstrate the selected subset, not complete Node.js compatibility.

| Surface | Supported behavior and boundary |
|---|---|
| JavaScript modules and common APIs | Both ES modules and CommonJS (the two common JavaScript module formats), plus text, JSON, core Wasm, and selected Buffer/path/URL/stream/crypto-style APIs work. |
| Files | `node:fs` uses packaged files, fresh temporary files, supported device files, and explicitly named folders. It cannot search the host filesystem. |
| `process` information | Applications see stable placeholder values such as `pid=1`, not the real host process. |
| Networking | HTTP and explicitly enabled TCP/TLS/UDP/WebSocket paths can reach listed destinations. Importing a module does not open the network. |
| Loading code | Applications can load modules included in their bundle. They cannot install packages at runtime, search host folders, or use `eval()`/`new Function()`. |
| Child processes | `node:child_process` can be imported, but process-creation methods report `ERR_METHOD_NOT_IMPLEMENTED`. |
| Worker threads | Operational worker threads are unavailable (`isMainThread=true`, `threadId=0`, `parentPort=null`); MessageChannel/MessagePort remain supported separately. |
| Native add-ons | `.node` loading and `process.dlopen()` report `ERR_METHOD_NOT_IMPLEMENTED`; use JavaScript, built-ins, or packaged core Wasm. |

### What this project adds around self-hosted Workerd

This is a comparison with vanilla self-hosted Workerd, not a comparison with
managed products.

| Feature | What Workerd already does | What this project adds |
|---|---|---|
| Temporary VM for each request | Runs JavaScript requests and events in isolated runtimes | Runs each request or event in a small temporary VM and destroys it afterward; a timeout does not poison the next request |
| Fast VM startup | Starts and manages its normal runtime processes | Saves a ready VM image, restores it on demand, or keeps an adaptive pool ready |
| Who is running, permissions, and limits | Uses configuration, bindings, and runtime limits | The host identifies the Worker, applies its permissions and usage limits, resets counters, and records activity across VM replacement |
| Scheduled events and message batches | Runs scheduled and queue handlers for supplied events | Passes a scheduled event or logical queue name plus message batch into the VM and returns completion, acknowledge, retry, batch-retry, or no-retry decisions |
| KV, Cache, D1, and Durable Objects | Provides these APIs and configured local or remote data services | Connects them to host-kept data that survives destruction of the temporary VM |
| Named folders | Provides bundle files, directory services, and virtual Node files | Adds named read-only/read/write host folders with path checks and per-VM operation/byte limits |
| TCP and TLS | Workerd can open outbound TCP connections. TLS adds encryption to that connection. | The host opens the connection for the temporary VM. Only configured hostnames, IP addresses, ports, and encryption settings are allowed. |
| WebSockets | Workerd can create and use WebSocket connections. | The host allows connections only to configured WebSocket endpoints and limits message size and connection lifetime. |
| UDP | UDP support depends on which Workerd API is being used. | The host can send and receive UDP messages for the temporary VM, but only for configured addresses and ports and within configured size limits. General network access is not provided. |
| WASI and Components | Supports core Wasm and Worker APIs | Adds WASI Preview 2/3 work and a pinned conversion from Components to core Wasm plus JavaScript |
| Rust bridge | Is normally built and embedded through C++ paths | Packages the executor through a Rust guest bridge into the same temporary-VM lifecycle |
| Measurements | Provides its own runtime diagnostics | Adds VM counts, waiting time, restore/refill, cleanup pressure, and phase timing |

WASI lets WebAssembly programs use a selected set of common services.
Components package portable WebAssembly interfaces; this project converts the
pinned example into core Wasm and JavaScript that Workerd can load.

### Scheduled jobs and external queues

This project is not a scheduler and does not connect an external queue product.
A host supplies the scheduled event or message batch. For queued work, the
boundary carries a logical queue name and batch into the temporary VM and
returns acknowledge or retry decisions. A separate host adapter is still
required to read from and write to a real queue service and apply those
decisions.

## Component fixture limitation

`component` rebuilds and verifies only the checked-in public example and runs
the matching file and bundle checks. The signed Workerd executor
limits an individual module to 32 KiB; the generated Component JavaScript is
45,194 bytes. Full guest execution therefore needs a separately reviewed
Workerd source change. This walkthrough does not patch or replace the signed
source.

## Matched latency and throughput

The two benchmark demos use the same bundle, 384 MiB scratch, 30-second request
timeout, active/client concurrency 32, queue 256, profile interval 64, and:

```text
hey -n 320 -c 32 http://127.0.0.1:8787/sync
```

`benchmark-on-demand` uses cold restore. `benchmark-prewarmed` uses owners48,
active32, restore1, warm floor1, ready low/high 16/32, and refill batch2.
Compare `demo-output/benchmark-on-demand/hey.txt` with
`demo-output/benchmark-prewarmed/hey.txt` on the same machine. These are local
latency/throughput illustrations, not pass/fail targets.

The presenter also saves pool status. On-demand is quiescent when admitted,
active, queued, execution, teardown, and completion gauges are zero. Prewarmed
is quiescent when those gauges are zero, all inventory is ready, no restore is
active, and refill is inactive.

## Stop and clean up

The presenter stops its Worker and loopback helper on success, failure,
`Ctrl-C`, or exit. To remove local build and demo output:

```bash
rm -rf \
  demo-output \
  "${XDG_CACHE_HOME:-$HOME/.cache}/hyperlight-workerd" \
  build-elfloader/workerd-executor \
  experiments/workerd-component-model/node_modules \
  target
docker image rm workerd-hyperlight-builder
```

If `/dev/kvm` or Docker is denied, correct the current user's group/session
permissions and rerun the prerequisite check. If a build runs out of memory,
lower `WORKERD_BAZEL_JOBS` and `CARGO_BUILD_JOBS`.
