# Run Workerd on Hyperlight

This walkthrough starts from an existing x86-64 Linux machine with KVM. It
builds the Workerd and Hyperlight forks containing this integration, then
presents every feature as an interactive, individually runnable demo.

## 1. Clone and check KVM

```bash
git clone \
  --branch simongdavies-adaptive-prewarm-profiling \
  https://github.com/simongdavies/hyperlight-unikraft.git
cd hyperlight-unikraft
test -c /dev/kvm && test -r /dev/kvm && test -w /dev/kvm
```

Your user must be able to use `/dev/kvm` and Docker. Run the setup script as
that user, not with `sudo`; it invokes `sudo` only for apt. The script keeps
Rustup and Cargo in writable user directories (normally `~/.rustup` and
`~/.cargo`) and installs Ubuntu's `docker.io` and `containerd` packages.

If Docker CE packages such as `containerd.io`, `docker-ce`, or
`docker-ce-cli` are already installed, remove them before using
`--install-deps`; they conflict with Ubuntu's `containerd` package:

```bash
sudo apt-get remove containerd.io docker-ce docker-ce-cli
```

The setup script detects this conflict before apt changes anything and reports
the installed packages to remove.

Docker may warn that its legacy builder is deprecated. That warning is
harmless for this setup run. On Ubuntu releases that provide the distro
package, the setup script installs it automatically; it can also be installed
directly:

```bash
sudo apt-get install -y docker-buildx
```

Do not install Docker CE's `docker-buildx-plugin`; keep the Docker packages
on Ubuntu's `docker.io`/`containerd` path.

If a previous run fails while linking a builder tool with an undefined libc++
symbol such as `std::__1::__hash_memory`, remove only the stale Bazel output
and action caches, then rerun:

```bash
rm -rf \
  "$HOME/.cache/hyperlight-workerd/bazel/output" \
  "$HOME/.cache/hyperlight-workerd/bazel/action-cache"
tools/setup-workerd-demo.sh --install-deps
```

The setup script namespaces new Bazel output and action caches by the exact
builder image ID so objects built with an earlier Clang/libc++ image cannot be
reused. Downloaded Bazel repositories remain shared across builder images.
Before starting the expensive Workerd build, it also verifies Clang 22 and
links a small libc++ program inside the builder.

## 2. Build

```bash
tools/setup-workerd-demo.sh --install-deps
```

This checks out the matching Workerd fork, builds both projects, and prepares
the demos. Omit `--install-deps` when the build tools are already installed.

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
| `storage` | Packaged files and host folders | Packaged files and host-allowed folders work; all other host paths, read-only writes, and excess use are denied |
| `node` | Node-style modules and files | Included modules and allowed files work; processes, worker threads, native add-ons, and other host files remain unavailable |
| `core-wasm` | Core WebAssembly | The packaged Wasm export runs in disposable request VMs |
| `component` | WebAssembly Component example | The example builds and runs in Workerd on Hyperlight |
| `wasi-p2` | WASI Preview 2 | Portable HTTP, streams, clock, random, cleanup, and allowed-access checks pass |
| `wasi-p3` | WASI Preview 3 | Ordering, backpressure, deadlines, cancellation, and cleanup pass |
| `tcp-tls` | TCP and encrypted TCP connections | Network access is denied by default; destinations allowed by the host connect and everything else is blocked before a connection opens |
| `udp` | Controlled UDP messages | Bounded messages work only for host-approved destinations |
| `websocket` | Controlled WebSocket connections | Configured endpoint, message-size, lifetime, reset, and denial checks pass |
| `web-apis` | WinterTC and Web APIs | Timers, streams, handlers, MessagePort, and state reset pass |
| `fetch` | Constrained outbound fetch | One declared loopback service works; other routes and redirects stay bounded |
| `benchmark-on-demand` | Create VMs as requests arrive | Sends 320 requests, with up to 32 running at once, and confirms every request succeeds and cleanup finishes |
| `benchmark-prewarmed` | Reuse a pool of ready VMs | Sends the same requests and shows how many ready VMs remain and whether replacements are being prepared |

The presenter reuses the repository's existing VFS, named-storage, WinterTC,
and fetch scripts and their checked-in Worker bundles. Use their `--list`
options when presenting the lower-level route checks.

## Compatibility at a glance

Workerd runs JavaScript applications built for the Workers platform.
Hyperlight runs the untrusted Worker code for each request inside a small
temporary virtual machine. The host provides the external interfaces used by
Workerd features and applies the configured rules there. For example, network
connections can reach only destinations allowed by the host, and file APIs can
see only packaged files and the specific host folders allowed for that Worker.

These tables show which Workerd features are available, what restrictions
apply, and which compatibility gaps remain.

### Worker API capabilities

WinterTC is a shared checklist of browser-style JavaScript APIs that are useful
outside a browser, such as requests, responses, URLs, streams, timers, files,
and cryptography.

| Capability group | Supported | Missing or restricted |
|---|---|---|
| Core web APIs | URL and request/response APIs, FormData, Blob/File, text codecs, digest, secure random values, timers, readable/writable/transform/compression streams, MessagePort, and byte streams with caller-provided buffers | Support status is not yet documented for WinterTC APIs outside this list |
| Network clients | Outbound fetch, TCP/TLS, UDP, and WebSocket through host-provided interfaces | Denied by default; only allowed destinations and operations work; direct host sockets and listeners are unavailable |
| Files | Packaged files, fresh temporary files, supported device files, and specific host folders granted to the Worker | Every other host path is inaccessible; temporary files are discarded with the VM |
| Saved application data | KV, Cache, D1, and Durable Objects backed by host-kept data | Access is limited to configured services and allowed operations |
| Request isolation | A fresh VM and fresh mutable module state for each request or event | VM-local state does not persist after the VM is destroyed |
| Code and WebAssembly | Bundled JavaScript modules, core Wasm, and the WebAssembly Component example | `eval()`, `new Function()`, native add-ons, arbitrary host extensions, and native Component Model loading are unavailable |

### Node compatibility

The Workerd fork supports many Node-style JavaScript APIs, but it is not
a complete Node.js runtime. Prior selected runs passed the configured suite
**409/409** and the broader selected suite **518/518**. Those results
cover the selected subset; they do not establish complete Node.js
compatibility.

| Surface | Supported behavior and boundary |
|---|---|
| JavaScript modules and common APIs | Both ES modules and CommonJS (the two common JavaScript module formats), plus text, JSON, core Wasm, and selected Buffer/path/URL/stream/crypto-style APIs work. |
| Files | `node:fs` can read packaged files and use fresh temporary files. The host may also grant the Worker access to specific folders. Every other host path is inaccessible. |
| `process` information | Applications see stable placeholder values such as `pid=1`, not the real host process. |
| Networking | The host controls every external connection, and access is denied by default. It may allow a Worker to use outbound fetch, TCP/TLS, UDP, or WebSocket only for approved destinations and operations. Loading a module grants no network access. |
| Loading code | Applications can load modules included in their bundle. They cannot install packages at runtime, search host folders, or use `eval()`/`new Function()`. |
| Child processes | `node:child_process` can be imported, but process-creation methods report `ERR_METHOD_NOT_IMPLEMENTED`. |
| Worker threads | Code cannot create additional JavaScript worker threads. `node:worker_threads` reports that code is running on the main thread. MessageChannel and MessagePort can pass messages, but they do not create another thread. |
| Native add-ons | `.node` loading and `process.dlopen()` report `ERR_METHOD_NOT_IMPLEMENTED`; use JavaScript, built-ins, or packaged core Wasm. |

### What the Hyperlight integration changes

Workerd already provides the features in the middle column. The integration
runs untrusted Worker code in temporary VMs and applies host rules to the
external interfaces they use.

| Feature | What Workerd already does | What this project adds |
|---|---|---|
| Temporary VM for each request | Runs JavaScript requests and events in isolated runtimes | Runs each request or event in a small temporary VM and destroys it afterward; a timeout does not poison the next request |
| Fast VM startup | Starts and manages its normal runtime processes | Saves a ready VM image, restores it on demand, or keeps an adaptive pool ready |
| Worker permissions and limits | Uses configuration, bindings, and runtime limits | Before a VM starts, the host selects the Worker bundle and registers its allowed network, timer, and file services. Each external operation is sent to the host, which checks the policy, performs or denies the operation, and counts usage. A replacement VM gets fresh per-request counters, while Worker code cannot read or change the host policy. |
| Scheduled events and message batches | Runs scheduled and queue handlers for supplied events | Passes a scheduled event or logical queue name plus message batch into the VM and returns completion, acknowledge, retry, batch-retry, or no-retry decisions |
| KV, Cache, D1, and Durable Objects | Provides these APIs and configured local or remote data services | Connects them to host-kept data that survives destruction of the temporary VM |
| Host folders | Provides bundle files, directory services, and virtual Node files | Before launch, the host opens each allowed folder and exposes it at a fixed path inside the VM. Worker code can use paths only inside that folder. The host blocks path escapes and writes to read-only folders, counts operations and transferred bytes, and rejects further access when the configured limit is reached. |
| TCP and TLS | Workerd can open outbound TCP connections. TLS adds encryption to that connection. | Worker code sends the requested destination and encryption requirements to the host. The host checks them before DNS lookup or connection setup, opens an allowed socket, and relays bytes for the VM. A denied request fails before a connection opens, and the Worker never receives general host socket access. |
| WebSockets | Workerd can create and use WebSocket connections. | Worker code requests an endpoint through the host interface. The host checks the endpoint, opens an allowed connection, relays messages, and enforces message-size and lifetime limits. Denied endpoints never connect. |
| UDP | UDP support depends on which Workerd API is being used. | Worker code sends the destination and message to the host. The host checks the address, port, and message size before sending it and returns allowed replies to the VM. Other destinations and oversized messages are denied; the Worker never receives a general UDP socket. |
| WASI and Components | Supports core Wasm and Worker APIs | Implements selected WASI Preview 2 and Preview 3 host interfaces and runs the included WebAssembly Component example. |
| Measurements | Provides its own runtime diagnostics | Shows how many VMs are ready, running, waiting, or being cleaned up, plus time spent in each step |

### Scheduled jobs and external queues

This project is not a scheduler and does not connect an external queue product.
A host supplies the scheduled event or message batch. For queued work, the
boundary carries a logical queue name and batch into the temporary VM and
returns acknowledge or retry decisions. A separate host adapter is still
required to read from and write to a real queue service and apply those
decisions.

One adapter option is a small Azure Function. A timer, queue, or Service Bus
trigger can receive work; a
[managed connector trigger](https://learn.microsoft.com/azure/azure-functions/functions-connectors-overview)
can receive events from services such as Microsoft 365, Teams, or SharePoint.
The function converts the incoming event into this project's scheduled or
queue request, calls the host boundary, and maps the returned completion or
retry decision back to the source service. Azure Functions and the connected
service remain separate from this project.

## WebAssembly Component example

The WebAssembly Component example builds and runs in Workerd on Hyperlight.
This demo does not add native Component Model loading.

## Compare creating VMs with using ready VMs

Both demos send the same 320 requests, with up to 32 running at once.
`benchmark-on-demand` creates or restores VMs as requests arrive.
`benchmark-prewarmed` begins with ready VMs and prepares replacements as they
are used.

The presenter reports response time and request rate, confirms that no work is
left running or waiting, and shows whether replacement VMs are still being
prepared. Run both demos on the same machine for a meaningful comparison. Raw
results are saved under `demo-output/benchmark-on-demand/` and
`demo-output/benchmark-prewarmed/`.

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
