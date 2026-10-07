# Run Workerd on Hyperlight

This walkthrough starts from an existing x86-64 Linux machine with KVM. It
builds the Workerd and Hyperlight forks containing this integration, then
presents every feature as an interactive, individually runnable demo.

## 1. Clone and check KVM

```bash
git clone \
  --branch workerd-on-hyperlight \
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

The setup script builds on the same Debian Trixie base used by Workerd's
release container. It creates a matching libc++ 22 shared runtime from the
packaged static archive for Bazel host tools, then verifies the
`std::__1::__hash_memory` link before starting the expensive Workerd build.
On a fresh machine, `--install-deps` also installs Rustup when `rustup` or
`cargo` is missing, then installs the pinned Rust toolchain and development
tools as the current user.
Builder-specific Bazel output and action caches are namespaced by the exact
builder image ID; downloaded repositories remain shared. A packaged executor
that passes its self-test is stamped with the Workerd commit and builder image
ID, so rerunning setup skips Bazel while those inputs remain unchanged.

## 2. Build

```bash
tools/setup-workerd-demo.sh --install-deps
```

This checks out the matching Workerd fork, builds both projects, and prepares
the demos. Omit `--install-deps` when the build tools are already installed.
`just setup-workerd-demo` runs the same script from a checkout with `just`
installed.

## 3. Choose a demo

List the presenter menu:

```bash
tools/hyperlight-demo --list
```

Preview the complete audience-facing narration without KVM or built artifacts:

```bash
tools/hyperlight-demo --preview
tools/hyperlight-demo --describe sql
```

`--preview` shows the narration for every demo. `--describe NAME` shows one
demo's narration and a `WORKER CODE` section without starting a VM. For example:

```bash
tools/hyperlight-demo --describe ingress
tools/hyperlight-demo --describe node
tools/hyperlight-demo --describe benchmark-prewarmed
```

The code section identifies each module by name and type and prints at most 20
source lines so large Workers do not overflow the presenter. It writes the
complete module set to `demo-output/worker-code/<demo>.json` and prints a
copy-paste `cat ... | jq -r ...` command that renders the full source with
module labels. The capability proof uses the checked-in files under
`examples/workerd-capability-workers/`, so the displayed code is the same
source compiled into the proof executable. Component demos show both their
entry module and generated JavaScript module. Host-only WASI and network-broker
demos state explicitly that they do not run JavaScript inside Workerd.

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

`just workerd-demos --demo resident` runs the same presenter through the
justfile.

Clear all generated output for this checkout before a presentation:

```bash
tools/hyperlight-demo --clear
```

Each successful step prints a compact evidence table containing the values
that were actually observed, not only a PASS label. Before execution, each
selected demo also prints the same `WORKER CODE` section available through
`--describe`. Ordinary logs and JSON output go to `demo-output/`; override
that location with `--output-dir DIR` or `HYPERLIGHT_DEMO_OUTPUT`. Raw command
output stays in those logs so the default presenter view remains concise. Add
`--verbose` when you want to stream it during a run. A failed step prints the
failing stage, artifact location, and final 30 log lines.

## Demo index

| Demo | What it shows | Expected result |
|---|---|---|
| `isolation` | Timeout disposal and fresh-VM recovery | HTTP 504 is followed by a successful clean request |
| `policy` | Who is running, permissions, usage limits, activity log, and reset | The host applies the correct Worker's rules; the Worker cannot change them |
| `ingress` | Scheduled events and message batches | A host passes an event or batch through the VM and receives completion, acknowledge, or retry decisions |
| `kv` | KV key/value data | Stored data remains after the temporary VM is replaced |
| `cache` | Cache API | Cached state survives a fresh VM reset |
| `sql` | SQL transactions | A transactional batch commits atomically and persists across reset |
| `durable-objects` | Durable Objects | Each object's data stays separate and survives VM replacement |
| `storage` | Packaged files and host folders | Packaged files and host-allowed folders work; all other host paths, read-only writes, and excess use are denied |
| `node` | Node-style modules and files | Included modules and allowed files work; processes, worker threads, native add-ons, and other host files remain unavailable |
| `core-wasm` | Core WebAssembly | The packaged Wasm export runs in disposable request VMs |
| `component` | WebAssembly Component example | The example builds and runs in Workerd on Hyperlight |
| `wasi-p2` | WASI Preview 2 executable contract | A standalone proof prints observed HTTP, stream, clock, random, CLI, quota, and denial results |
| `wasi-p3` | WASI Preview 3 executable contract | A standalone proof prints observed future, stream-ordering, backpressure, cancellation, and import-policy results |
| `tcp-tls` | TCP and encrypted TCP broker boundary | Real local TCP and TLS 1.3 exchanges pass through the host broker; denied destinations fail before connect |
| `udp` | Controlled UDP broker boundary | Real bounded datagrams work only for host-approved destinations |
| `websocket` | Controlled WebSocket broker boundary | Real local WebSocket exchanges enforce endpoint, message-size, lifetime, reset, and denial rules |
| `web-apis` | WinterTC and Web APIs | Timers, streams, handlers, MessagePort, and state reset pass |
| `fetch` | Constrained outbound fetch | One declared loopback service works; other routes and redirects stay bounded |
| `resident` | Reusable resident VM | A resident VM serves several sequential requests without teardown, then is explicitly retired once it crosses its configured request limit |
| `multi-app` | Multi-app routing in one host process | One `workerd-host` process serves two independently configured apps, routed by hostname, from a single listening port |
| `orchestrator-contract` | Orchestrator-ready process contract | `/healthz`, `/readyz`, and `/status` report correctly while running, and `SIGTERM` drains in-flight work and exits cleanly |
| `benchmark-on-demand` | Create VMs as requests arrive | Sends the configured load through a bounded number of concurrent request VMs and confirms every request succeeds and cleanup finishes |
| `benchmark-prewarmed` | Reuse a pool of ready VMs | Sends the same configured load and shows how many ready VMs remain and whether replacements are being prepared |
| `benchmark-resident` | Resident-host throughput across independent apps | Real `hey` load against several independently running resident `workerd-host` processes, each on its own loopback port, with per-app and aggregate throughput |
| `benchmark-multi-app` | Multi-app routing throughput and fairness | Real `hey` load against several hostname-routed apps behind one `workerd-host` process, with per-app and aggregate throughput plus routing/queue counters |
| `benchmark-orchestrator-contract` | Orchestrator contract under load and drain | Real `hey` load against several apps, then `SIGTERM` mid-flight proves readiness goes false, new traffic is not admitted, admitted requests still drain, and exit happens within the configured drain timeout |

The presenter reuses the repository's existing VFS, named-storage, WinterTC,
and fetch scripts and their checked-in Worker bundles. Use their `--list`
options when presenting the lower-level route checks.

### Independent backing-store verification

The KV, Cache, SQL, and Durable Object demos do not rely only on text emitted by
the presenter. Each demo:

1. clears its named SQLite backing files;
2. creates a new empty database;
3. prints a copy-paste `sqlite3 -readonly` command that proves the database has
   zero application tables;
4. performs the write and fresh-VM read through Workerd;
5. prints measured VM teardown times;
6. prints the database path and SHA-256; and
7. prints a second copy-paste `sqlite3 -readonly` command plus the unmodified
   CLI rows.

The proof executable has exited before the second CLI query runs. An audience
member can paste the displayed command into another terminal and query the same
backing file directly. Raw JSON, CLI output, hashes, and logs remain below
`demo-output/`.

The WASI P2 and P3 entries run standalone proof executables rather than parsing
unit-test names. Their complete JSON output is saved as
`demo-output/wasi-p2-proof.json` and `demo-output/wasi-p3-proof.json`. These are
typed host-interface contract demonstrations; they are not claims that a
native WASI component was loaded inside the Workerd VM.

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
| Network clients | Outbound fetch through a Workerd request VM; TCP/TLS, UDP, and WebSocket through the separately tested host broker | Denied by default; the current sandbox executor does not yet connect Workerd's standard socket APIs to the host broker |
| Files | Packaged files, fresh temporary files, supported device files, and specific host folders granted to the Worker | Every other host path is inaccessible; temporary files are discarded with the VM |
| Saved application data | KV, Cache, SQL, and Durable Objects backed by host-kept data | Access is limited to configured services and allowed operations |
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
| Networking | The host controls every external connection, and access is denied by default. Outbound fetch is wired through the Workerd request path. TCP/TLS, UDP, and WebSocket have real host-broker socket proofs but are not yet wired to Workerd's standard socket APIs. Loading a module grants no network access. |
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
| Fast VM startup | Starts and manages its normal runtime processes | Saves a ready VM image, restores it on demand, or keeps an adaptive pool ready. An app can instead choose a bounded pool of resident VMs that each serve several requests before being recycled, alongside the unchanged disposable behavior |
| Multi-app hosting | Serves one application per process | `workerd-host` serves multiple independently configured apps from one process, routed by hostname and optional path prefix |
| Orchestration contract | Managed by its own process supervisor | `workerd-host` exposes `/__hyperlight/{healthz,readyz,status}` and drains in-flight requests to a deterministic exit code on `SIGTERM`/`SIGINT`, so an external orchestrator (not part of this project) can supervise it |
| Worker permissions and limits | Uses configuration, bindings, and runtime limits | Before a VM starts, the host selects the Worker bundle and registers its allowed network, timer, and file services. Each external operation is sent to the host, which checks the policy, performs or denies the operation, and counts usage. A replacement VM gets fresh per-request counters, while Worker code cannot read or change the host policy. |
| Scheduled events and message batches | Runs scheduled and queue handlers for supplied events | Passes a scheduled event or logical queue name plus message batch into the VM and returns completion, acknowledge, retry, batch-retry, or no-retry decisions |
| KV, Cache, SQL, and Durable Objects | Provides these APIs and configured local or remote data services | Connects them to host-kept data that survives destruction of the temporary VM |
| Host folders | Provides bundle files, directory services, and virtual Node files | Before launch, the host opens each allowed folder and exposes it at a fixed path inside the VM. Worker code can use paths only inside that folder. The host blocks path escapes and writes to read-only folders, counts operations and transferred bytes, and rejects further access when the configured limit is reached. |
| TCP and TLS | Workerd has standard outbound socket APIs. | The host broker performs real TCP and TLS 1.3 exchanges, checks policy before connect, owns trust roots, audits activity, and invalidates handles between assignments. Connecting Workerd's socket channel to this broker remains outstanding. |
| WebSockets | Workerd can create and use WebSocket connections. | The host broker performs real WebSocket exchanges and enforces endpoint, message-size, lifetime, and assignment-reset rules. Connecting Workerd's WebSocket channel to this broker remains outstanding. |
| UDP | UDP support depends on which Workerd API is being used. | The host broker performs real UDP exchanges and checks address, port, and message size before send. Connecting a Workerd UDP API to this broker remains outstanding. |
| WASI and Components | Supports core Wasm and Worker APIs | Runs executable typed-contract proofs for selected WASI Preview 2 and Preview 3 host interfaces and runs the included WebAssembly Component example. Native WASI Component loading in the Workerd VM is not claimed. |
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

Both demos derive their defaults from:

```bash
LC_ALL=C lscpu | grep -E '^(CPU\(s\)|Socket|Core|Thread)'
```

The presenter calculates real cores as `Socket(s) × Core(s) per socket`. If
those fields are unavailable, it falls back to
`CPU(s) ÷ Thread(s) per core`. Total requests default to 100 times that
real-core count. Concurrent requests and concurrent request VMs both default
to the real-core count rather than the hyper-thread/vCPU count. The prewarmed
inventory defaults to real cores plus one VM because the pool reserves one
warm VM while allowing one active request VM per real core.

A machine with 32 vCPUs, one socket, 16 cores, and two threads per core
therefore defaults to 1600 total requests, 16 concurrent requests, 16
concurrent request VMs, and 17 prewarmed VMs.
`benchmark-on-demand` creates or restores VMs as requests arrive.
`benchmark-prewarmed` begins with the derived ready inventory and prepares
replacements as VMs are used.

Override any dimension independently:

```bash
tools/hyperlight-demo \
  --benchmark-requests 1000 \
  --benchmark-concurrency 64 \
  --benchmark-vms 32 \
  --benchmark-pool-vms 48 \
  --demo benchmark-on-demand

tools/hyperlight-demo \
  --benchmark-requests 1000 \
  --benchmark-concurrency 64 \
  --benchmark-vms 32 \
  --benchmark-pool-vms 48 \
  --demo benchmark-prewarmed
```

`--benchmark-requests` controls the total `hey` request count.
`--benchmark-concurrency` controls concurrent `hey` requests.
`--benchmark-vms` bounds concurrent request VMs for either mode.
`--benchmark-pool-vms` controls the initial ready inventory for the prewarmed
mode, must be at least 2, and is ignored by the on-demand server. The same
settings can be supplied through `HYPERLIGHT_BENCHMARK_REQUESTS`,
`HYPERLIGHT_BENCHMARK_CONCURRENCY`, `HYPERLIGHT_BENCHMARK_VMS`, and
`HYPERLIGHT_BENCHMARK_POOL_VMS`.

The presenter reports response time and request rate, confirms that no work is
left running or waiting, and shows whether replacement VMs are still being
prepared. Its evidence includes both a compact logical/physical CPU summary and
an `LSCPU OUTPUT` section containing the exact filtered command output used to
derive the defaults. Run both demos on the same machine for a meaningful
comparison. Raw results, including `lscpu.txt` and the complete `hey` report,
are saved under `demo-output/benchmark-on-demand/` and
`demo-output/benchmark-prewarmed/`.

## Resident, disposable, and prewarmed-disposable VMs

These terms describe three distinct VM lifecycles available today; none of
the existing disposable behavior described above changes:

- **Disposable** (`benchmark-on-demand`, and every demo above except those
  listed below): one VM per request. A fresh VM is restored for each
  request or event and destroyed immediately afterward. No guest-visible
  state can ever carry over between requests.
- **Prewarmed disposable** (`benchmark-prewarmed`): still one VM per
  request — a request's VM is still destroyed after it completes — but an
  adaptive pool of already-restored VMs is kept ready ahead of demand, so a
  request need not wait for a fresh restore. This is purely a latency
  optimization; it does not let guest state persist across requests.
- **Resident** (`resident`, `multi-app`, `orchestrator-contract`, via
  `hluk workerd-host`): a bounded pool of VMs that each serve many
  requests — not one — before being explicitly recycled, either because
  they cross a configured `max_requests_per_vm`/`max_lifetime_secs` limit
  or because they error. Guest-visible state (anything the Worker script
  itself keeps in memory between requests) persists across requests on the
  same resident VM until it is recycled. `connection_affinity: sticky`
  additionally pins one keep-alive HTTP connection to the same resident VM
  for its whole lifetime. See
  [`examples/workerd-host/README.md`](../examples/workerd-host/README.md)
  for the full configuration schema and orchestrator contract.

## Resident-host throughput, routing, and drain under real load

`benchmark-resident`, `benchmark-multi-app`, and `benchmark-orchestrator-contract`
drive real `hey` load against the resident `workerd-host` process instead of
only functional checks, to back the `resident`, `multi-app`, and
`orchestrator-contract` claims above with throughput evidence:

- **`benchmark-resident`** starts several independent resident
  `workerd-host` processes, each bound to its own loopback port, and
  distributes the configured total requests across all of them at the
  configured aggregate concurrency.
- **`benchmark-multi-app`** starts one `workerd-host` process configured
  with several routed apps and distributes load across each app's
  hostname/routing contract, so one process and port serve all of it.
- **`benchmark-orchestrator-contract`** starts one `workerd-host` process
  with several apps, verifies `/healthz`/`/readyz`/`/status` under a
  reference load, then sends `SIGTERM` while a further share of requests is
  still in flight. It proves readiness goes false, new connections receive
  no response, already-admitted requests still drain successfully, and the
  process exits within its configured drain timeout. The post-`SIGTERM`
  readiness probe is expected to fail to connect (curl observes a timeout,
  connection-refused, empty-reply, or reset, depending on the exact race
  with process exit); the demo captures that probe's raw `curl` diagnostics
  to `post-signal-probe-stderr.txt` instead of printing them unexplained,
  and the presenter reports an explicit `PASS post-signal new connection:
  no response within 1s (expected during drain; curl exit N)` line. Any
  other outcome (a real HTTP response, or a curl failure outside that
  documented set) is reported as a `FAIL` with the diagnostic preserved.
  The in-flight-at-signal share sent right before `SIGTERM` is always
  exactly one synchronized wave of `conc` requests per app (never a larger
  share of the total load): every request in that wave is proven admitted
  by the admit-wait poll before the signal fires, so all of it drains
  deterministically regardless of `--benchmark-apps`/
  `--benchmark-concurrency` scale. Every internal assertion in this demo
  (hey exit status, response-count checks, the admit-wait poll, and the
  final process-exit check) now prints a diagnostic and the relevant
  artifact path to the log before failing, so a `FAIL` never leaves the
  raw log empty.

```bash
tools/hyperlight-demo \
  --benchmark-apps 4 \
  --benchmark-load-requests 1000 \
  --benchmark-concurrency 64 \
  --demo benchmark-resident

tools/hyperlight-demo \
  --benchmark-apps 4 \
  --benchmark-load-requests 1000 \
  --benchmark-concurrency 64 \
  --demo benchmark-multi-app

tools/hyperlight-demo \
  --benchmark-apps 4 \
  --benchmark-load-requests 1000 \
  --benchmark-concurrency 64 \
  --demo benchmark-orchestrator-contract
```

`--benchmark-apps` (or `HYPERLIGHT_BENCHMARK_APPS`) controls how many
independent or routed apps the load is spread across; it defaults to the
same derived physical-core count as the other `benchmark-*` defaults. That
default is intentionally left uncapped rather than silently reduced, so a
large machine keeps a correspondingly large default; pass a smaller
explicit value on a machine where that many loopback ports or resident
processes would be wasteful. `--benchmark-load-requests` (or
`HYPERLIGHT_BENCHMARK_LOAD_REQUESTS`) defaults to `--benchmark-apps x
1000` and `--benchmark-concurrency` reuses the same default as the other
benchmarks; both still accept the same `--benchmark-concurrency`,
`--benchmark-vms`, and `--benchmark-pool-vms` flags used above.
`--benchmark-concurrency` must be at least `--benchmark-apps`, and
`--benchmark-apps` cannot exceed `--benchmark-load-requests`.

When requests or concurrency do not divide evenly across apps, the
presenter allocates the remainder deterministically (earlier apps receive
one extra unit each) so the same inputs always produce the same
per-app split. Each `hey` process runs as a separate OS process; by
default (`--benchmark-hey-parallel N` / `HYPERLIGHT_BENCHMARK_HEY_PARALLEL`
not given) every app's `hey` process launches in a single synchronized
wave (`--benchmark-hey-parallel` defaults to `--benchmark-apps` itself),
not split into sequential wave_1-finishes-then-wave_2-starts batches —
lowering the cap below `--benchmark-apps` restores (bounded) wave-
splitting, at the cost of serializing later apps' load behind earlier
ones finishing. Within one wave, every `hey` job is pre-spawned *paused*
behind a shared gate file and released together as a batch, rather than
requests starting as a side effect of whatever order forking happened to
occur in; `--benchmark-hey-ramp-ms N` / `HYPERLIGHT_BENCHMARK_HEY_RAMP_MS`
(default 10ms, not 0) additionally delays each paused job's release by
its position in the batch times that many milliseconds, so the default
single-wave launch above ramps up instead of every `hey` process starting
to send requests in the same instant (a CPU "blast" at wave release); set
it to 0 to release every job in a wave at once. This adds roughly
`(batch_size - 1) * ramp_ms` to each wave's wall-clock time, folded into
the reported phase timing like any other wave cost. Both flags apply to
every `hey` launch site in this script, not only the three load
benchmarks here: the legacy `benchmark-on-demand`/`benchmark-prewarmed`
single-process benchmarks use the same pause/release mechanism (trivially,
since there is only one job to release, so these two flags' defaults have
no observable effect on them), and `benchmark-orchestrator-contract`'s
drain-test phase B also pre-spawns its apps paused and releases/ramps them
together, though it always does so in one ungated batch regardless of
`--benchmark-hey-parallel` (that phase polls real admission state
immediately after launch rather than waiting out a full wave, so it
cannot route through the same wave-capped helper the three load
benchmarks use).

Each demo prints per-app and aggregate `hey` results, along with the
specific evidence for its contract: resident-status and retirement
counters for `benchmark-resident`; `/status` routing/queue counters and
cross-app fairness for `benchmark-multi-app`; and pre-drain/during-drain
throughput, latency, and shutdown duration for
`benchmark-orchestrator-contract`. Raw `hey` output, `lscpu.txt`,
`/status` snapshots, and a machine-readable `summary.json` per demo are
saved to the usual demo output files under
`demo-output/benchmark-resident/`, `demo-output/benchmark-multi-app/`,
and `demo-output/benchmark-orchestrator-contract/`.

These three demos differ from `benchmark-on-demand` and
`benchmark-prewarmed` above: those two compare disposable VM creation
strategies for a single app under one server, while these three measure
the resident `workerd-host` binary itself — independent resident
processes, one process routing several apps, and that process's
orchestrator shutdown contract — each under real concurrent `hey` load
rather than only functional request/response checks.

### Resident-pool lifecycle limits during load, by demo

`max_requests_per_vm`/`max_lifetime_secs` (the resident pool's per-VM
recycling limits, introduced above) are deliberately left `null`
(unbounded) for the duration of every load benchmark below, so that VM
recycling never interferes with the throughput measurement itself. This
is different from the functional `resident` demo, which intentionally
sets a small `max_requests_per_vm` to demonstrate retirement:

- **Functional `resident` demo**: `{"max_requests_per_vm": 5,
  "max_lifetime_secs": null}` — chosen specifically so the sixth of six
  sequential requests crosses the limit and triggers one observable
  retirement.
- **`benchmark-resident`**: every per-app pool is configured as
  `{"max_requests_per_vm": null, "max_lifetime_secs": null}` — unbounded,
  so the configured resident VM(s) keep serving the entire load without
  a mid-benchmark recycle.
- **`benchmark-multi-app`**: even-indexed apps use a resident pool with
  the same `{"max_requests_per_vm": null, "max_lifetime_secs": null}`;
  odd-indexed apps use a `disposable` pool instead (one sandbox per
  request, which has no per-VM lifecycle limits to begin with).
- **`benchmark-orchestrator-contract`**: every app uses a `disposable`
  pool only; this benchmark exercises the orchestrator drain contract
  (`/healthz`, `/readyz`, `/status`, `SIGTERM`), not resident-VM
  recycling, so `max_requests_per_vm`/`max_lifetime_secs` do not apply to
  it at all.

At large `--benchmark-apps` counts, `timing.setup_seconds` is dominated by
per-app VM creation (`WorkerVersionSandbox::restore()`), not guest boot/init
work. `harness/app_config` can pass every app the same
`hluk workerd-prewarm-snapshot` output via `snapshot_dir` (see
[`examples/workerd-host/README.md`](../examples/workerd-host/README.md#prewarmed-snapshots-hluk-workerd-prewarm-snapshot)),
but doing so does **not** meaningfully reduce `setup_seconds` at scale:
measured restores from a prewarmed snapshot cost essentially the same as a
cold boot (the skipped guest-init step is a small fraction of the total).
There is currently no mechanism in this repo that avoids paying VM-creation
cost per app.

### `--benchmark-apps`, `--benchmark-load-requests`, and `--benchmark-concurrency` scope

`--benchmark-apps` controls how many independent or routed apps the load
is spread across for `benchmark-resident`, `benchmark-multi-app`, and
`benchmark-orchestrator-contract` only; it has no effect on
`benchmark-on-demand`/`benchmark-prewarmed`, which always run a single
app. `--benchmark-load-requests` controls the aggregate request count
distributed evenly across those `--benchmark-apps` apps for the same
three benchmarks. `--benchmark-concurrency` is always an aggregate figure
for every `benchmark-*` demo; for the three load benchmarks it must not
exceed the effective request total for that demo
(`--benchmark-load-requests`), while for `benchmark-on-demand`/
`benchmark-prewarmed` it must not exceed `--benchmark-requests` instead.

### `--benchmark-vms` and `--benchmark-pool-vms` scope

`--benchmark-vms` (default: physical cores) bounds concurrent
request-execution VMs, and `--benchmark-pool-vms` (default: physical
cores + 1, must be at least 2) sets the prewarmed ready inventory — both
apply **only** to the legacy `benchmark-on-demand`/`benchmark-prewarmed`
demos (`--benchmark-pool-vms` further only affects `benchmark-prewarmed`;
it is ignored by the on-demand server). Neither flag is read anywhere by
`benchmark-resident`, `benchmark-multi-app`, or
`benchmark-orchestrator-contract` — passing them alongside those three
demos has no effect, because resident-pool sizing for those benchmarks is
derived instead from the per-app allocated concurrency (see
`--benchmark-concurrency` above), not from `--benchmark-vms`/
`--benchmark-pool-vms`.

### `--benchmark-hey-parallel` and `--benchmark-hey-ramp-ms` scope

Both apply to **every** `hey` launch site in this script, not just one
demo family: the three load benchmarks (`benchmark-resident`,
`benchmark-multi-app`, `benchmark-orchestrator-contract`'s phase A), the
legacy `benchmark-on-demand`/`benchmark-prewarmed` single-process
benchmarks, and `benchmark-orchestrator-contract`'s phase B drain test.
`--benchmark-hey-parallel N` (or `HYPERLIGHT_BENCHMARK_HEY_PARALLEL`,
default `--benchmark-apps`) bounds how many `hey` processes run
concurrently in one wave for the three load benchmarks; by default this
equals `--benchmark-apps`, so every app's `hey` process launches in a
single synchronized wave rather than sequential bounded batches — lower
it explicitly to restore bounded wave-splitting. It has no visible effect
on the legacy demos (always exactly one `hey` process) and does not
change phase B's batch size (phase B always launches every app's `hey` in
one ungated batch, uncapped, for reasons explained above).
`--benchmark-hey-ramp-ms N` (or `HYPERLIGHT_BENCHMARK_HEY_RAMP_MS`,
default 10) delays each paused job's release within its batch by its
position times this value, regardless of which demo launched it, so the
default single-wave launch above ramps up instead of releasing every job
at the same instant; set it to 0 for the old all-at-once release.

### `--benchmark-load-driver {hey|vegeta}`

`benchmark-resident`'s load phase defaults to `hey` (or
`HYPERLIGHT_BENCHMARK_LOAD_DRIVER`, default `hey`): one `hey` process per
app, the per-app/per-wave model described above, unchanged. Passing
`--benchmark-load-driver vegeta` instead runs exactly ONE `vegeta attack`
process against a single shared targets file listing every app's exact
request count, round-robin interleaved across apps, avoiding the
N-process fork/barrier/progress overhead of one `hey` process per app —
this is what makes high `--benchmark-apps` counts (hundreds to
thousands) practical, where per-process fork/barrier overhead with `hey`
previously dominated wall-clock time. `--benchmark-load-driver` has no
effect on `benchmark-multi-app` or `benchmark-orchestrator-contract`,
which always use `hey`; `require_runtime` only requires the `vegeta`
binary on `PATH` when `--benchmark-load-driver vegeta` is selected.
`tools/setup-workerd-demo.sh --install-deps` `go install`s `vegeta`
automatically (same as it already does for `hey`), so a normal setup
run makes both drivers available without any extra steps.

Under vegeta, per-app results are synthesized back into the exact same
`hey`-report-format text file (`Total:`/`Average:`/`Requests/sec:`/
`[200] N responses`) that `hey` itself would have produced, from
vegeta's own `report -type=json` (overall duration) and
`encode -to=json` (per-request records, grouped by target URL) — so
every downstream consumer (`build_app_summary`, the collected
`hey.txt`, `summary.json`'s aggregate fields) works unmodified
regardless of which driver ran. Since vegeta delivers each app's exact
requested count via a finite target list (no `hey`-style "count must be
a multiple of concurrency" constraint), the achievable-count snapping
and `conservation-remainder` extra-job mechanism described above (the
`hey`-only paragraph just above) is skipped entirely for vegeta: every
app's allocated request count is used as-is, and no
`conservation-remainder` artifacts are ever produced for a
vegeta-driven run. Because vegeta runs everything as one shared
process/timeline rather than hey's per-app/per-wave model,
`summary.json`'s `aggregate.max_parallel_hey_jobs`, `hey_waves`, and
`barrier_scope` are always `null` for a vegeta-driven run, and
`aggregate.load_driver` records `"vegeta"` so a `summary.json` alone
says which driver produced it; `hey_self_measured_requests_per_sec` and
the other aggregate throughput numbers remain meaningful for either
driver (vegeta's single-attack timing plays the same role hey's
barrier-anchored timing does for `active_wave_requests_per_sec`/
`end_to_end_requests_per_sec`, since there is only one "wave").

### Raw `hey` output, logs, timing, and throughput metrics

Each of the three load benchmarks saves the complete raw `hey` report for
every app: a per-app file (`demo-output/<demo>/app-N/hey.txt` for
`benchmark-resident`, `demo-output/<demo>/app-N-hey.txt` for
`benchmark-multi-app`) plus a single collected top-level `demo-output/
<demo>/hey.txt` concatenating all per-app reports with `==== app: NAME
====` separators. For `benchmark-resident` and `benchmark-multi-app`,
the presenter prints this collected report inline under a
`RAW HEY OUTPUT` heading only when exactly one hey client ran
(`--benchmark-apps 1`); with more than one hey client it prints a short
note instead (the per-app and collected files on disk are unaffected —
only the terminal/log inline dump is omitted), since concatenating many
independent per-app raw reports into the terminal does not scale and is
rarely what's needed there. A deliberately-concatenated multi-app
raw-hey report is a presentation of independent per-app `hey` runs
placed one after another, not a single merged statistical distribution
— `hey` reports cannot be averaged or combined after the fact into one
true aggregate latency/percentile distribution, which is why the two
numeric aggregate metrics below are computed independently rather than
parsed out of the concatenated text.

For `benchmark-resident` and `benchmark-multi-app`, `hey` itself truncates
a client's delivered request count to a multiple of its `-c` concurrency
(e.g. `hey -n 31 -c 2` sends only 30 requests), so per-app request/
concurrency shares are jointly allocated to keep every app's count an
exact multiple of its own concurrency while still summing to exactly the
requested `--benchmark-load-requests` total. On the rare combination of
app count, concurrency, and request total where that isn't exactly
expressible (every app sharing one concurrency value with a deficit that
isn't a multiple of it), the shortfall is covered by one extra
concurrency-1 `hey` job against the first app, reported as its own
`summary.json` entry (`bench-0-remainder` / `app-0-remainder`) and its
own `demo-output/<demo>/conservation-remainder-hey.txt` file — never
silently dropped from the reported total.

Each demo also writes a `run.log` recording start/stage/failure lines
(including the lifecycle-limit values noted above, plus the effective
`hey_max_parallel_jobs`/`hey_ramp_ms` for the run) and a `timing.json`-
shaped `timing` object inside `summary.json`, with `setup_seconds`,
`load_seconds`, `teardown_seconds`, `total_seconds`, `wave_active_seconds`,
and `load_wall_seconds` phase durations (the last two are the
barrier-anchored measurements described below, timed with a true
monotonic clock — `/proc/uptime` on Linux/WSL, immune to wall-clock/NTP
adjustments — falling back to wall-clock only if `/proc/uptime` is
unavailable).
`hey` itself is always launched as a separate OS process per app; by
default every app's `hey` process launches in one synchronized wave
(`hey_max_parallel_jobs` defaults to `--benchmark-apps`, overridable with
`--benchmark-hey-parallel`/`HYPERLIGHT_BENCHMARK_HEY_PARALLEL` — see
above); lowering that cap below the app count makes later apps' `hey`
runs happen in a later sequential wave rather than fully overlapping the
first wave. **The release barrier is scoped to one wave**: when more
than one wave runs, there is no single barrier spanning every app —
only the jobs within a given wave are pre-spawned paused (forked but
blocked behind that wave's own gate file) and released together.
Within a wave, once every job is forked, a timestamp is taken
immediately before the gate file is created, so none of that wave's own
forking/spawn cost is ever counted as load; staggered by default
(`hey_ramp_ms` defaults to 10, overridable with
`--benchmark-hey-ramp-ms`/`HYPERLIGHT_BENCHMARK_HEY_RAMP_MS` — see above,
set to 0 to release every job in a wave at once, which happens after the
barrier timestamp so it is correctly counted as load, not startup);
`run.log` records each job's queue/release and each wave's
queue/release/finish lines, with the finish line's elapsed time now
labeled as barrier-release-to-wave-finish. `summary.json`'s
`aggregate.hey_waves` reports the wave count and `aggregate.barrier_scope`
is always `"per_wave"` when timing data is present, to make this
single-wave-only guarantee explicit rather than implying every app
launched simultaneously.

`summary.json`'s `aggregate` object reports four different throughput
numbers because of that wave behavior:

- `sum_of_app_requests_per_sec` adds up each app's own independently
  measured `hey` requests/sec. This is only a reasonable approximation of
  true aggregate throughput while every app's `hey` process actually ran
  concurrently — true by default now that every app launches in one
  wave, unless `--benchmark-hey-parallel` is explicitly lowered below the
  app count, in which case apps beyond that cap run in later waves whose
  rates this sum keeps adding even though they did not overlap in
  wall-clock time, overstating true throughput by roughly the wave count.
- `end_to_end_requests_per_sec` divides the total request count by
  `timing.load_wall_seconds`: (first wave's barrier-release instant →
  last wave's finish), a single span that still excludes only the first
  wave's pre-release forking cost but DOES include every inter-wave gap
  (post-wave gate-file cleanup, next wave's `mktemp`/fork setup, etc.).
  This is the TRUE end-to-end number — the one to report/compare as a
  run's overall throughput — matching what an external observer watching
  the whole load phase actually experiences once `hey_waves > 1`.
- `active_wave_requests_per_sec` divides the total request count by
  `timing.wave_active_seconds`: the sum, across every wave, of
  (barrier-release instant → that wave's last job finishing). This
  excludes every wave's pre-release forking/spawn cost AND every
  inter-wave gap, so it measures only the time `hey` processes were
  actually permitted to send requests — a narrower "pure load-serving
  only" number, useful for isolating request-serving cost from
  inter-wave orchestration overhead, but NOT what "end to end" means
  here. `end_to_end_requests_per_sec` and `active_wave_requests_per_sec`
  are equal when `hey_waves == 1` (no inter-wave gap exists).
- `hey_self_measured_requests_per_sec` is computed ENTIRELY from each
  `hey` process's own self-reported output — no orchestrator wall-clock
  timestamps involved at all. It divides `aggregate.actual_successful_
  responses` (the sum, across apps, of each app's own "Status code
  distribution: `[200] N responses`" count — requests `hey` itself
  confirmed got a 200, not the configured/requested count) by
  `aggregate.max_app_hey_total_seconds` (the largest of each app's own
  self-reported `Summary: Total: X secs` duration — the straggler app's
  own measured span). Since every app in a wave is released from the
  same barrier at effectively the same instant, this closely tracks
  `active_wave_requests_per_sec`/`end_to_end_requests_per_sec`; a large
  divergence from those orchestrator-measured numbers is worth
  investigating (clock skew, barrier-release jitter, a wall-clock
  instrumentation bug), while small differences are expected since
  `hey`'s own clock and this script's monotonic-clock reads are
  independent measurements of almost (but not exactly) the same span.

Both `end_to_end_requests_per_sec` and `active_wave_requests_per_sec` are
`null` when timing/wave data is unavailable for a phase (e.g.
`benchmark-orchestrator-contract`'s phase summaries do not currently
measure them). `hey_self_measured_requests_per_sec` (and
`max_app_hey_total_seconds`) are `null` whenever no app reported any
successful response, independent of whether timing/wave data is present.

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
