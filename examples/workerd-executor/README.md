# Workerd executor foundation (v0.14 prototype ABI)

This harness packages a **trusted external workerd-fork executor** or a tiny
native mock, not stock workerd. Static executors use the dedicated v0.14
direct-initrd elfloader; dynamic executors retain the general CPIO/VFS kernel.
Both use the existing 64 KiB transport and Hyperlight 0.17 without feature changes.
It grants neither hostfs nor hostsock by default. The committed
general-purpose kernel contains those drivers, but the wrapper registers no
`fs_*` or `net_*` host functions unless the Rust host supplies an explicit
storage or network policy.

For a complete clean-VM Azure KVM walkthrough, including executor packaging,
WinterTC capability routes, isolation/timeout checks, `hey` load, adaptive
prewarm diagnostics, profiling, and cost cleanup, see
[`docs/azure-workerd-hyperlight-runbook.md`](../../docs/azure-workerd-hyperlight-runbook.md).
Pinned package tarballs and deterministic assertions for the representative
Node compatibility workloads are recorded in
[`workerd-node-workload-pins.json`](workerd-node-workload-pins.json) and
[`workerd-node-workload-acceptance.json`](workerd-node-workload-acceptance.json).
These manifests define acceptance; they do not claim the workloads passed.

## Build

`just guests` builds the native fixture with Linux gcc and cpio (Ubuntu-24.04
WSL on Windows) into `build-elfloader/workerd-executor-fixture`, without
overwriting a packaged real executor. No Zig wrappers are used.
To package an external static-PIE x86-64 Linux executor, run:

```sh
bash examples/workerd-executor/build-rootfs.sh /path/to/workerd-executor
```

Outputs are `build-elfloader/workerd-executor/rootfs.img` and the matching
`executor`. Static PIE is preferred: the executor itself becomes the raw
initrd, avoiding a full CPIO-to-RAMFS copy before ELF loading. For a trusted
dynamic PIE, the image is a minimal CPIO containing only the executor, its
explicit ELF interpreter, and resolved `ldd` closure at their exact guest
paths; unresolved or ambiguous dependencies are rejected. `rootfs.cpio` is a
compatibility alias with identical contents. The deterministic closure digest
is recorded in the snapshot binding alongside the authoritative image and
executor digests. Supply artifacts from a trusted build: recording hashes
does not prove an arbitrary image contains the separately supplied executor.

Once packaged, start the minimal host HTTP bridge with:

```sh
cargo run --example workerd-demo -- \
  --bundle examples/workerd-bundles/helloworld_esm.json \
  --bind 0.0.0.0:8787 \
  --restore-mode prewarmed --prewarmed-sandboxes 8 \
  --max-concurrent-restores 2 \
  --warm-floor 1 --ready-low-watermark 4 \
  --ready-high-watermark 8 --max-replenish-batch 2 \
  --max-concurrent-sandboxes 4 --queue-capacity 64 \
  --profile-log-every 64
curl http://127.0.0.1:8787/
```

`--bundle` loads trusted deployment JSON. For a convenient one-file Worker,
use `--script worker.js --version my-worker-v1 --compatibility-date
2025-01-01`. The source is serialized only into the one `init` call and is
never included in a request or resent after snapshotting.

The main CLI also exposes a one-request bundle path:

```sh
hluk workerd --bundle examples/workerd-bundles/helloworld_esm.json \
  --url https://example.test/
```

The bridge defaults to `--restore-mode on-demand`, which preserves the original
behavior: each owner restores, runs, joins the watchdog for, and drops one fresh
request VM. `--restore-mode prewarmed` instead makes each owner restore its own
VM before advertising readiness, execute at most one request in it, destroy it,
and replenish. `--prewarmed-sandboxes` controls owner/ready capacity and the
number of reserved VMs (and therefore reserved VM/RSS footprint). It may be
larger than `--max-concurrent-sandboxes`, which remains the active execution cap
so replenishment can overlap guest execution. In prewarmed mode,
`--max-concurrent-restores` (default 1) separately bounds restore CPU pressure;
owners waiting for a restore permit block rather than spin.
`--warm-floor` (default 1) reserves that many fully restored ready VMs from
dispatch, so shutdown is the only path that destroys the final warm VM.
Crossing below `--ready-low-watermark` starts an adaptive refill toward
`--ready-high-watermark`; `--max-replenish-batch` bounds scheduler-issued
restore permits. Owners without a VM sleep until the central scheduler grants
a permit, and requests never restore on the listener or completion thread.
Restored owners
publish a mailbox handle into one bounded central ready queue. A single
dispatcher atomically pairs a queued request with a ready owner under the
active cap and sends the request to that owner's mailbox. Owners never compete
on the request queue and VMs never move between OS threads. Both prewarm-only
sizing flags are rejected in on-demand mode, where owner and effective
execution concurrency equal `--max-concurrent-sandboxes`.

`--diagnostic-no-refill-wave N` pre-fills every configured owner, pauses normal
replacement restores for the first `N` dispatched requests, and resumes
adaptive replenishment after all `N` complete. The warm floor still overrides
the pause. Use this only with an exact `-n N -c N` diagnostic wave.

Budget active execution and restore together for the host. For example, a
32-core host could use 28 active sandboxes plus 4 concurrent restores, subject
to measured guest CPU and memory headroom. Startup logs report active, owner,
and restore counts; per-request profiles report ready wait and restore time.
`/__hyperlight/pool-status` reports the selected mode, warm floor, ready
watermarks, refill batch, pause state/reason, idle owners, outstanding restore
permits, diagnostic-wave progress, actual prewarmed VM inventory, ready and
replenishing counts, and cumulative pairing hits and misses. Each restored VM
records exactly one result when it takes a request: a
hit when it was already ready at admission, or a miss when the request arrived
before that VM finished restoring. Those prewarmed metrics remain zero in
on-demand mode. Startup and status also distinguish the configured active cap,
owner count, resolved `prewarmed_sandboxes`, resolved
`max_concurrent_restores`, and effective concurrency
(`min(owners - warm floor, active cap)`). The owner and restore configuration
fields remain `null` in on-demand mode; policy gauges report zero or false.

Status also exposes admitted requests, current execution/restore occupancy,
recycle queue depth, teardown activity, completion queue depth and callbacks in
flight. Peak/count/total/average/maximum timing fields cover teardown,
completion, restore-permit wait and restore execution; restore attempt,
successful and failed counts are separate. Ready and replenishing observed
minima/peaks remain cumulative from startup. Restore summaries include every
restore attempt; `completed_restores` counts only successful restores. These
fields let a 10 ms sampler distinguish restore pressure, recycle backlog,
teardown, and slow response writes even when all current gauges are zero at
phase boundaries.

The execution permit covers guest execution, watchdog join, response
validation, and one-shot VM teardown. Inventory and active accounting are
updated and the permit is released before the user completion callback formats
profiles or writes the HTTP response. Completion runs on a separate bounded
completion executor, so the owner immediately enters recycle/restore scheduling
and slow client writes consume neither owner restore time nor active execution
capacity. Admission remains counted until completion finishes, bounding the
completion backlog together with queued and executing work.

`--profile-log-every N` controls deterministic `ExecutionProfile` logging on
the response path. The default `1` preserves logging for every request, `0`
disables it, and `64` logs request sequences 1, 65, 129, and so on. Each line
includes the request sequence, request ID, and monotonically increasing sample
number. Pool status exposes both the configured interval and
`profile_samples_logged`. Benchmark both `1` and `0` once to quantify logging
overhead, then use one matched sampled interval for comparative phase evidence.

`ExecutionProfile.ready_wait_ms` remains the total time from bounded admission
to owner dispatch. It is split into `admission_wait_ms` (waiting behind earlier
work or the active cap) and `ready_owner_wait_ms` (front-of-queue with an
execution slot but no dispatchable ready owner). The next request handled by a
recycled owner reports its scheduler-permit delay separately as
`replenishment_policy_wait_ms`; `replenishment_wait_ms` remains restore-slot
waiting.

Requests above the active limit wait in a bounded queue (`--queue-capacity`,
default 64); admission never waits for queue space, and overflow receives HTTP
503. The bridge grants no guest filesystem or networking capability. Use
`--request-timeout-ms` to set the watchdog deadline; a timed-out request receives
HTTP 504 and later requests continue in fresh VMs. All capacities must be
nonzero.

## Workerd-fork executor ABI

Use `drivers/hl_driver.h` and `drivers/hl_fc.h`, as the fixture does. Open
`/dev/hlcall`, size the call buffer with `HLCALL_IOC_MAXLEN`, and read complete
size-prefixed Hyperlight FunctionCall FlatBuffers. Returning to the next read
completes the call; write an `int32_t` nonzero status to fail it.

* `init(init_json)` loads exactly one Worker version. Without named storage,
  the argument is canonical `WorkerBundle` JSON with `protocol_version: 1`.
  With named storage, it is canonical `ExecutorInit` JSON with
  `protocol_version: 2` and a required sorted `storage` array containing only
  logical `name` and `mode` values. Startup before this call is trusted and
  must not execute tenant code. The wrapper snapshots only after successful
  init; the initialized driver remains alive. The source bundle is never sent
  again.
* `fetch(request_json)` carries protocol 1, `request_id`, `method`, `url`,
  ordered `headers` (`name`, `value`), and `body_base64`.
* During fetch, stdout is **protocol-only**: exactly one JSON response followed
  by one LF. The v0.14 console translates LF to CRLF before HostPrint; the
  collector requires exactly that CRLF, never arbitrary trailing whitespace.
  Fields: `protocol_version`, matching `request_id`, `status`,
  `headers`, `body_base64`. No leading/trailing whitespace outside the object,
  prefix, suffix or extra line. Arbitrarily split HostPrint chunks are accepted.
  Any malformed, oversized, stale or duplicate response poisons the request,
  even if the guest ignores a negative HostPrint acknowledgement.

```json
{"protocol_version":1,"request_id":"r-1","status":200,"headers":[],"body_base64":"b2s="}
```

The `WorkerBundle` JSON object has exact top-level field order
`protocol_version`, `worker_version`, `compatibility_date`,
`compatibility_flags`, `main_module`, `modules`. Module field order is
`name`, `type`, `source`:

```json
{"protocol_version":1,"worker_version":"hello-v1","compatibility_date":"2025-01-01","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default { fetch() { return new Response('ok') } }"}]}
```

The `ExecutorInit` JSON object has the same fields in the same order, followed
by `storage`. Each storage entry has exact field order `name`, `mode`; entries
are sorted by `name`, and `mode` is exactly `ro` or `rw`:

```json
{"protocol_version":2,"worker_version":"hello-v1","compatibility_date":"2025-01-01","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default { fetch() { return new Response('ok') } }"}],"storage":[{"name":"readonly","mode":"ro"},{"name":"scratch","mode":"rw"}]}
```

The host parses trusted JSON, rejects unknown/duplicate/invalid values, sorts
flags, places the main ES module first, sorts remaining modules by name, and
serializes compact canonical JSON. Types are exactly `esModule`,
`commonJsModule`, `wasm`, `text`, and `json`; the main module remains an ES
module. Wasm `source` is canonical base64 and is decoded before Workerd module
registration. There are at most 32 flags and 32 modules; each decoded module
and aggregate decoded/text source are at most 48 KiB. Module names are safe
relative import paths. The final escaped JSON and Hyperlight FlatBuffer remain
within the unchanged 64 KiB transport (60 KiB JSON limit).

This replaces the old callback-pointer and JSON `/dev/hcall` ABI, which the
v0.14 kernel does not provide. `HostPrint` transports stdout in chunks; there
is no separately registered `worker_response` function. Diagnostics belong on
stderr, **but this kernel multiplexes stderr and stdout onto HostPrint**, so
the executor must defer diagnostics until outside fetch. A dedicated typed
guest-to-host return channel is a later ABI improvement, not part of this slice.

Limits: 60 KiB serialized JSON, 32 KiB decoded body, 64 headers, 8 KiB aggregate
header data, 8 KiB URL, 32-byte method, 64-byte request ID, 256-byte Worker
version ID. Serialization escaping/base64 and actual FlatBuffer framing are
accounted for; the whole encoded call fits the unchanged 64 KiB transport.
The response stream allows only two additional bytes for the console CRLF. Unknown JSON
fields and invalid base64 are rejected. Fetch/D1 traits are extension
interfaces only unless the host installs the outbound fetch broker described
below.

### Outbound fetch broker v1

Outbound access is denied by default. A configured `WorkerVersionSandbox`
registers a host-owned broker; no policy is sent to, visible to, or mutable by
the guest. The demo's `--fetch-loopback-port PORT` option allows only HTTP to
`localhost:PORT`. The demo also accepts repeatable `--fetch-allow-host`,
`--fetch-allow-scheme`, and `--fetch-allow-port` options, with separate
loopback, private, and metadata opt-ins and bounded broker-limit overrides.
The shorthand and general policy forms are intentionally mutually exclusive.
Production callers must construct an explicit `FetchPolicy`.

The executor invokes registered host functions through the generic
`HLCALL_IOC_HOSTCALL` ioctl on `/dev/hlcall`. `drivers/hl_driver.h` defines the
versioned ioctl structures and helpers. Calls are synchronous and bounded, but
network work is not: these functions only create, feed, poll, read or cancel an
independently abortable host task:

* `WorkerdFetchV1Start(String) -> String`
* `WorkerdFetchV1Write(u64, Vec<u8>) -> i32`
* `WorkerdFetchV1Finish(u64) -> i32`
* `WorkerdFetchV1Poll(u64) -> String`
* `WorkerdFetchV1Read(u64, u64) -> Vec<u8>`
* `WorkerdFetchV1Cancel(u64) -> i32`

Start metadata is protocol-1 JSON with `request_id`, `method`, `url`,
`header_block_length`, and decoded `body_length`. Write carries first exactly
`header_block_length` bytes of UTF-8 JSON containing the ordered header array,
then exactly `body_length` raw body bytes, in chunks of at most 60 KiB. Start
returns an operation ID between 1 and 2^53-1 or an error. Poll returns
`receiving`, `pending`, or `complete`; a completed response contains
`request_id`, `status`, `header_block_length`, and `body_length`. Read returns
the response's ordered-header JSON block followed by its raw body. Cancel is
idempotent for an ID that was issued by that request VM.

Limits are a 16 KiB URL, 32-byte method, 256-byte request ID, 128 headers and
64 KiB aggregate header data, 1 MiB request body, 4 MiB response body, and 16
concurrent operations per Worker version. The broker's total deadline is 10
seconds, clamped to the request VM watchdog deadline; connect time is at most 2
seconds. Automatic HTTP redirects are disabled, so Workerd observes each 3xx
and each follow-up is independently resolved and authorized. Errors use
`invalid_request`, `policy_denied`, `dns_failed`, `connect_failed`, `timeout`,
`cancelled`, `response_too_large`, `redirect_limit`, or `overloaded`.

### Outbound fetch broker v2

Protocol 2 keeps the same host-owned policy, DNS authorization, redirect
behavior, deadlines, cancellation and concurrency admission, but streams both
directions with bounded four-chunk queues instead of imposing v1's total body
limits. The exported calls are:

* `WorkerdFetchV2Start(String) -> String`
* `WorkerdFetchV2Write(u64, Vec<u8>) -> i32`
* `WorkerdFetchV2Finish(u64) -> i32`
* `WorkerdFetchV2Poll(u64) -> String`
* `WorkerdFetchV2Read(u64, u64) -> Vec<u8>`
* `WorkerdFetchV2Cancel(u64) -> i32`

Start accepts exactly `protocol_version`, `request_id`, `method`, `url`,
`header_block_length`, nullable `body_length`, `preferred_write_chunk`, and
`preferred_read_chunk`. Success returns a positive JSON-safe `operation_id`
and the effective `max_write_chunk` and `max_read_chunk`; failure returns zero
for all three numeric fields. The effective limits are the minimum of the
guest preference, host configuration, and ABI-safe maxima: 61,440 write bytes
and 61,439 read payload bytes.

Write is all-or-nothing. A positive result is the accepted byte count;
`-EAGAIN` means bounded upload backpressure, `-EPIPE` means upload is closed or
the upstream response began early, `-EFBIG` means the chunk or known body
length was exceeded, `-EINVAL` means the operation phase is invalid, and
`-ENOENT` means the ID is unknown. Finish closes the upload and verifies a
known body length. Poll reports `receiving_headers`, `uploading`, `response`,
or `complete`; response metadata is visible before the full body arrives.

Read's `maxBytes` is the maximum **data payload**, so the returned vector may
contain `maxBytes + 1` bytes including its tag. Tag `0` is pending and tag `2`
is EOF, each exactly one byte. Tag `1` is followed by response bytes, with the
complete ordered-header JSON block preceding body bytes. Reading EOF collects
a successful handle. Cancel aborts DNS, connect, upload, or download work and
request-VM teardown cancels every remaining operation.

### Named storage policy

Workerd storage is denied by default: no mounts means no `fs_*` host
functions. The demo accepts repeatable `--storage-ro NAME=HOST_DIR` and
`--storage-rw NAME=HOST_DIR` options. The Rust host canonicalizes each
directory before VM construction, validates the logical name, and mounts it
only at `/mnt/workerd-storage/NAME`. Worker input never supplies a host path.
Duplicate names, nonexistent/non-directory host paths, malformed names, and
limits for unknown bindings fail launch. Policies contain at most eight
bindings; names are lowercase ASCII letters, digits, and `-`, start with a
letter, and are at most 32 bytes.

Each binding has per-sandbox host-side budgets. Defaults are 128 operations,
1 MiB read, and 64 KiB write; repeatable
`--storage-max-operations NAME=N`, `--storage-max-read-bytes NAME=N`, and
`--storage-max-write-bytes NAME=N` options override them within launcher
caps. Valid-mount calls consume one operation before filesystem access.
Successful reads charge bytes actually returned. Writes reserve and charge
the submitted payload before I/O, including a payload whose underlying I/O
later fails. Extending a file with `truncate` charges the requested growth;
shrinking it charges no write bytes. A denied read-only mutation returns
`EROFS` before quota accounting. Exhaustion returns stable Linux `EDQUOT`.
Counters are independent per mount and are recreated for every fresh or
snapshot-restored VM, so every one-request owner begins with the same full
budget.

`cap_std::fs::Dir` confines every host operation beneath the canonical opened
directory. The policy does not claim race-free aggregate storage, maximum file
size, or file-count quotas: the stateless `fs_*` host functions can enforce
operation and transferred-byte budgets, but concurrent external changes and metadata-only
operations prevent authoritative capacity accounting. `CHUNK=32768` remains
only the per-call transfer bound.

The pinned Workerd fork implements a substantial Node compatibility surface
on V8 when `nodejs_compat` and the required feature flags are enabled; it does
not embed the Node.js runtime. Its Worker-visible `node:fs` API exposes the
in-memory Workerd VFS (`/bundle`, `/tmp`, `/dev`). Many npm packages can use
that compatibility surface, but general Node process/OS, native-addon,
child-process, and host-filesystem parity is not implied.

Hyperlight hostfs is a different guest-kernel facility. A real Worker can see
named host-backed storage only when the external `workerd-sandbox-executor`
attaches `/mnt/workerd-storage/NAME` into Workerd's VFS. The `init` guest call
receives an `ExecutorInit` JSON object with `protocol_version: 2`; it carries
only sorted `{name,mode}` entries, never host paths. The executor maps each
validated guest directory to `/storage/NAME`. The native fixture
exercises the guest mount and quota boundary directly; final real-Hyperlight
storage/VFS evidence passes the complete 18/18 suite.

The `ExecutorInit` object is Workerd-specific serialization for bundle, module,
and named-storage initialization. Per-operation storage access instead invokes
the registered Hyperlight host functions `fs_stat`, `fs_read_bytes`,
`fs_write_bytes`, and the other `fs_*` functions; those guest-to-host function
calls are not `ExecutorInit` fields or messages.

Persistent KV/SQL is not implemented by this mount policy. A future broker
should keep logical authorization, credentials, connection pools,
transactions, and quotas in the trusted Rust host. SQLite databases and WALs
belong on durable host storage, one host-owned database per namespace;
MySQL/PostgreSQL remain external services reachable only by the broker. Never
mount a live database into disposable request VMs or enable direct Worker
database connections by default.

Today the fetch, timer, and hostfs brokers are in-process Rust objects and
registered host-call closures in the trusted `workerd-demo` process, not
separate daemons. Immutable policy and process-level backend/concurrency state
may be shared; each restored one-request VM receives fresh fetch/timer sessions
and hostfs quota counters that are dropped with that VM. Prewarmed owners
create them during background restore, while on-demand owners create them
during request restore. A future database broker should follow the same
process-level backend plus per-sandbox capability-session model.

### Monotonic timer channel v1

The generic host-call bridge also exposes one-shot monotonic deadlines. This is
the Hyperlight side of the Workerd timer adapter; it adds no timer ioctl, kernel
mechanism, background host thread or blocking host call:

* `WorkerdTimerV1Start(u64 delay_ns) -> String`
* `WorkerdTimerV1Read(u64 timer_id) -> String`
* `WorkerdTimerV1Cancel(u64 timer_id) -> i32`

All JSON objects require exactly their documented keys; key order is not part
of the contract. Start succeeds with
`{"protocol_version":1,"timer_id":N,"state":"pending","error":null}`.
`N` is session-local, never reused and between 1 and 2^53-1. A zero delay is an
immediately due timer. If adding the full `u64` nanosecond duration to the
monotonic clock overflows, Start returns timer ID zero, state `error`, and
`invalid_duration`. Admission failures use `overloaded`.

Read never waits. It returns state `pending`, or terminal state `fired` or
`cancelled`; a terminal Read releases the handle. Reading an unknown or
released ID returns state `error` with `unknown_timer`. The error object is
`{"code":"invalid_duration|overloaded|unknown_timer","message":"..."}`.

Cancel returns zero for any issued, unreleased handle, including one already
cancelled. Cancellation wins until terminal delivery: even if the deadline has
elapsed, Cancel marks the timer cancelled when Read has not yet returned and
released `fired`. A later Cancel on a released or unknown handle returns
`-ENOENT`. Workerd combines nonblocking Read with the guest monotonic
sleep/yield path; the existing Hyperlight step model parks the halted VM until
the guest deadline rather than blocking inside a host call or spinning.

`TimerLimits` defaults to 1,024 active pending timers per Worker version and
4,096 unreleased handles per request VM. Hosts can supply a configured
`TimerLimits` through `WorkerVersionSandbox::initialize_with_capabilities`;
zero limits and an unreleased-handle limit below the active limit are rejected.
Cancelled handles release active admission immediately but retain session
handle capacity until terminal Read or VM teardown. Each fresh request VM gets
an isolated timer session. Watchdog expiry and session/VM drop cancel and
release every remaining handle before the next request can run.

## Snapshots, deadlines and trust

`WorkerVersionSandbox::initialize` creates an initialized, version- and
bundle-bound snapshot. The SHA-256 identity of canonical bundle JSON is part
of `SnapshotBinding`, so changing JavaScript, compatibility configuration, or
module content prevents snapshot reuse. `snapshot().save(new_directory)` saves
the v0.14 OCI layout and
read-only `worker.json`; it refuses an existing directory.
`VerifiedSnapshot::open(directory, expected_binding)` checks the full trusted
binding (Worker version, canonical bundle, embedded kernel, rootfs, executor,
capability set),
metadata/protocol/host versions and OCI manifest/config/layer SHA-256 digests.
The metadata is immutable through the Rust API.

Read-only files are accidental-mutation protection, **not authenticity or
protection from the file owner**. Store layouts in a trusted immutable store.
Hyperlight memory-maps the layer: never overwrite, delete or truncate it
while a snapshot or sandbox uses it. Do not open untrusted snapshots.

Every fetch uses a fresh VM from the initialized snapshot and drops it on
success or failure. InterruptHandle kills a spinning VM at the deadline;
bounded host waits also cover guest sleeps. The watchdog is joined and
request/output state cleared before another request can run. Response state is
created per request; unrelated VMs do not share a response collector or mutex.
A killed VM is never reused, and an instance cannot be assigned another Worker
version.
Artifact loading and VM construction are not covered by the execution timeout.
Repeated request IDs are allowed across completed requests; correlation and
duplicate-response checks apply to the one active request, not a replay store.

KVM documents vCPU ioctls as thread-affine: they should be issued from the
thread that created the vCPU unless an ioctl is explicitly asynchronous, and
switching threads can impose a first-ioctl penalty. The pool therefore does not
move VMs through an async executor: restore/create, `KVM_RUN` handling, normal
vCPU operations, teardown, and destruction all remain on one named
`workerd-sandbox-N` thread for a request. Hyperlight's existing watchdog is the
only cross-thread interaction. On Linux it calls `pthread_kill` with Hyperlight's
configured real-time signal; the signal makes the owner's `KVM_RUN` return
`EINTR`, and the owner performs all subsequent vCPU handling. The watchdog does
not issue ordinary vCPU ioctls and is joined before VM destruction. See the
[Linux KVM API](https://www.kernel.org/doc/html/latest/virt/kvm/api.html)
(`KVM_RUN`, `KVM_SET_SIGNAL_MASK`) and
[the kernel API source](https://github.com/torvalds/linux/blob/master/Documentation/virt/kvm/api.rst).

`execute_profiled()` reports each host-side phase separately:

* `ready_wait_ms`: time from request admission until an owner starts it.
* `replenishment_wait_ms`: time that prewarmed owner waited for a bounded
  restore permit, exposing restore contention. Together with
  `replenishment_restore_ms`, this describes creation of the one-shot VM assigned
  to the request; for a hit, that work completed before admission and is not
  request critical-path latency.
* `replenishment_restore_ms`: restore time paid by a prewarmed owner before it
  advertised readiness.
* `snapshot_restore_ms`: clone the immutable snapshot handle and construct a
  fresh Hyperlight VM from it in on-demand mode. This remains zero for a
  prewarmed request because restore completed before admission to that VM.
* `request_setup_ms`: validate/serialize the request, clear request state,
  register `HostPrint`, and activate the request ID.
* `guest_execution_ms`: resume the restored guest and execute `fetch` under
  the watchdog.
* `vm_teardown_ms`: drop the per-request VM after the watchdog has joined.
* `response_finish_ms`: validate/take the completed response, or clear state
  after failure.
* `total_ms`: owner execution time after the request is paired with a VM. It
  excludes `ready_wait_ms`, includes `snapshot_restore_ms` in on-demand mode,
  and excludes prewarm replenishment that completed before execution. Do not
  sum `ready_wait_ms`, replenishment fields, and `total_ms` as if every field
  were on the same critical path.

The demo writes this profile as compact JSON to stderr at the interval selected
by `--profile-log-every`; the default still logs every request. The memory probe
includes profiles for hello, busy-loop timeout, and recovery.
The observed 60–100 ms restore portion is therefore not Worker JavaScript
execution: it is the intentional cost of constructing a fresh isolated VM,
restoring mapped snapshot state/page tables, and resuming it. Keeping that
boundary preserves kill/recovery semantics and prevents request-to-request VM
state reuse. Native Linux KVM measurements may differ from WHP.

## Native Linux / Azure KVM reproduction

Use a repository and Cargo target directory on the VM's native ext4 disk, not
an SMB mount, `/mnt/c`, or another Windows-backed filesystem:

```sh
case "$PWD" in /mnt/*) echo "clone the repository onto native ext4" >&2; exit 1;; esac
test "$(stat -f -c %T .)" = "ext2/ext3" || {
  echo "repository is not on ext4" >&2
  exit 1
}
test -c /dev/kvm && test -r /dev/kvm && test -w /dev/kvm
grep -E -m1 '(^flags|^Features).*(vmx|svm)' /proc/cpuinfo
export CARGO_TARGET_DIR="$HOME/.cache/hluk-v014-target"
```

Initialize the exact pinned sources, build the direct-initrd kernel, and
package the trusted executor artifact:

```sh
git submodule update --init --recursive
just build-workerd-kernel
export WORKERD_EXECUTOR=/absolute/ext4/path/to/workerd-sandbox-executor
bash examples/workerd-executor/build-rootfs.sh "$WORKERD_EXECUTOR"
sha256sum kernel/workerd_hyperlight-x86_64 \
  build-elfloader/workerd-executor/executor \
  build-elfloader/workerd-executor/rootfs.img
```

Run the optimized executor's 344 MiB real-V8 acceptance probe. Use 768 MiB only
as a diagnostic fallback when a 344 MiB failure is specifically attributable
to memory, not for functional adapter or bundle failures:

```sh
HYPERLIGHT_MAX_SURROGATES=2 HYPERLIGHT_INITIAL_SURROGATES=0 \
RUST_LOG=hyperlight_unikraft=debug \
cargo run --release --locked --example workerd-memory-probe -- 344 \
  --bundle examples/workerd-bundles/acceptance.json \
  2>build-elfloader/workerd-memory-344-kvm.stderr \
  | tee build-elfloader/workerd-memory-344-kvm.json
```

Do not descend to a lower allocator candidate until the complete 344 MiB
evidence matrix is green. Stop at the first failing candidate and retain every
machine-readable result.

Run the reproducible real-V8 bundle probes through initialization, snapshot,
fresh restore, and fetch:

```sh
cargo run --release --locked --example workerd-bundle-probe -- \
  examples/workerd-bundles/helloworld_esm.json https://example.test/ 344
cargo run --release --locked --example workerd-bundle-probe -- \
  examples/workerd-bundles/web-streams.json https://example.test/sync 344
cargo run --release --locked --example workerd-bundle-probe -- \
  examples/workerd-bundles/api-smoke.json \
  https://example.test/wintertc-smoke 344 POST x-smoke:yes
# Equivalent repeatable compatibility smoke:
just workerd-api-probe 344
```

`api-smoke.json` is a representative probe derived from Workerd's
machine-readable ECMA-429/WPT support matrix, not an authoritative conformance
runner. `api-smoke-matrix.json` records the selected pure Web APIs,
capability-backed APIs intentionally unavailable in this sandbox, untested
surfaces, the Hyperlight/executor ownership boundary, and the expected SHA-256
digest vector. The timer host channel is covered by the native executor fixture;
the Workerd KJ adapter and real-V8 timer qualification are separate. No
additional Hyperlight kernel or host capability is currently identified for
safe WebAssembly enablement, MessageChannel, File or BYOB streams: those remain
executor embedder-policy/API verification tasks, followed by this same real-VM
bundle probe.

For a curlable listener, run the demo in one terminal and issue requests from
another:

```sh
HYPERLIGHT_MAX_SURROGATES=2 HYPERLIGHT_INITIAL_SURROGATES=0 \
cargo run --release --locked --example workerd-demo -- \
  --bundle examples/workerd-bundles/acceptance.json \
  --bind 0.0.0.0:8787 --scratch-mb 344 --request-timeout-ms 500 \
  --max-concurrent-sandboxes 2 --queue-capacity 32
```

```sh
curl -i -X POST http://127.0.0.1:8787/hello -d hello
curl -i -X POST http://127.0.0.1:8787/busy -d busy
curl -i -X POST http://127.0.0.1:8787/after -d after
```

For a bounded load check, keep `hey` concurrency at or above the configured
sandbox count:

```sh
hey -n 200 -c 16 http://127.0.0.1:8787/hello
```

With the example configuration, up to two requests execute in parallel in two
separate VMs, the next 32 wait in the queue, and further simultaneous admissions
receive deterministic HTTP 503 responses. Increasing `hey -c` does not create
unbounded VMs or an unbounded host queue.

`cargo test --test workerd_sandbox` runs the real-hypervisor cases and **fails**
if artifacts/hypervisor access are missing; it does not silently self-skip.
Use `cargo test --lib workerd` for the hardware-independent tests.
Cross-Clippy is cfg/type validation only. This prototype claims neither W^X,
stock-workerd support, production hardening, nor platform runtime qualification
without an actual run on that platform.
