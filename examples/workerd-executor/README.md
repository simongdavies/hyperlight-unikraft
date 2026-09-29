# Workerd executor foundation (v0.14 prototype ABI)

This harness packages a **trusted external workerd-fork executor** or a tiny
native mock, not stock workerd. Static executors use the dedicated v0.14
direct-initrd elfloader; dynamic executors retain the general CPIO/VFS kernel.
Both use the existing 64 KiB transport and Hyperlight 0.17 without feature changes.
It grants neither hostfs nor hostsock. The committed general-purpose kernel
contains those drivers, but the wrapper supplies no mounts/network policy
and registers no `fs_*` or `net_*` host functions.

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
  --bind 0.0.0.0:8787
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

The bridge is deliberately sequential and forwards each bounded HTTP request
to a fresh VM restored from the initialized one-Worker snapshot. It grants no
guest filesystem or networking capability. Use `--request-timeout-ms` to set
the watchdog deadline; a timed-out request receives HTTP 504 and the next
request starts from a fresh VM.

## Workerd-fork executor ABI

Use `drivers/hl_driver.h` and `drivers/hl_fc.h`, as the fixture does. Open
`/dev/hlcall`, size the call buffer with `HLCALL_IOC_MAXLEN`, and read complete
size-prefixed Hyperlight FunctionCall FlatBuffers. Returning to the next read
completes the call; write an `int32_t` nonzero status to fail it.

* `init(bundle_json)` loads exactly one Worker version from canonical protocol
  1 JSON. Startup before this call is trusted and must not execute tenant code.
  The wrapper snapshots only after successful init; the initialized driver
  remains alive. The source bundle is never sent again.
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

The init bundle has exact top-level field order
`protocol_version`, `worker_version`, `compatibility_date`,
`compatibility_flags`, `main_module`, `modules`. Module field order is
`name`, `type`, `source`:

```json
{"protocol_version":1,"worker_version":"hello-v1","compatibility_date":"2025-01-01","compatibility_flags":[],"main_module":"worker.js","modules":[{"name":"worker.js","type":"esModule","source":"export default { fetch() { return new Response('ok') } }"}]}
```

The host parses trusted JSON, rejects unknown/duplicate/invalid values, sorts
flags, places the main ES module first, sorts remaining modules by name, and
serializes compact canonical JSON. Types are exactly `esModule`, `text`, and
`json`. There are at most 32 flags and 32 modules; each module is at most
32 KiB and aggregate decoded source is at most 48 KiB. Module names are safe
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
interfaces only: no brokers or outbound access are installed.

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
request/output state cleared before another request can run. A killed VM is
never reused, and an instance cannot be assigned another Worker version.
Artifact loading and VM construction are not covered by the execution timeout.
Repeated request IDs are allowed across completed requests; correlation and
duplicate-response checks apply to the one active request, not a replay store.

`execute_profiled()` reports each host-side phase separately:

* `snapshot_restore_ms`: clone the immutable snapshot handle and construct a
  fresh Hyperlight VM from it.
* `request_setup_ms`: validate/serialize the request, clear request state,
  register `HostPrint`, and activate the request ID.
* `guest_execution_ms`: resume the restored guest and execute `fetch` under
  the watchdog.
* `vm_teardown_ms`: drop the per-request VM after the watchdog has joined.
* `response_finish_ms`: validate/take the completed response, or clear state
  after failure.

The demo writes this profile as compact JSON to stderr for every request, and
the memory probe includes profiles for hello, busy-loop timeout, and recovery.
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

Run the 512 MiB real-V8 acceptance probe:

```sh
HYPERLIGHT_MAX_SURROGATES=2 HYPERLIGHT_INITIAL_SURROGATES=0 \
RUST_LOG=hyperlight_unikraft=debug \
cargo run --release --locked --example workerd-memory-probe -- 512 \
  --bundle examples/workerd-bundles/acceptance.json \
  2>build-elfloader/workerd-memory-512-kvm.stderr \
  | tee build-elfloader/workerd-memory-512-kvm.json
```

Probe the configured-scratch floor sequentially while retaining the official
ladder sizes:

```sh
for mib in 256 320 384 448 512 640 768 1024 1536 2048; do
  HYPERLIGHT_MAX_SURROGATES=2 HYPERLIGHT_INITIAL_SURROGATES=0 \
  cargo run --release --locked --example workerd-memory-probe -- "$mib" \
    --bundle examples/workerd-bundles/acceptance.json
done | tee build-elfloader/workerd-memory-ladder-kvm.jsonl
```

Run the reproducible real-V8 bundle probes through initialization, snapshot,
fresh restore, and fetch:

```sh
cargo run --release --locked --example workerd-bundle-probe -- \
  examples/workerd-bundles/helloworld_esm.json https://example.test/ 512
cargo run --release --locked --example workerd-bundle-probe -- \
  examples/workerd-bundles/web-streams.json https://example.test/sync 512
cargo run --release --locked --example workerd-bundle-probe -- \
  examples/workerd-bundles/api-smoke.json \
  https://example.test/wintertc-smoke 512 POST x-smoke:yes
# Equivalent repeatable compatibility smoke:
just workerd-api-probe 512
```

`api-smoke.json` is a representative probe derived from Workerd's
machine-readable ECMA-429/WPT support matrix, not an authoritative conformance
runner. `api-smoke-matrix.json` records the selected pure Web APIs,
capability-backed APIs intentionally unavailable in this sandbox, untested
surfaces, and the expected SHA-256 digest vector.

For a curlable listener, run the demo in one terminal and issue requests from
another:

```sh
HYPERLIGHT_MAX_SURROGATES=2 HYPERLIGHT_INITIAL_SURROGATES=0 \
cargo run --release --locked --example workerd-demo -- \
  --bundle examples/workerd-bundles/acceptance.json \
  --bind 0.0.0.0:8787 --scratch-mb 512 --request-timeout-ms 500
```

```sh
curl -i -X POST http://127.0.0.1:8787/hello -d hello
curl -i -X POST http://127.0.0.1:8787/busy -d busy
curl -i -X POST http://127.0.0.1:8787/after -d after
```

`cargo test --test workerd_sandbox` runs the real-hypervisor cases and **fails**
if artifacts/hypervisor access are missing; it does not silently self-skip.
Use `cargo test --lib workerd` for the hardware-independent tests.
Cross-Clippy is cfg/type validation only. This prototype claims neither W^X,
stock-workerd support, production hardening, nor platform runtime qualification
without an actual run on that platform.
