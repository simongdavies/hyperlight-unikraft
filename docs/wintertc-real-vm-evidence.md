# WinterTC real-VM evidence harness

This harness records scoped WinterTC evidence from the packaged static Workerd
executor running each case in a fresh Hyperlight VM. It does **not** turn smoke
tests into a claim of full WinterTC compliance.

The checked-in manifest separates API presence from behavior and classifies
results as `pass`, `fail`, `unsupported`, `policy_denied`, `dns_denied`, or
`pending`. An API global being present never satisfies a behavior case.
Executor policy denial is expected only for cases whose manifest expectation
names that denial; missing or unimplemented APIs are `unsupported`, not policy
successes.

## Scope and acceptance

`examples/workerd-bundles/wintertc-evidence-manifest.json` is the
machine-readable scope. Harness acceptance requires:

* every `required` case to pass;
* each `policy` case to produce its named denial class;
* no required case to be reported as unsupported.

Optional API cases may pass or be unsupported. Timer cases, the slow producer,
and the offline-restore pool rows are required. The executor must provide its
KJ timer adapter before the matrix can pass. Harness acceptance means only that
this declared matrix met its threshold.

`compliance_ready` remains false while any authoritative core row is pending.
In particular, the current inventory blocks an ECMA-429 claim on the
WorkerGlobalScope `onerror`, `onunhandledrejection`, and `onrejectionhandled`
handler properties. It also blocks on the full
`webmessaging/message-channels/` MessagePort subset, the
`streams/readable-byte-streams/` general, tee, respond-after-enqueue, and
pending-read `releaseLock` subset, and executor-specific hostile WebAssembly
memory/CPU qualification. These rows must be replaced by results from the exact
pinned Workerd/WPT revision; local smoke cannot clear them.

The authoritative conformance inputs are ECMA-429 and the WPT subsets selected
by the exact Workerd revision, including Workerd's `url`, `urlpattern`,
`encoding`, `fetch/api`, `streams`, `compression`, `WebCryptoAPI`, and
`performance-timeline` targets. This harness provides deployment-specific
real-VM evidence around those sources; it does not replace their full results.

The matrix covers the existing capability-free APIs, WebAssembly,
MessageChannel, File and BYOB where runnable, fetch v1-compatible GET/POST and
ordered duplicate headers, and fetch v2 streaming beyond v1 caps. Streaming
cases include known and unknown lengths, a slow consumer, bounded
backpressure, early response, cancellation, redirects, policy and DNS denial,
timeout, and overload. Slow-producer evidence is timer-dependent and remains
explicitly pending.

Large and unknown-length download probes consume and count response chunks
instead of aggregating them with `arrayBuffer()`. This validates the streaming
contract without turning the row into a JavaScript heap-size test. The overload
row accepts the executor's exact 17th-operation rejection text,
`too many concurrent outbound fetch v2 operations`.

MessageChannel, File, the local BYOB probe, and the basic WebAssembly probe are
verification-only. MessagePort transfer lists and port serialization,
`DataCloneError`, `messageerror`, queue/start/close/GC semantics, the known BYOB
failure areas, and hostile Wasm resource limits remain authoritative pending
rows. The executor must allow only its Wasm compilation callback while
continuing to deny `eval` and `new Function`.

## Evidence contents

The output JSON records:

* the host commit and dirty-worktree state;
* the Unikraft guest gitlink revision;
* the explicitly supplied executor source revision;
* SHA-256 and byte length for kernel, rootfs, executor, and Worker bundle;
* Worker envelope and outbound fetch protocol versions;
* one result row per manifest case with detail and observations.
* cold-boot, repeated restore, pool depletion/refill, throughput/latency,
  timer/network-after-restore, tenant isolation, cancellation recovery, and
  clean-shutdown resource-accounting evidence;
* a separate machine-readable performance report;
* separate `accepted` (harness threshold) and `compliance_ready` gates.

The executor revision is mandatory because a binary digest alone cannot identify
its source. Keep the generated evidence with the corresponding upstream WPT
report from the same executor checkout.

The two Worker bundles have different roles and must not replace one another:

* `examples/workerd-bundles/wintertc-evidence.json` is the authoritative
  Hyperlight matrix contract with one `/case/<id>` route per manifest row.
* `examples/workerd-bundles/workerd-wintertc-demo.json` is the Workerd-owned
  runnable behavior demo with `/evidence/...` routes. Its SHA must match the
  package handoff, and its results supplement rather than replace matrix rows.

## End-to-end Linux, WSL, and Azure runbook

The supported runtime path is native Linux with KVM. WSL 2 is supported when
the selected distribution exposes a readable and writable `/dev/kvm`. Azure is
supported on an x86-64 VM size with nested virtualization, such as
`Standard_D32s_v5`, using Standard security. QEMU is not part of this flow.

Install the host prerequisites:

```sh
export HOME="${HOME:-/root}"
sudo apt-get update
sudo apt-get install -y \
  build-essential cpio curl file git jq pkg-config python3 binutils rsync
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. "$HOME/.cargo/env"
cargo install just
```

`just build-test-bins` also requires Go. The final qualification used a pinned
user-local Go installation rather than changing WSL system packages:

```sh
mkdir -p "$HOME/.cache" "$HOME/.local"
curl -fsSL https://go.dev/dl/go1.24.4.linux-amd64.tar.gz \
  -o "$HOME/.cache/go1.24.4.linux-amd64.tar.gz"
echo \
  '77e5da33bb72aeaef1ba4418b6fe511bc4d041873cbf82e5aa6318740df98717  /home/simon/.cache/go1.24.4.linux-amd64.tar.gz' \
  | sha256sum --check
tar -xzf "$HOME/.cache/go1.24.4.linux-amd64.tar.gz" -C "$HOME/.local"
mv "$HOME/.local/go" "$HOME/.local/go1.24.4"
export PATH="$HOME/.local/go1.24.4/bin:$HOME/.cargo/bin:$PATH"
go version
```

The official archive is 78,559,214 bytes and must report
`go version go1.24.4 linux/amd64`. Adapt the absolute checksum path when the
WSL user is not `simon`; do not omit checksum verification.

On WSL, keep the repository, Cargo target directory, executor, and generated
artifacts on the distribution's ext4 filesystem, not under `/mnt/c`:

```sh
test "$(stat -f -c %T "$HOME")" = ext2/ext3
test -c /dev/kvm && test -r /dev/kvm && test -w /dev/kvm
grep -E -m1 '(^flags|^Features).*(vmx|svm)' /proc/cpuinfo

mkdir -p "$HOME/src" "$HOME/.cache/hluk-compliance-target"
git clone --recurse-submodules \
  --branch simongdavies-hyperlight-compliance-integration \
  https://github.com/simongdavies/hyperlight-unikraft.git \
  "$HOME/src/hyperlight-unikraft"
cd "$HOME/src/hyperlight-unikraft"
export CARGO_TARGET_DIR="$HOME/.cache/hluk-compliance-target"
```

Do not copy an entire Windows checkout over an ext4 clone: that creates
line-ending-only modifications across the repository. Clone on ext4 and apply
only the intended Git patch, or use the pushed branch directly.

When the integration remains uncommitted, export both tracked changes and
untracked additions. Plain `git diff` silently omits new files:

```sh
patch=/absolute/path/to/authoritative-integration.patch
git diff --binary --full-index >"$patch"
while IFS= read -r path; do
  git diff --binary --no-index -- /dev/null "$path" >>"$patch" ||
    test "$?" -eq 1
done < <(git ls-files --others --exclude-standard)
git apply --check "$patch"
```

### Optional disposable Azure host

Run these commands from a workstation with Azure CLI authentication and an SSH
public key. The dedicated resource group makes cleanup deterministic:

```sh
export AZURE_LOCATION=uksouth
export AZURE_RESOURCE_GROUP=hyperlight-workerd-validation-rg
export AZURE_VM=hyperlight-workerd-validation
export AZURE_VM_SIZE=Standard_D32s_v5
export AZURE_USER=simon
export AZURE_SSH_PUBLIC_KEY="$HOME/.ssh/id_rsa.pub"
export AZURE_EMERGENCY_CLEANUP_UTC=2026-10-01T06:00:00Z

az group create \
  --name "$AZURE_RESOURCE_GROUP" \
  --location "$AZURE_LOCATION" \
  --tags \
    cleanupLeaseUtc="$AZURE_EMERGENCY_CLEANUP_UTC" \
    purpose=wintertc-authoritative-kvm
az vm create \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --name "$AZURE_VM" \
  --location "$AZURE_LOCATION" \
  --size "$AZURE_VM_SIZE" \
  --image Canonical:ubuntu-24_04-lts:server-gen1:latest \
  --admin-username "$AZURE_USER" \
  --ssh-key-values "$AZURE_SSH_PUBLIC_KEY" \
  --public-ip-sku Standard \
  --storage-sku Premium_LRS \
  --os-disk-size-gb 256 \
  --tags \
    cleanupLeaseUtc="$AZURE_EMERGENCY_CLEANUP_UTC" \
    purpose=wintertc-authoritative-kvm
export AZURE_IP="$(
  az vm show -d \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --name "$AZURE_VM" \
    --query publicIps -o tsv
)"
ssh "$AZURE_USER@$AZURE_IP" \
  'test "$(nproc)" -ge 32 &&
   test "$(awk "/MemTotal/ { print int(\$2 / 1024 / 1024) }" /proc/meminfo)" -ge 120 &&
   grep -E -m1 "(^flags|^Features).*(vmx|svm)" /proc/cpuinfo &&
   test -c /dev/kvm && test -r /dev/kvm && test -w /dev/kvm'
```

The authoritative performance comparison requires 32 vCPUs and 128 GiB RAM.
`Standard_D16s_v5` and smaller SKUs are invalid even if they expose nested KVM.
Use `Standard_D32s_v5` or an equal/larger nested-virtualization-capable SKU,
and fail before installation or build work when CPU, RAM, Premium disk, free
space, VMX/SVM, or `/dev/kvm` does not satisfy the preflight.
Delete the resource group immediately after every evidence artifact has been
downloaded and hashed. `AZURE_EMERGENCY_CLEANUP_UTC` is only a hard backstop
for an interrupted overnight run; it is not permission to retain the VM until
that time or to truncate validation before it.

Some subscriptions reject an explicit `--security-type Standard` until
`Microsoft.Compute/UseStandardSecurityType` is registered. In that case, use a
Gen1 Ubuntu image and omit the security flag; do not silently accept Trusted
Launch when nested virtualization is required. If SSH is unavailable, Azure
Run Command is a supported control path and runs as root:

```sh
az vm run-command invoke \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --name "$AZURE_VM" \
  --command-id RunShellScript \
  --scripts \
    'export HOME="${HOME:-/root}";
     grep -E -m1 "(^flags|^Features).*(vmx|svm)" /proc/cpuinfo;
     test -c /dev/kvm && test -r /dev/kvm && test -w /dev/kvm'
```

Set `HOME` explicitly in every Run Command wrapper before sourcing Cargo;
otherwise `/root/.cargo/env` fails under `set -u`. Pass longer scripts as
base64 and decode them on the VM so the local shell cannot expand Bash
variables. The wrapper must use an `EXIT` trap to archive and upload logs,
evidence, performance output, and its exit status even when setup or validation
fails. Archive only those outputs plus identity manifests; exclude the cloned
repository, `.git`, submodules, and Cargo/build caches so a small evidence
bundle does not become a multi-hundred-megabyte source/build archive.

If `/dev/kvm` is group-restricted during an SSH-driven run, add the user to
`kvm`, reconnect, and rerun the preflight. Copy the executor package to the VM
with `scp` or temporary private blob storage, then follow the same package and
evidence commands below. When all artifacts have been copied off the VM, remove
every validation resource and wait for deletion to complete:

```sh
az group delete \
  --name "$AZURE_RESOURCE_GROUP" \
  --yes \
  --no-wait
while az group exists --name "$AZURE_RESOURCE_GROUP" | grep -qx true; do
  sleep 15
done
test "$(az group exists --name "$AZURE_RESOURCE_GROUP")" = false
```

### Verify and package the optimized executor

The optimized executor handoff is one executable x86-64 Linux PIE at an
absolute path, a machine-readable handoff, and its exact build-source identity:
the source HEAD plus a frozen binary patch digest when the build worktree was
dirty. A separate clean checkout may be comparison metadata but is not build
provenance.
For the current optimized package, the expected executor SHA-256 is
`887e9533a40c71263ae51bc80c9aeb5e0138db5342414876a70ebbfba9c699bd`,
the GNU Build ID is `6038fe01f84d1abf709d8d96c040126d2eb08468`,
and the PT_LOAD page span is 92,340,224 bytes. The finalized handoff SHA-256
is `0842e50da19bb5d47d9db1270cd6cfff1b850e76c25aa0740f25bafbe65b4e07`,
the archive SHA-256 is
`e5f5c8add5e8d23d4882dc4c17a77746c71d036540500a2551199bbe6b5d97d3`,
and the Workerd demo bundle SHA-256 is
`31a591ba2740f087e81344b5d2e6fd4aa6180c31ea6bf074bb99304a0d62b1aa`.
The build source is HEAD
`039b00382b21d0de328359f68e1b26e266c48826` plus patch SHA-256
`e8e7f33fdebad007d081bbdf3d3382a2701ddb0cfa84e17d4f41956313b739b0`,
which produces source identity
`85ceb5424d6b763e11e0ad9edc0c97af6a786f627f64e68f1670166c8ed8c48f`.

```sh
export WORKERD_EXECUTOR=/absolute/path/to/workerd-sandbox-executor
export WORKERD_HANDOFF=/absolute/path/to/hyperlight-handoff.json
test -x "$WORKERD_EXECUTOR"

test "$(sha256sum "$WORKERD_EXECUTOR" | cut -d' ' -f1)" = \
  "$(jq -r '.executor.sha256' "$WORKERD_HANDOFF")"
test "$(readelf -n "$WORKERD_EXECUTOR" |
  sed -n 's/.*Build ID: //p')" = \
  "$(jq -r '.executor.build_id' "$WORKERD_HANDOFF")"
export WORKERD_EXECUTOR_REVISION="$(
  jq -r '"workerd-head:" + .build_source.head +
    ",source-identity:" + .build_source.head_and_patch_identity_sha256' \
    "$WORKERD_HANDOFF"
)"

test "$(jq -r '.schema_version' "$WORKERD_HANDOFF")" = 2
WORKERD_BUILD_PATCH="$(
  dirname "$WORKERD_HANDOFF"
)/$(basename "$(
  jq -r '.build_source.tracked_binary_patch.path' "$WORKERD_HANDOFF"
)")"
test "$(sha256sum "$WORKERD_BUILD_PATCH" | cut -d' ' -f1)" = \
  "$(jq -r '.build_source.tracked_binary_patch.sha256' "$WORKERD_HANDOFF")"
test "$(
  printf '%s\n%s\n' \
    "$(jq -r '.build_source.head' "$WORKERD_HANDOFF")" \
    "$(jq -r '.build_source.tracked_binary_patch.sha256' "$WORKERD_HANDOFF")" |
    sha256sum | cut -d' ' -f1
)" = "$(jq -r '.build_source.head_and_patch_identity_sha256' \
  "$WORKERD_HANDOFF")"

bash examples/workerd-executor/build-rootfs.sh "$WORKERD_EXECUTOR"
sha256sum \
  kernel/workerd_hyperlight-x86_64 \
  build-elfloader/workerd-executor/executor \
  build-elfloader/workerd-executor/rootfs.img \
  examples/workerd-bundles/wintertc-evidence.json \
  examples/workerd-bundles/wintertc-evidence-manifest.json \
  | tee build-elfloader/wintertc-artifact-sha256.txt
cat build-elfloader/workerd-executor/image-mode
cat build-elfloader/workerd-executor/dependency-closure.sha256

python3 - "$WORKERD_EXECUTOR" <<'PY'
import re
import subprocess
import sys

text = subprocess.check_output(["readelf", "-W", "-l", sys.argv[1]], text=True)
segments = []
for line in text.splitlines():
    fields = line.split()
    if fields and fields[0] == "LOAD":
        segments.append((int(fields[2], 16), int(fields[5], 16)))
if not segments:
    raise SystemExit("executor has no PT_LOAD segments")
page_size = 4096
start = min(address for address, _ in segments) // page_size * page_size
end = (
    max(address + size for address, size in segments) + page_size - 1
) // page_size * page_size
span = end - start
print(f"pt_load_span_bytes={span}")
print(f"pt_load_span_mib={span / 1024 / 1024:.3f}")
raise SystemExit(0 if span <= 128 * 1024 * 1024 else 2)
PY
```

The packager must report `direct-elf` for the current static PIE package.

### Run the optimized executor baseline

The optimized executor's authoritative complete-matrix baseline is 344 MiB.
Real KVM diagnostics already established that it boots and passes core
timeout/recovery rows there. Use 768 MiB only as a diagnostic fallback when a
344 MiB failure is specifically attributable to memory, not for functional
bundle or adapter failures:

```sh
export HYPERLIGHT_MAX_SURROGATES=4
export HYPERLIGHT_INITIAL_SURROGATES=0

cargo run --release --locked --example workerd-memory-probe -- \
  344 \
  --bundle examples/workerd-bundles/acceptance.json \
  >build-elfloader/workerd-memory-344.json \
  2>build-elfloader/workerd-memory-344.stderr
jq -e '.result == "passed"' build-elfloader/workerd-memory-344.json
```

### Run the complete evidence and performance matrix

Before the complete run, qualify the four corrected typed host-error paths
against the exact packaged executor:

```sh
cargo run --release --locked --example wintertc-vm-evidence -- \
  --executor-revision "$WORKERD_EXECUTOR_REVISION" \
  --artifact-dir build-elfloader/workerd-executor \
  --scratch-mib 344 \
  --focus-typed-errors \
  --output build-elfloader/wintertc-typed-errors.json \
  --performance-output build-elfloader/wintertc-typed-errors-performance.json
jq -e '
  .accepted == true
  and (.matrix | length == 4)
  and ([.matrix[]
    | select(
        .accepted != true
        or (.detail | contains("internal error; reference = "))
      )] | length == 0)
' build-elfloader/wintertc-typed-errors.json
```

Do not start the complete matrix when this focused qualification fails.

Use the same memory setting for the complete run. This exercises capability-free
APIs, streaming fetch, timer behavior, MessageChannel, File, BYOB streams,
core Wasm or an exact Component Model/core-Wasm blocker, repeated restore,
pool depletion/refill, throughput and latency, tenant-state isolation,
cancellation recovery, and clean shutdown:

```sh
bash tools/run-wintertc-evidence.sh \
  /absolute/path/to/extracted/workerd-package \
  344 \
  linux-kvm-344
```

The script performs the handoff/hash/BuildID/PT_LOAD checks, packaging, memory
probe, complete matrix, typed host-error assertions, and artifact generation.
It fails before expensive work when the current filesystem is read-only or has
less than 20 GiB free (`WINTERTC_MINIMUM_FREE_KIB` may raise the threshold).
The equivalent expanded evidence command is:

```sh

cargo run --release --locked --example wintertc-vm-evidence -- \
  --executor-revision "$WORKERD_EXECUTOR_REVISION" \
  --artifact-dir build-elfloader/workerd-executor \
  --scratch-mib 344 \
  --output build-elfloader/wintertc-evidence.json \
  --performance-output build-elfloader/wintertc-performance.json \
  >build-elfloader/wintertc-evidence.stdout.json \
  2>build-elfloader/wintertc-evidence.stderr

jq -e '.accepted == true' build-elfloader/wintertc-evidence.json
jq -e '
  [.matrix[]
    | select(.category == "offline-restore" or .category == "timers")
    | select(.accepted != true)]
  | length == 0
' build-elfloader/wintertc-evidence.json
```

Run the Workerd-owned behavior demo separately against the same packaged
executor and memory setting:

```sh
cargo run --release --locked --example workerd-demo -- \
  --executor build-elfloader/workerd-executor/executor \
  --bundle examples/workerd-bundles/workerd-wintertc-demo.json \
  --scratch-mb 344 \
  --bind 127.0.0.1:19091

curl --fail-with-body http://127.0.0.1:19091/evidence/global-handlers
curl --fail-with-body http://127.0.0.1:19091/evidence/core-wasm
curl --fail-with-body http://127.0.0.1:19091/evidence/byob
curl --fail-with-body http://127.0.0.1:19091/evidence/byte-stream-tee
curl --fail-with-body http://127.0.0.1:19091/evidence/timers
for stage in \
  construct listener-registration start post-message queued-delivery close \
  transfer-reentanglement clone-failure
do
  curl --fail-with-body \
    "http://127.0.0.1:19091/evidence/messageport?stage=$stage"
done
```

Record each endpoint and stage independently. A basic JavaScript BYOB or tee
pass does not clear a failing combined readable-byte-stream C++/WPT target, and
a MessagePort stage pass does not clear a later transfer/re-entanglement
failure.

An evidence command may exit nonzero after writing its JSON. Inspect every
failed row before changing memory. Workerd exceptions shaped as
`internal error; reference = ...`, guest call status `-1`, or an embedder
Wasm denial are executor-adapter failures, not memory-floor evidence.

### Run the apples-to-apples offline-restore pool benchmark

The authoritative pooling comparison uses the same 9,441-byte `/sync` response
and exact first command as the prior 32-core result:

```text
hey -n 320 -c 32 http://127.0.0.1:8787/sync
```

The frozen prior baseline is 320 HTTP 200 responses with zero errors,
291.3588 requests/s, p50 107.3 ms, p95 121.3 ms, and p99 135.4 ms. The
optimized pool passes only when the exact first run has zero errors, strictly
beats 291.3588 requests/s, and strictly improves all three percentiles. Do not
average a failure together with later runs. The stretch target is at least
500 requests/s with p95 at most 100 ms and p99 at most 120 ms.

Run with a clearly recorded pool size of 32 fixed request workers on a
32-vCPU host. The implementation restores a request VM on acquisition rather
than keeping 32 mutable tenant VMs resident. Then preserve the required
60-second c32/c64/c128 sustained runs:

```sh
bash tools/run-wintertc-pool-benchmark.sh \
  build-elfloader/workerd-executor \
  build-elfloader/wintertc-pool-performance \
  344 \
  32
jq -e '.accepted == true and .refill.passed == true' \
  build-elfloader/wintertc-pool-performance/wintertc-pool-performance.json
```

For the prewarmed A/B, keep the positional active cap unchanged and select the
ready-owner pool through environment variables:

```sh
WINTERTC_POOL_RESTORE_MODE=prewarmed \
WINTERTC_POOL_PREWARMED_SANDBOXES=48 \
WINTERTC_POOL_MAX_CONCURRENT_RESTORES=8 \
WINTERTC_POOL_PROFILE_LOG_EVERY=64 \
bash tools/run-wintertc-pool-benchmark.sh \
  build-elfloader/workerd-executor \
  build-elfloader/wintertc-pool-performance-o48-r8 \
  343 \
  32
```

The machine-readable report records the exact command, payload length and
hash, resolved mode/owner/restore configuration, peak admitted/active/queued
requests, ready-owner and recycle pressure, teardown/restore/completion
activity and timings, throughput, p50/p95/p99, errors, CPU, RSS, refill time,
and post-load recovery.
It fails immediately when the exact baseline is not beaten, while retaining
the JSON and server log for diagnosis. The sustained runs must also have zero
errors. Preserve the result, server log, and
`wintertc-pool-artifact-sha256.txt` with the other authoritative artifacts.

Only after the complete 344 MiB matrix passes may the exact same required
subset be rerun at lower candidates. Historical KVM diagnostics found that
340 MiB fails at boot, so the next candidate must come from the allocator and
existing memory-probe evidence rather than an arbitrary decrement. Preserve
one evidence and performance file per tested candidate; never record an
untested value as supported:

```sh
for mib in 320 288 256 224 192 160 128; do
  cargo run --release --locked --example wintertc-vm-evidence -- \
    --executor-revision "$WORKERD_EXECUTOR_REVISION" \
    --artifact-dir build-elfloader/workerd-executor \
    --scratch-mib "$mib" \
    --output "build-elfloader/wintertc-evidence-${mib}.json" \
    --performance-output "build-elfloader/wintertc-performance-${mib}.json" \
    >"build-elfloader/wintertc-evidence-${mib}.stdout.json" \
    2>"build-elfloader/wintertc-evidence-${mib}.stderr" || break
  jq -e '.accepted == true' \
    "build-elfloader/wintertc-evidence-${mib}.json" || break
done
```

Stop at the first failing candidate. Do not skip downward and do not claim the
last attempted value; claim only the lowest complete passing matrix.

The authoritative artifacts are:

* `build-elfloader/workerd-executor/executor`
* `build-elfloader/workerd-executor/rootfs.img`
* `build-elfloader/workerd-executor/dependency-closure.manifest`
* `build-elfloader/workerd-executor/dependency-closure.sha256`
* `build-elfloader/wintertc-artifact-sha256.txt`
* `build-elfloader/wintertc-evidence.json`
* `build-elfloader/wintertc-performance.json`
* `build-elfloader/wintertc-evidence.stdout.json`
* `build-elfloader/wintertc-evidence.stderr`

Copy these files, the executor handoff, executor archive, source revisions,
host details, and command logs off disposable hosts before cleanup. The
evidence command fails when required artifacts are absent or the declared
threshold is not met, while still writing the matrix when execution reaches
the reporting phase. It does not invoke QEMU.

Generate and verify the final checksum manifest before creating the archive:

```sh
bash tools/archive-wintertc-evidence.sh \
  build-elfloader/wintertc-export \
  build-elfloader/wintertc-evidence.tar.gz
sha256sum --check build-elfloader/wintertc-evidence.tar.gz.sha256
```

The helper writes `SHA256SUMS` from a `find` expression that explicitly
excludes `SHA256SUMS`, verifies every listed file, and requires the archive to
be outside the export directory. Do not redirect a `find` pipeline directly
into a manifest that the same `find` can discover: shell redirection creates
the output file before `find` runs, producing a self-referential checksum row
that cannot verify.

## Authoritative Azure D32 result

The accepted real-Hyperlight/KVM run used a Standard_D32s_v5 host with 32
vCPUs, 125.789 GiB RAM, KVM API version 12, and the corrected executor:

* executor SHA-256
  `887e9533a40c71263ae51bc80c9aeb5e0138db5342414876a70ebbfba9c699bd`;
* BuildID `6038fe01f84d1abf709d8d96c040126d2eb08468`;
* aligned PT_LOAD span 92,340,224 bytes;
* schema-v2 source identity
  `85ceb5424d6b763e11e0ad9edc0c97af6a786f627f64e68f1670166c8ed8c48f`;
* scratch memory 344 MiB.

The complete 41-row matrix had no failed rows and `.accepted == true`.
`compliance_ready` remains false only for the separately identified
authoritative Workerd/WPT inputs described above. The focused qualification
also accepted all four corrected typed-error paths: `policy_denied`,
`dns_failed`, visible timeout, and visible overload. None contained an opaque
`internal error; reference = ...` value. Cold boot was 650.084426 ms and the
repeated snapshot-restore p95 was 1.802809 ms. The complete matrix passed
offline restore, timer/network behavior after restore, tenant isolation,
depletion/refill, cancellation/failure recovery, and clean shutdown. No
lower-memory candidate was qualified, so 344 MiB remains the supported result.

The exact 9,441-byte c32 baseline run passed the minimum comparison with zero
errors, 452.7566 requests/s, p50 68.2 ms, p95 86.1 ms, and p99 88.3 ms. It did
not meet the 500 requests/s stretch target. Sustained 60-second runs remained
error-free but exposed saturation above the 32-worker pool:

| Concurrency | Requests | Requests/s | p50 | p95 | p99 | Peak queued |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 24,314 | 403.0321 | 73.5 ms | 129.4 ms | 153.3 ms | 1 |
| 64 | 16,584 | 274.4476 | 228.5 ms | 315.8 ms | 343.8 ms | 32 |
| 128 | 14,738 | 242.8498 | 522.1 ms | 620.9 ms | 648.3 ms | 96 |

The downloaded evidence archive is 75,241,827 bytes with SHA-256
`ddadf82d7cb221ad783831e05d6b4f7c34c50afd14b3382dad1cb5606da4eccc`.
All 42 non-self payload hashes were independently verified. The original
archive's generated manifest accidentally included its own in-progress
`SHA256SUMS`; that invalid self-row was excluded during independent
verification and is the failure prevented by
`tools/archive-wintertc-evidence.sh`. After archive download and hash
verification, the disposable Azure resource group was deleted; the resource
group existence check returned false and the subscription-wide matching
resource count was zero.
