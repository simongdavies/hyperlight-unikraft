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

Optional API cases may pass or be unsupported. Timer cases and the slow
producer case remain `pending` and excluded from the threshold until the
separate timer branch supplies the executor implementation. Harness acceptance
means only that this declared matrix met its threshold.

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
* separate `accepted` (harness threshold) and `compliance_ready` gates.

The executor revision is mandatory because a binary digest alone cannot identify
its source. Keep the generated evidence with the corresponding upstream WPT
report from the same executor checkout.

## Final merged-artifact command

After the timer branch and Workerd executor artifact are merged and packaged,
run this single command from the repository root on the real Hyperlight host:

```text
cargo run --release --locked --example wintertc-vm-evidence -- --executor-revision <exact-workerd-executor-git-sha> --output build-elfloader/wintertc-evidence.json
```

The command fails when required artifacts are absent or the declared acceptance
threshold is not met, while still writing the matrix when execution reaches the
reporting phase. It does not invoke QEMU.
