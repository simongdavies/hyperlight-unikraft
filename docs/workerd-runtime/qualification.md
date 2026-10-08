# Streaming, qualification and bounded load

[Canonical demo runbook](../azure-workerd-hyperlight-runbook.md)

## Bounded demo and load flow

Use the existing guided harness, not a new performance campaign:

```bash
tools/hyperlight-demo --list
tools/hyperlight-demo --describe benchmark-resident
tools/hyperlight-demo --preview
```

Select the exact listed load demo and inspect its `--describe` narration before
running it. Keep explicit app/VM/concurrency/request budgets, preserve the
artifact identities, and retain the raw driver logs as evidence rather than
printing an unbounded wall of output. See the canonical
[resident-host load recipes](../azure-workerd-hyperlight-runbook.md#resident-host-throughput-routing-and-drain-under-real-load)
for the existing `hey`/Vegeta selectors and interpretation.

A standalone multi-app benchmark is **not** the cross-tenant production
topology: the platform baseline is one tenant/app/revision home and process.
Request rate, live VM count, virtual scratch reservation, charged cgroup
memory, checkpoint scratch and shared template overhead are distinct metrics.
A small-counter result is not framework density; native guest self-tests are
not a Kubernetes/cross-node proof.

Existing scripts default to the presenter-friendly phase output. Use their
explicit raw/verbose log options when investigating; never print credential
files, request authentication headers or checkpoint keys.


Per-app `streaming: true` opts into the negotiated bounded HTTP/SSE/inbound
WebSocket host adapter for either execution mode. It does not silently alter
legacy buffered requests. Frame queues and frame/message bytes are bounded;
nonblocking pending/backpressure polls preserve owner-thread full-duplex
progress. The host synthesizes the WebSocket handshake from the validated
client key, checks subprotocol selection, masking, UTF-8 and close payloads.
Disconnect/timeout cancels the handler, and VM/capacity accounting lasts
through transport **and** tracked-work completion, not only response end.

Native fixture tests exercise real KVM and host processes but a C mock guest,
not V8. The changed-instance encrypted-file and idle-zero tests prove mutated
mock state survives reopening/resume, not JavaScript semantics or cross-node
recovery. Host transport tests prove progressive SSE and a real WebSocket
handshake/server greeting before client input, but use synthetic guest frames.
Real Workerd completion, init-v4, streams and changed-heap checkpoint
qualification are required before release claims. Run the focused tests and
then the final Hyperlight gates:

```bash
just guests
cargo test --locked --lib workerd::
cargo test --locked --test workerd_host_contract --test workerd_invocations \
  --test workerd_checkpoint --test workerd_outbound -- --test-threads=1
just fmt-apply
just build
just clippy
just clippyw
just test
```

## Real Workerd evidence

The corrected optimized, stripped executor candidate has SHA-256
`fd1a6a578b67d12a671e6f341552ded8a847f3a2f9f328db6be69b5c9f5ad31e`.
The matching direct-ELF rootfs has the same hash; its dependency-closure value is
`280a554ec88d610808d910362c848d150aceb81871cd71304edb7f1767289974`.
Do not substitute older development executors: they have confirmed completion
and GET/HEAD defects. The preceding `e2f034...` candidate additionally repeats
guest-generated UUIDs across initialized snapshot clones and is not a shipping
artifact.
The intermediate `2008d4...` fixes entropy but incorrectly retains built-in
request-deadline timers after otherwise completed requests; it is also not a
shipping artifact.

Eleven real Workerd/KVM cases cover the following required outcomes with
512 MiB guest scratch. The corrected candidate passed the complete eleven-case
batch, including UUID/random-value/AES-GCM/ECDSA clone/resume, bodyless HTTP
reuse and built-in deadline cleanup. All mandatory native Rust gates also
passed without excluding legacy integration suites. Repeat against the exact
artifacts being deployed; signing and publication remain separate gates:

```bash
export WORKERD_REAL_EXECUTOR="$PWD/build-elfloader/workerd-executor-deadlines-qualified/executor"
export WORKERD_REAL_ROOTFS="$PWD/build-elfloader/workerd-executor-deadlines-qualified/rootfs.img"
export WORKERD_REAL_SCRATCH_MB=512
cargo test --locked --test workerd_real -- --ignored --test-threads=1
```

| Case | Required outcome |
| --- | --- |
| Large init-v4 and durable resume | 757597-byte module, both modes, finite tracked work, changed JS heap retained |
| Scheduled and queue | Native handlers and tracked completion in both modes |
| Literal never-settling tracked promise | Host error, resident retired, no quiescent checkpoint |
| Buffered HTTP | GET/HEAD null-body clones and CL0 headers; 40000-byte POST and 80000-byte response |
| Inbound WebSocket | Real handshake, greeting, echo, close and resident reuse in both modes |
| Typed bindings | Native SQL Fetcher and host-only webhook verification across durable resume and key rotation |
| Cryptographic freshness | Distinct UUID/random bytes/AES-GCM keys/ECDSA keys across fresh template clones and durable resume |
| Bodyless response reuse | Seven sequential HTTP calls in both modes, including PATCH/DELETE 204; no early admission release or invalid chunked bodies |
| Legacy-format admission | Actual guest capability discovery and fetch for v1 bundles in both modes, without forcing init-v4 |
| Half-open WebSocket EOF | Two consecutive abnormal close/EOF connections through a max-one pool in each mode; cancelled owners retire and release capacity before their 30-second deadline |
| Request-owned deadlines | Built-in `AbortSignal.timeout()`/`any()` timers retire only after handler, transport and tracked work complete; tracked side effects survive, while ordinary future timeouts/intervals still fail safe-point validation |

The WebSocket host does not acknowledge a peer close on behalf of an
application that has not closed its outgoing side. The guest's close code and
reason are preserved. EOF before the guest closes cancels and reconciles the
owner; normal guest close/end still retains admission through tracked-work
completion. With a 2025 compatibility date, the sample explicitly replies to
the close event rather than silently enabling a newer auto-reply behavior.

The platform owner separately reported all five actual built sample packages
passing in both modes with the preceding guest and a frozen **debug integration host**:
Remix SSR plus an exact 255684-byte hydration asset, KV todo SQL CRUD/CAS,
synthetic-key Stripe verification/dedup/tamper, local SSE chat, and masked
WebSocket echo/close. These are owned test-cluster integration results, not
final release-host, live Azure, production density or cross-node guarantees.
Repeat all five against the corrected guest and final frozen host; the prior
integration result does not waive the discovered entropy and fast-completion
defects.
Real changed-state Pod replacement used the same authoritative RWO volume and
retained key; ephemeral `emptyDir` is not durable recovery.

The corrected candidate additionally passed the actual 959439-byte typed-agent
package: three SQL tool turns in request mode, and resident add/complete,
durable park, process replacement, generation-2 resume and retained task plus
three-turn conversation state. Original SDK timeout options were unchanged.
These remain owned local/test-cluster proofs, not live Azure service claims.

The authenticated-provider candidate was separately qualified against the
actual three-module Remix microphone application's pinned Azure SDK:
`@ai-sdk/azure 4.0.97` and `ai 7.0.133`. The immutable host SHA-256 was
`a0d9163c415f8de8654f5169aec76128d424226d9f97be2b98a81907f38d3bb1`;
the corrected executor and direct-ELF rootfs both had SHA-256
`c818ed87c08b05eb4833ad2e10b3dcd1a3ca9faa7d7e05a834acc1f8064e8212`.
The additive contract remained
`e1183a8690b9b95712e74d8889285a31170f8e8f0f1206213f40364f75b92dd1`.
These are artifact identities, not signed source-commit pins.

The local fixture used certificate-verified TLS, a non-CA server leaf with
an IP SAN, and host-only authentication. Both execution modes passed exact
SDK session/model/PCM control, 3200-byte audio frames, partial/final events,
Stop and Cancel. All seven sessions closed with code 1000, including SDK
no-argument close. Resident execution passed a real safe-point durable park,
zero live VMs, generation-2 resume, and a new provider connection. Fixture
audio and transcripts were synthetic: this is not live ASR, PC microphone,
AKS deployment, or cross-node recovery proof.

One bounded counter-only lifecycle measured 512 MiB virtual guest scratch,
328368128 bytes parked host RSS and 431800320 bytes peak host RSS.
The conservative parked/template/runtime overhead is therefore 314 MiB
(rounded up), not peak RSS minus virtual scratch. Its committed encrypted
checkpoint contained 103097438 ciphertext bytes. These values do not establish
framework density or cross-node recovery; `live_vms == 0` is not zero process
RSS. Releasing the home process is a separate platform operation.
Measured ciphertext size is evidence, not the configured checkpoint reservation
or a proxy for virtual guest scratch. Framework admission requires its own
measured framework profile; do not inflate or relabel this counter-only result.

Run `clippyw` inside Linux with MinGW installed. Do not run macOS validation
without a native macOS host and Apple SDK. Commit/sign/push only after the
final checks and real qualification, using the configured signing key and
fresh just-in-time YubiKey confirmation.
