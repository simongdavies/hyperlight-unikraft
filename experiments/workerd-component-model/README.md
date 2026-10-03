# Workerd Component Model decision fixture

## Decision

Use pinned `jco` transpilation with **minified synchronous instantiation** for
the Workerd-on-Hyperlight prototype. Do not wait for or build a native
Component Model executor into workerd.

Workerd's documented Wasm surface accepts core modules through JavaScript's
`WebAssembly` API. It does not expose a native Component Model/WASI Preview 2
loader. The checked-in `component.wasm` is intentionally a Component Model
binary, and `npm test` proves that V8's core-module API rejects it. The pinned
`jco` path lowers the same component to an ES module plus a core Wasm module,
which matches workerd's existing module registry.

The tested mode is important. Split output using the default URL loader fails
because workerd does not provide a usable module URL to the generated loader.
Inline output with top-level-await compatibility can lose the first request
while initialization settles. `--instantiation sync` lets workerd import the
generated core Wasm module directly and initialize without URL loading or
top-level await.

This fixture does not create new storage or network integrations. The component
has no imports, the worker has no bindings, and the reserved `internet` service
is explicitly replaced with a network service whose allow list is empty.

## Pinned inputs

| Artifact | Version |
| --- | --- |
| `wasm-tools` used to build the fixture | `1.252.0` |
| `@bytecodealliance/jco` | `1.35.0` |
| `@bytecodealliance/preview2-shim` | `0.26.0` |
| `workerd` | `1.20260925.1` |
| workerd compatibility date | `2026-09-25` |
| fixture world | `hyperlight:component-evaluation/arithmetic@0.1.0` |

`package.json` uses exact versions. `bundle.lock.json` records the toolchain,
byte length, and SHA-256 for every identity-bearing source and available
generated artifact. Run `npm run lock` again after transpilation so the
generated JS and core Wasm join the identity. The snapshot identity for
integration must include that lock file's content digest, the Unikraft kernel
digest, initrd digest, and Hyperlight snapshot format/version.

## Reproduce

The exact registry packages are executed through the existing npm cache; they
are not added to or installed from the repository manifest:

```text
npm run build:component
node scripts/transpile-workerd.mjs
npm run lock
npm test
npm run probe
npm exec --yes --package workerd@1.20260925.1 -- workerd serve workerd.capnp componentEvaluation
```

Then request `http://127.0.0.1:8787/add?left=20&right=22`.

Observed on the evaluation host:

- `wasm-tools 1.252.0` parsed and validated the 196-byte component.
- Node.js `v22.20.0` rejected native Component Model loading through the core
  Wasm API.
- Two pinned `jco 1.35.0` lowerings produced byte-identical output.
- Pinned `workerd 1.20260925.1` returned `{"result":42}` for 1,000 consecutive
  requests with one distinct response body.
- Request latency was 0.981 ms p50, 1.819 ms p95, and 3.896 ms p99 on the
  evaluation host. Direct workerd process-to-ready time was 73.786 ms p50.
- The deny-all `internet` service blocked HTTP egress with workerd's
  `connect() blocked by restrictPeers()` diagnostic.
- Generated runtime size was 45,255 bytes, 45,059 bytes over the 196-byte
  fixture, passing the 64 KiB absolute-overhead gate.

Exact package integrity values, output hashes, and measurements are in
`evidence.json`; runtime artifact hashes are in `bundle.lock.json`.

## Acceptance criteria

| Area | Gate |
| --- | --- |
| Native feasibility | A checked-in Component Model binary loads directly through workerd without JS lowering. **Failed; workerd accepts core Wasm only.** |
| Functional | `GET /add?left=20&right=22` returns `{"result":42}` for 1,000 consecutive requests. **Passed.** |
| Ambient capabilities | No component imports, no storage bindings, and outbound `fetch()` fails because `internet.allow=[]`. **Passed.** |
| Policy | Inputs outside signed 32-bit range fail with HTTP 400; request target over 1,024 bytes fails with HTTP 413. |
| Quota | Outer Hyperlight executor interrupts any invocation exceeding 50 ms and caps the guest at 64 MiB for this fixture. |
| Snapshot identity | Startup refuses a snapshot unless bundle lock, kernel, initrd, workerd, compatibility date, and policy digests all match. |
| Determinism | Two clean builds produce byte-identical generated JS/core Wasm; 1,000 calls produce identical response bytes. **Passed.** |
| Cold performance | Snapshot load plus first call p95 <= 25 ms on the agreed reference host. **Pending Hyperlight guest integration; direct workerd process p50 was 73.786 ms.** |
| Warm performance | Restore plus call p95 <= 5 ms and p99 <= 10 ms over 10,000 calls, excluding HTTP client time. **Workerd-only proxy passed at 1.819/3.896 ms over 1,000 calls; Hyperlight restore remains pending.** |
| Size | Transpiled JS plus core Wasm <= 1.25x the source component size or <= 64 KiB absolute overhead, whichever is larger. **Passed via 45,059-byte absolute overhead.** |
| Security | No WASI shim is emitted for this import-free world; generated JS contains no filesystem, process, or network imports. **Passed for the focused fixture.** |

## Integration shape

Reuse the existing lifecycle rather than adding an executor lane:

1. Treat the transpiled directory as an immutable bundle placed in the existing
   initrd/bundle assembly step.
2. Boot workerd once with the deny-all config, call `/health`, then use
   `Sandbox::snapshot_now()`.
3. For each invocation, use the existing restore/call sequence and the same
   host timeout/interrupt pattern as `pyhl::Runtime::run_code_with_timeout`.
4. Keep network disabled by not calling `SandboxBuilder::network`; the inner
   workerd deny-all service is defense in depth, not a replacement policy.
5. Carry the canonical bundle identity beside `snapshot.hls`; reject rather
   than silently load when any identity field differs.

Required dependencies are a Workerd-capable Unikraft guest image, an initrd
assembly hook for the transpiled bundle, a small guest entrypoint mapping the
existing named-call protocol to workerd's local service, and verified snapshot
identity metadata. Existing hostfs and network dispatch code are not required.

## Aggressive decision target

The first **2-hour** gate is complete: exact packages were verified, lowering
is deterministic, workerd serves the component, deny-all egress is proven, and
the workerd-only latency gate passes. Use the remaining **2 hours** to package
the locked bundle into the existing initrd, snapshot after `/health`, run the
restore/call response loop, and bind kernel/initrd/bundle/snapshot identities.
Continue only if Hyperlight warm p99 remains at most 10 ms and snapshot load
plus first call p95 is at most 25 ms.
