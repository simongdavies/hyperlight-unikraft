# Registered host functions

Hyperlight-Unikraft registers the kernel lifecycle host functions documented in
[`execution.md`](execution.md). Filesystem and socket functions remain opt-in as
documented in [`fs.md`](fs.md) and [`net.md`](net.md).

The bounded broker source adds two further opt-in host functions:

| Registered function name | Registered by default | Registration API |
|---|---|---|
| `__hl_broker_v1` | **Off** | `SandboxBuilder::broker_runtime()` with a network adapter |
| `WorkerdLogicalServiceV1Invoke` | **Off** | `SandboxBuilder::broker_runtime()` with a logical service |

Neither function is registered by default. Constructing
`BrokerRuntime::deny_all()` alone grants no network or logical-service
capability. The embedding host must explicitly install a bounded adapter or
logical binding allowlist before the corresponding function exists.

Broker runtime counters, tracked handles, and adapter state are reset before a
fresh sandbox evolves and whenever `AppSandbox::restore()` replaces the guest
state. `AppSandbox::snapshot()` rejects a sandbox with registered broker host
functions because guest-visible handles cannot be saved with their host
resources. Registering these functions does not enable ambient networking and
does not prove that a Workerd guest invokes them correctly.

The implementation is in [`src/lib.rs`](../src/lib.rs) and
[`src/broker_runtime.rs`](../src/broker_runtime.rs). Protocol and policy details
are in [`workerd_broker_protocol.md`](workerd_broker_protocol.md).
