# Workerd bounded broker protocol foundation

The shared contracts in `src/broker.rs` define the host-owned boundary for
Workerd compatibility work. They are intentionally independent of the current
`lib/hostsock` JSON handlers so TCP/TLS, UDP, WebSocket, scheduled, and queue
lanes can implement against one policy and quota model without duplicating it.

## Security posture

- No ambient guest networking. `BrokerPolicy::deny_all()` is the baseline.
- Egress requires an explicit host, port range, and protocol match.
- Loopback, link-local, unspecified addresses, and `localhost` remain denied
  even if a rule attempts to list them.
- DNS has a separate allowlist for both names and resolver IPs.
- TLS is terminated by the host. `TlsProfile` exposes only server name, ALPN,
  and minimum version; guest-visible credential or trust-root fields do not
  exist.
- Scheduled and queue events are created only through `TrustedIngress`
  constructors at the trusted host boundary and carry the same
  `RequestIdentity` used for policy, quota, and audit correlation.
- `RequestIdentity { workload_id, snapshot_id, attempt }` is host-owned and
  supplied out-of-band to the adapter. Guest wire data carries only a bounded
  `request_id` correlation value and cannot select workload or snapshot identity.
- Every request has finite connection, socket, datagram count/size, WebSocket
  message count/size, aggregate byte, time, operation-rate, and concurrency
  budgets.
- `BrokerBudget::reset_for_fresh_vm()` clears all host-side counters. Existing
  restore wiring already clears the live host socket table, so protocol
  implementations reset both before fresh boot and at the restored request-VM
  assignment boundary. A sandbox with a broker runtime rejects snapshots:
  guest-visible broker handles cannot be captured with their host resources.

## Protocol lane integration

Each lane should translate its guest wire request into `BrokerRequest`, call
`BrokerPolicy::authorize`, charge `BrokerBudget` before allocating or sending,
and release connection/socket/concurrency counters on close. The first live
integration should replace the current fixed `MAX_SOCKETS`, `MAX_NET_SEND`, and
`SOCKET_TIMEOUT` checks with values derived from a request-scoped broker budget
while retaining those constants as conservative compatibility defaults.

TCP and UDP can initially wrap the existing `SocketTable`. TLS and secure
WebSockets require a host TLS implementation chosen by the embedding host; no
private keys, bearer tokens, client certificates, or trust roots cross into the
guest. WebSocket implementations must charge each complete message and each
frame's bytes, enforce the message byte limit before buffering, and count the
upgrade as a connection plus socket.

Scheduled and queue delivery do not create inbound guest listeners. The trusted
host creates a bounded `TrustedIngress`, starts or restores a fresh request VM,
injects the payload through the normal request dispatch path, and discards all
broker state afterward.

## Durable Objects seam

`src/actor.rs` is deliberately separate from request-VM parity. The
minimal seam is:

1. Stable `ActorId` namespace/key identity.
2. `ActorRouter` ownership lookup returning an `ActorRoute` generation.
3. Serialized delivery per actor and stale-generation rejection.
4. Explicit activate, deliver, alarm, and passivate lifecycle events.
5. Durable storage ownership and transaction semantics supplied by a later
   actor runtime, outside the disposable request VM.

An actor activation may use a Hyperlight VM internally, but it must not inherit
the request VM's fresh-restore lifecycle or treat request snapshots as durable
actor state.

## Integration dependencies

- Guest wire schemas for broker requests and handles in the Workerd/Unikraft
  adapter.
- Host TLS/WebSocket implementation and certificate/trust configuration.
- Request dispatcher ownership of `RequestIdentity` and budget construction.
- Queue/scheduler adapters that can attest trusted ingress and apply retry
  policy.
- Metrics/audit export keyed by request identity and budget rejection reason.
- Actor placement and durable storage contracts before Durable Objects
  execution can begin.

  ## Deterministic fixtures

  Cross-runtime semantic fixtures use UTF-8 TSV with a comment header and no
  platform-derived values. `tests/fixtures/broker_policy_cases.tsv` fixes
  the column order as protocol, normalized host, decimal port, and expected
  authorization. A future serialized ABI must use an explicitly versioned schema
  and numeric tags; Rust enum layout is not a wire contract.

  ## Guest wire v1

  `src/broker_wire.rs` defines an explicit big-endian binary envelope:

  - Four-byte magic `HLBR`, `u16` version `1`, then message type (`1` request,
    `2` response).
  - Strings are `u16` byte length plus UTF-8 and are capped at 1024 bytes.
  - Payloads are `u32` byte length plus bytes and are capped below 1 MiB.
  - Request fields begin with guest correlation `request_id`, then an explicit
    operation tag. Host-owned workload/snapshot identity is not present.
  - Request operation tags are `1` TCP connect, `2` TLS connect, `3` UDP send,
    `4` WebSocket open, `5` WebSocket send, and `6` close.
  - Endpoint host tags are `1` IPv4, `2` IPv6, and `3` normalized DNS.
  - Response statuses are `0` ok, `1` denied, `2` quota exceeded, `3` invalid
    request, and `4` host error. Responses expose only stable category codes.
  - Unknown versions/tags, invalid booleans, malformed UTF-8, oversized fields,
    truncation, and trailing bytes fail closed.

  `tests/fixtures/broker_wire_v1_tcp.hex` fixes the exact bytes for a v1 TCP
  request. The Workerd logical-service JSON v1 ABI is a separate protocol; both
  share the out-of-band `RequestIdentity { workload_id, snapshot_id, attempt }`
  semantics and non-leaking status classes.

  ## Host adapter and audit event

  `src/broker_adapter.rs` supplies a `BrokerExecutor` seam beneath decoding,
  policy, quota, and handle lifecycle enforcement. The dispatcher calls
  `handle_wire(bytes, host_identity, elapsed)`; guest bytes cannot replace the
  host identity. Fresh-VM reset clears executor resources, tracked handles, and
  all budget counters.

  `src/broker_runtime.rs` is the actual Hyperlight registration boundary.
  `SandboxBuilder::broker_runtime()` conditionally registers the raw host
  functions `__hl_broker_v1` and `WorkerdLogicalServiceV1Invoke`; neither exists
  by default. The runtime captures trusted identity, rejects identity mismatch
  before decoding or adapter execution, checks logical binding registration
  after canonical inspection but before typed dispatch, and resets all registered
  services before fresh boot, after snapshot load, and on every in-place restore.

  Every invocation returns a host-only `BrokerAuditEvent` with schema version,
  host identity, optional guest request ID, stable action/decision/outcome/code,
  optional destination, request/response byte counts, and post-operation budget
  usage. Its canonical TSV record excludes payloads, credentials, raw host errors,
  backing paths, and executor details.

  ## Generic logical-service seam

  `src/logical_broker.rs` provides a protocol-independent host dispatcher for
  the Workerd JSON v1 lane without copying its schema or Rust types. Validated
  request/response types implement metadata traits; the dispatcher then applies:

  1. host-owned identity plus deny-by-default `LogicalPolicy`;
  2. `LogicalBudget::try_reserve` before dispatch;
  3. typed `LogicalServiceAdapter::dispatch` with no credential/backing arguments;
  4. canonical response and request-ID validation;
  5. exactly one `settle` for every issued reservation, including invalid
     responses and adapter errors;
  6. payload-free audit with identity, binding, operation, distinct decision,
     safe status/code, and request/response byte counts.

  Policy denial and quota denial do not settle because no reservation was issued.
  Budget errors, invalid adapter responses, and adapter exceptions are distinct
  audit decisions. Deterministic tests use the Workerd fixture byte counts of
  308-byte request and 146-byte response while remaining independent of its JSON
  implementation.
