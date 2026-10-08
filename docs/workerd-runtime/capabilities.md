# Host capabilities and private credentials

[Canonical demo runbook](../azure-workerd-hyperlight-runbook.md)


Empty capability policy denies fetch, raw networking, filesystem mounts and
logical services. Unknown fields and unsupported binding kinds fail closed.
Timers have finite validated limits. Fetch policy example:

```json
{
  "fetch": {
    "hosts": ["api.example.com"],
    "schemes": ["https"],
    "ports": [443],
    "methods": ["GET", "POST"],
    "paths": ["/v1/messages"],
    "query_parameters": {},
    "ip_ranges": ["203.0.113.0/24"],
    "allow_loopback": false,
    "allow_private": false,
    "allow_metadata": false,
    "credential": {
      "reference": "example-api",
      "value_file": "/run/secrets/example-api-header",
      "header": "authorization",
      "scheme": "https",
      "host": "api.example.com",
      "port": 443
    }
  },
  "storage": [{
    "name": "documents",
    "host_path": "/srv/hello/documents",
    "mode": "read_only",
    "max_operations": 128,
    "max_read_bytes": 1048576,
    "max_write_bytes": 1
  }]
}
```

The example CIDR is documentation-only: use the destination's genuinely
admitted addresses. HTTP methods, hostname, scheme, port and **all** resolved
addresses must pass before connect and credential insertion. The client has
proxies disabled and connects only to that pinned authorized resolution,
retaining normal TLS hostname verification. Host redirects are not followed:
every subsequent Worker fetch revalidates policy and credential destination.
Cross-origin requests cannot inherit the injected credential.

Resource grants are **exact normalized URL paths**, not an account-wide
hostname permission disguised as one model. Query parameter names/values are
explicitly admitted and duplicates are rejected. For an existing Azure chat
deployment, use its exact `/openai/deployments/<deployment>/chat/completions`
path, an admitted `api-version`, and
`body_policy: {"kind":"azure_chat","max_messages":32,"max_prompt_bytes":65536,"max_tokens":512}`.
The host validates actual JSON fields and token/prompt ceilings before DNS,
credential insertion and transmission; untrusted SDK constants are not
enforcement. Unknown operation fields, a different deployment path and missing
bounded `max_tokens` fail closed. Existing cloud resource creation and access
grants are out of scope and must already be authorized.

Credentials are fixed operator-admitted references, never arbitrary guest
URL/body/header interpolation. The trusted broker alone reads the private
secret file on each request, supporting rotation after resume; it inserts
only the admitted header for the exact destination. Conflicting guest
authentication headers, missing/invalid refs, unsafe permissions and overlap
with guest-mounted folders fail explicitly. Policy fingerprints bind the
reference/authority, not secret contents. Secret values are not in templates,
instance checkpoint metadata or diagnostics. A malicious authorized remote
server can echo data it received; this is not a blanket echo-prevention claim.

Filesystem mounts are confined named capabilities, `read_only` or
`read_write`, with operation/read/write quotas and trusted-host path/symlink
confinement. The stateless directory ABI is reconstructed by reopening the
same canonical, policy-bound confined directories; mount indices are sorted
canonically and every read/write reopens a path beneath its captured `Dir`.
Changed checkpoints retain host handle watermarks and logical request counts,
and cannot be loaded as pristine revision templates. Restores conservatively
require the same host OS/architecture/backend and CPU-feature compatibility
digest as well as artifact/policy identities.

Typed persistent `kv` and `sql` bindings reconstruct distinct per-VM
authority/session state against stable external backing files; their policy
contains names, paths and limits, not credentials. Azure KV/CAS/TTL, AI
provider live-resource proofs and arbitrary custom broker reconstruction
remain separate integration work; do not claim local SQLite is Azure Blob/KV.
`binding_budget_scope: "invocation"` is the typed-policy default: reset only
quiescent host-session budgets at each admitted invocation, never external
backing data or live handles. Explicit `"incarnation"` retains quotas for that
VM incarnation. This scope is part of the policy fingerprint.

SQL currently uses SQLite, not an Azure SQL service. Host configuration accepts
`kind: "sql"` and the legacy `kind: "d1"` alias. The private authority serializer
and ordered guest metadata retain the frozen `d1` spelling; the composite
operation remains `d1_batch`. This explicit boundary preserves existing policy
fingerprints and executor compatibility. New clients use `binding-sql`;
`binding-d1` remains a capability alias for older clients.

SQL uses a deny-by-default SQLite authorizer, not SQL string filtering.
ATTACH/DETACH, PRAGMA, VACUUM INTO, extension/file functions, foreign database
access and virtual tables are denied. Approved ordinary main-database CRUD
and pure/time functions support atomic UPSERT/RETURNING for CAS/TTL adapters.
SQL allocation limits, incremental result-size checks and a progress deadline
bound work; response quota failures roll back before mutation acknowledgement.
These are not a claim of total external database-storage quota enforcement.

The additive native `webhook` binding invokes a host-only HMAC verifier against
an operator-owned private key reference and trusted-host replay clock. The
scaffold may alias `env.webhook` to `WEBHOOK`; no signing key is passed to the
guest. Its exact raw-byte/signature limits and false/error semantics are in
the shared contract. Native unit/JS proofs do not replace real KVM callback
and recovery qualification.

Raw TCP/TLS/UDP/WebSocket broker rules separately authorize protocol,
hostname/IP, ports, DNS names and resolved IP ranges, with finite transport
quotas. They are not HTTP verb filters, and encrypted raw TLS cannot enforce
HTTP verbs. Registering the raw broker is not proof that every standard
Workerd socket API is wired to it.

The raw outbound WebSocket broker currently connects only to the admitted
host/port root path and supports subprotocol selection, not host-only
authentication headers or approved resource paths. It does not qualify
authenticated Azure MAI WebSocket streaming. Inbound WebSocket and outbound
HTTP streaming proofs cannot substitute for that separate capability.

### Authenticated provider WebSocket

The separate `provider_websocket` logical binding exposes
`env.transcribe.connect(publicWssUrl, subprotocols = [])`. The guest must
negotiate `authenticated-egress-websocket-v1`; the host advertises
`provider-websocket-v1` only for an admitted binding and compatible guest.
It does not grant a global WebSocket constructor or raw socket authority.

Configure `name`, exact `hosts`, `ports`, `paths`, `query_parameters`,
`subprotocols`, `ip_ranges`, `resolver_identity`, `credential`, and `limits`
in the app's `capability_policy.bindings`. `resolver_identity` is the
authorized resolver identity, not a guest-supplied DNS address; literal-IP
fixtures need no DNS lookup. Loopback/private access defaults to denied.
All resolved addresses must pass the IP policy before connecting.

The host-only credential contains `reference`, a private `value_file`,
`header`, `scheme: "https"`, `host`, and `port`. Its complete header value is
read for each new connection after destination/TLS checks, allowing rotation.
Never put credentials in a bundle, URL query, browser public configuration,
or guest connect arguments. TLS certificate verification remains mandatory;
optional `trusted_ca_file` adds an operator-owned PEM trust root without
disabling verification.

Finite `limits` are `max_connections` (at most four),
`queue_frames` (at most four retained data frames per direction, including
in-flight writes), `max_frame_bytes` (at most 16384), `max_total_bytes`
(at most 8388608 combined incoming/outgoing bytes per connection), and
`connect_timeout_ms` (at most 5000). Connections also retain the original
invocation deadline. Retry backpressure without advancing the send sequence
or charging rejected bytes. `bufferedAmount` is a JavaScript number.

Optional `session_policy` has `discriminator_pointer`, `control_type`, and
`required_values`, a JSON-pointer map of required scalar values. It requires
the approved initial control before audio and checks subsequent matching
control updates, enabling model/format restrictions without provider-specific
transport code.

Close every provider socket before completing the handler or parking:
`socket.close()` normalizes to code 1000 with an empty reason; explicit valid
wire close codes are also supported. Unexpected EOF is an error followed by
the guest's abnormal close event, not a graceful wire close. Cancellation and
failed transports retain ownership until the host worker actually joins;
pending cancellation is not a checkpoint safe point. Durable recovery
reconstructs authority and dials a new connection, never serializes a live
TLS socket or credential into a checkpoint.

Local real-TLS/KVM tests cover both modes, repeated explicit/default close,
16KiB duplex frames, and the resident checkpoint safe point. Host regressions
cover credential rotation, abnormal EOF, joined cancellation, authority and
quota boundaries. These are not live MAI, PC microphone, AKS, or cross-node
qualification; the pinned SDK/application fixture is a separate gate.
