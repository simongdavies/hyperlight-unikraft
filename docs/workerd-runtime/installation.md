# Standalone installation and immutable packages

[Canonical demo runbook](../azure-workerd-hyperlight-runbook.md)


The standalone core has no Kubernetes, Knative or fiberd dependency. Those
systems may supervise an immutable revision home through the operator-local
interface. An authenticated platform gateway owns tenant authentication and
network authorization; do not expose the management listener publicly.

Mutually untrusted tenants require separate app/revision homes and `hluk`
processes. The standalone multi-app router is useful for trusted operator
applications, but is **not** a cross-tenant host-process isolation boundary.
Callbacks capture admitted app/revision authority and per-VM sessions; they
do not consult a mutable global "current tenant".

`docs/workerd-runtime-contract-v1.json` is the shared additive target contract,
including positive and negative vectors. A schema is not a capability claim.
`GET /v1/capabilities` on the management listener advertises only available
interfaces; callers must reject required capabilities that are absent.
Every configured app probes the actual guest before Ready, including legacy
bundle formats in either execution mode. The qualified guest still accepts the
unchanged v1 envelopes; older executors without the required completion/control
extensions fail admission explicitly rather than silently downgrading safety.

Create `host-config.json` with operator-owned Linux paths:

```json
{
  "rootfs_path": "/runtime/rootfs.img",
  "executor_path": "/runtime/executor",
  "apps": [{
    "route": {
      "app_id": "hello",
      "hostnames": ["hello.example.test"],
      "path_prefix": null
    },
    "bundle_path": "/app/bundle.json",
    "scratch_memory_mb": 512,
    "execute_timeout_secs": 30,
    "capability_policy": {},
    "pool": {
      "kind": "disposable",
      "max_concurrent_sandboxes": 2,
      "queue_capacity": 8
    },
    "connection_affinity": "none",
    "snapshot_dir": null,
    "instance_home": null,
    "streaming": false
  }]
}
```

The rootfs and executor must be the matching packaged guest build, with its
dependency-closure digest alongside the executor. The compatible Unikraft
kernel is embedded in `hluk`; do not replace it independently.

## Migrating to upstream v0.18.0

The migration base is upstream release commit
`a2ad7ab464bbd46b49798452281b848e47f3cea5`, using Rust 1.99 and
Hyperlight 0.18's virtqueue transport. Rebuild the direct-ELF Workerd kernel
with `just build-workerd-kernel` and native test kernels with
`just build-native-kernel`. An older embedded kernel is not a compatible
substitute. Keep the previous release, packages, and persisted state intact
until the new revision has been qualified.

The generic upstream host-call ioctl remains distinct from the additive
typed-v1 bridge: on x86-64 their encoded requests are respectively
`0xc0384803` (56-byte structure) and `0xc0c84803` (200-byte structure).
The typed bridge retains existing executor wire values and validates its
version, reserved fields, argument lengths, UTF-8, and active invocation.
The host collector accepts byte-oriented `HostWrite`, bounds `HostWriteErr`
separately from protocol responses, and denies byte-oriented `ReadStdin`.
`python3 tools/test-workerd-hostcall-abi.py` tests native layout and helper
failure paths; it does not replace real KVM qualification.

Pre-v0.18 templates and changed-instance checkpoints are incompatible.
Use a new revision home and newly initialized templates; never restore an
old heap into the new kernel, relabel its metadata, or silently substitute
fresh state for a failed resume. See [checkpoint migration](checkpoints.md).
Published old artifact hashes remain evidence for the old release only.

```bash
hluk workerd-identity --config host-config.json
hluk workerd-host --config host-config.json \
  --bind 127.0.0.1:8080 --admin-bind 127.0.0.1:8081 \
  --max-connections 256 --max-admin-connections 16 \
  --drain-timeout-ms 10000
curl --fail http://127.0.0.1:8081/__hyperlight/readyz
curl --fail http://127.0.0.1:8081/__hyperlight/status
curl --fail http://127.0.0.1:8081/v1/capabilities
curl --fail -H 'Host: hello.example.test' http://127.0.0.1:8080/
```

When the admin listener is separate, the app listener does not expose probes
or control routes, and the admin listener never routes ordinary app requests.
The connection budgets are independent, include idle keep-alive sockets, and
bound connection threads. Each HTTP head/body has an absolute read deadline.
Shutdown closes admission on existing sockets as well as new connections;
already admitted work retains accounting through the response write and guest
completion. A disconnect cancels queued/active guest work and its fetch/timer
sessions. A timeout or cancellation retires the VM; it is not reused.

`readyz` includes configured app identities. Compare
`apps[].identity.worker_version`, `bundle_sha256`, and
`capability_policy_sha256` with the **actual** output from
`workerd-identity`. Do not substitute an archive digest or a hash of arbitrary
Go/JSON serialization for the canonical bundle hash. A configured home with
zero instances can be ready to admit instances; this does not mean a VM is
already live. `active` counts executing work, while `live_vms` counts actual
resident VMs.


## Larger immutable bundles

Legacy bundle/init protocols 1/2/3 retain their original 48 KiB source and
60 KiB envelope bounds. Explicit package `protocol_version: 4` selects the
additive chunk loader: at most 32 modules, 1 MiB decoded source per module,
8 MiB aggregate source, and 16 MiB package JSON. Module metadata is sent in
a bounded init-v4 descriptor; the guest reads exact chunks of at most 16 KiB
from immutable host-owned sources and checks each SHA-256 before constructing
the runtime. Reads are bound to revision, bundle identity and module name,
and are revoked before the initialized template is captured. A guest cannot
request arbitrary host paths.

Do not describe a parsed large package as an executed large Worker. Real
init-v4 guest execution must pass before advertising its larger source limits
or deploying a large framework bundle. The 757597-byte init-v4 fixture is
synthetic; actual framework execution is separate evidence. Larger assets require
the negotiated stream transport; init-v4 does not raise buffered body limits.
