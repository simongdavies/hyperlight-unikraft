# Resident checkpoints and failure recovery

[Canonical demo runbook](../azure-workerd-hyperlight-runbook.md)


For per-instance lifecycle control, add `instance_home` to a resident app.
This **replaces** the conventional resident pool; one logical instance is one
VM, with no hidden pool behind it:

```json
{
  "checkpoint_policy": "durable",
  "database_path": "/state/checkpoints.db",
  "encryption_key_path": "/run/secrets/checkpoint-key",
  "max_checkpoint_bytes": 2147483648,
  "idle_timeout_secs": 60,
  "checkpoint_timeout_secs": 30,
  "scratch_directory": "/tmp"
}
```

Durable is the default checkpoint policy. The external key is exactly 32 raw
bytes in an operator-owned private regular file (mode 0600 on Linux); do not
put it in the package, checkpoint volume, guest environment or image. The
database is also private. For explicit local-only testing use
`checkpoint_policy: "local"` and null database/key paths. Local-only state
does not survive home/process replacement.

Checkpoint capture/decryption needs writable **private temporary scratch**.
`scratch_directory` selects the existing operator-provided directory; omitted
uses the process temporary directory. A read-only root filesystem therefore
needs a writable bounded tmp volume. The mock/KVM Pod-replacement proof used
256 MiB tmp with a 64 MiB fixture VM; do not assume that is enough for a real
512 MiB Workerd VM. Size and qualify scratch from actual snapshot/decryption
footprints and simultaneous checkpoint limits. Scratch contains transient
plaintext OCI data, is not guest-mounted, and is unsealed/removed explicitly
on normal success/error paths. Durable ciphertext, identity, ownership and
publication transactions live only in the FULL-synchronized authoritative
database on the persistent volume, never in tmp.

Create a caller-selected instance under a fixed admitted app/revision:

```bash
curl --fail http://127.0.0.1:8081/v1/instances/hello-1/create \
  -H 'Content-Type: application/json' --data-binary \
  '{"protocol_version":1,"request_id":"create-1","app_id":"hello","revision":"hello-v1","expected_generation":0,"checkpoint_policy":"durable"}'
```

Use the real bundle's `worker_version` for `revision`, not the illustrative
`hello-v1` above. The active reply includes the full runtime-local fenced
invocation endpoint. Every invocation validates app, revision, route instance,
generation, matching correlation IDs and a positive remaining lifetime.
Do not route a fiber to the unfenced app/pool invocation endpoint.

`park`, `resume` and `release` use the same lifecycle envelope with the
**current** generation. Park closes single-owner admission, negotiates a
tracked-work safe point, validates zero guest work and no unreconstructable
host handles, and snapshots the **changed live VM**, not `worker.image` or an
initialized template. It atomically commits authenticated AES-256-GCM
encrypted chunks before terminating the VM and publishing `parked`.

After park, `instances[].live_vms` must be zero. Idle expiry uses this same
park operation; a live stream, socket or handler is not idle. Resume claims
the parked generation atomically, verifies artifacts/target/policy, reconstructs
host sessions, and publishes generation+1 only after restoration. Racing/stale
claims and corrupt/wrong-key checkpoints fail; there is no pristine fallback.
Release is acknowledged only after VM teardown and owner-thread reconciliation.
Superseded chunks are retained until the replacement checkpoint is safely
parked, then removed; fenced parked/failed deletion is explicit.

The durable-file backend uses a **single authoritative local SQLite database**
with DELETE journal and FULL synchronization. It is not a distributed lease
service. Replacement-host testing requires exclusive persistent block-volume
handoff, verified termination/fencing of the previous owner, and the same
externally retained key. Do not copy the database per Pod, put it on arbitrary
RWX/NFS storage, claim recovery on `emptyDir`, or infer cross-node safety from
local tests. Ambiguous active/resuming ownership stays failed/quarantined until
authoritative reconciliation; it is not silently reclaimed.
Unloaded records expose the last durably captured request count, authenticated
from bounded checkpoint metadata without loading the VM image. An unreconciled
active/resuming owner is not a current live-count claim; parked counts retain
their durable history rather than resetting to zero after process replacement.

Layered/incremental snapshots, a parked-snapshot hot cache and registry-backed
snapshot distribution are roadmap work, not enabled by this implementation.
They require separate format compatibility, ownership and real recovery
qualification; no measured Hyperloom performance improvement is claimed here.
