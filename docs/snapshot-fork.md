# Experimental VM-per-process snapshot fork

This document records a prototype design for an OpenShell-oriented process
model in which each Unix-like process runs in a separate Hyperlight-Unikraft
(HLUK) VM. It is a design and acceptance contract, not a claim of general
`fork(2)` support.

## Status and scope

The first prototype proves only the snapshot-fork mechanism and its host
lifecycle. It targets Linux x86-64 with KVM and a single live application
thread at the fork point. It does not integrate with OpenShell or replace
HLUK's existing `vfork()+execve()` support.

## Verified current behavior

- HLUK has one vCPU, a cooperative scheduler, and a single address space.
  Threads share that address space. `vfork()+execve()` works for sequential
  subprocesses, while `fork()` and `clone()` without `CLONE_VM` are rejected.
- The Hyperlight 0.18 transport used by this branch moves guest/host calls,
  console/stdin, and snapshot checkpoints over VIRTIO packed rings.
- The guest transport copies received messages out of the transport buffers
  immediately. `hl_transport_checkpoint()` resets the guest-to-host and
  host-to-guest rings to a canonical snapshot state.
- A snapshot cannot be taken while the synchronous host call that requests a
  fork still owns live transport descriptors. Fork therefore needs two
  phases: request/prepare, return from that host call, reach a clean execution
  boundary, checkpoint, clone, then resume.
- `Sandbox::snapshot()` produces an in-memory snapshot from which multiple
  sandboxes can be restored. Hyperlight restores their private guest memory
  with copy-on-write behavior.
- Ordinary HLUK restore invokes `resume`. The current `resume` path reseeds the
  guest CSPRNG, reconciles hostfs mounts, recreates listeners, re-anchors the
  clock, and makes connected host sockets from the old VM read as closed.
- `InterruptHandle::kill()` is the hard termination primitive. It interrupts a
  running guest entry and poisons the sandbox. It cannot stop a host function
  that is already executing. A killed process VM must be dropped, not pooled
  or reused.
- Hyperlight currently rejects writable dynamic memory mappings. Its public
  `Sandbox::map_region()` accepts read-only mappings but returns the
  "Writable mappings not yet supported" error when `WRITE` is requested.

## Proposed behavior

An OpenShell sandbox is a host-side object containing policy, workspace
providers, a virtual process/task registry, and a capability registry. A Unix
process is represented by one HLUK VM plus host lifecycle state:

```text
logical sandbox
  policy + workspace + providers
  process registry
    virtual PID -> VM, parent PID, status, waiters
  capability registry
    capability ID -> host-owned resource + fork disposition
```

At a supported fork point:

1. The guest calls a fork-prepare host capability.
2. The host validates the restrictions and records the request, then returns
   from the host call without cloning.
3. The guest reaches a clean scheduler boundary. The kernel checkpoints the
   VIRTIO rings into their canonical empty state.
4. The host snapshots the VM and allocates a virtual PID.
5. The original VM resumes as the parent and receives that virtual PID.
6. A second `AppSandbox` is restored from the snapshot, receives zero, and is
   registered as the child.

Both VMs continue from the same logical post-fork point. Private memory starts
with identical contents but diverges through Hyperlight snapshot copy-on-write.
The host, not the guest kernel, owns the process tree, virtual PID allocation,
wait/reaping, termination status, policy, and all external capabilities.

Fork restore requires a distinct reason and contract from warm restore. The
prototype supplies the fork result through fresh per-VM host capabilities, but
a production kernel path should distinguish at least `WarmRestore`,
`ForkParent`, and `ForkChild`. Transport/control channels and their
authentication are always freshly constructed for each VM and are never
inherited as process resources.

## Resource inheritance

Guest descriptor numbers identify host-owned capabilities; they are not native
host file descriptors. `SCM_RIGHTS` can transfer a capability reference and
rights, but cannot turn a host FD into a native guest FD.

Every capability type must declare one fork disposition:

| Disposition | Meaning |
|---|---|
| Share open description | Parent and child refer to the same host offset/state. |
| Duplicate view | New capability object over the same underlying resource. |
| Private copy | Child receives independent copied state. |
| Recreate | Child receives a freshly established equivalent resource. |
| Close in child | Capability is absent from the child. |
| Unsupported | Fork is rejected while the capability is live. |

OpenVMM mesh is the model: typed movable capabilities, ports, bounded
backpressured pipes, cancellation/deadlines, and host-side ownership. The
prototype connects child stdout to a bounded host channel and uses channel
closure as EOF.

Private guest memory uses Hyperlight snapshot copy-on-write. Explicit shared
memory requires one host-backed shared-memory capability mapped into both VMs.
That is not implemented by this prototype because writable external mappings
are not supported. The required Hyperlight change is a safe writable mapping
API that registers host-backed pages with the sandbox memory manager, preserves
or explicitly rebinds them by stable capability identity on snapshot restore,
and defines rollback/unmap behavior. HLUK then needs a guest mapping operation
that maps that capability at an agreed virtual address.

## Constraints and invariants

- Fork preparation succeeds only with one live application thread in the
  initial experiment. Full POSIX multithreaded fork semantics are not claimed.
- The snapshot is taken only after the requesting host call has returned and a
  clean guest boundary has been observed.
- A virtual PID is allocated and published by the host; it is not a VM-local
  kernel PID.
- Parent and child receive distinct fork results exactly once.
- New transport/control channels and authentication are created per VM.
- Resource inheritance is explicit. Unknown capability types reject fork.
- Wait status is host-owned and delivered once; reaping removes the task from
  the registry.
- Hard cancellation uses `InterruptHandle::kill()`. The resulting poisoned VM
  is dropped and reported as deterministically terminated.
- Host functions must not block indefinitely while holding resources needed to
  cancel or reap a child. Bounded output pipes intentionally apply
  backpressure, so a host consumer must drain them.

## Threat model

The guest workload and its memory are untrusted. It may forge capability IDs,
repeat protocol messages, write excessive output, race cancellation, or crash
during fork. The host must validate identity, rights, process ownership,
protocol phase, payload bounds, and resource disposition on every operation.
Virtual PID reuse must not let a stale child reference control a new process.

The snapshot is trusted only as guest state produced by the current sandbox
contract. It must not contain reusable host authentication material or raw host
resource handles. A child cannot gain capabilities that the parent did not
hold or that policy did not explicitly recreate. Output and control queues are
bounded to prevent unbounded host memory growth.

The prototype does not defend against denial of service beyond bounded output
and hard termination. CPU, memory, process-count, and wall-clock quotas belong
in the host task registry.

## Non-goals

- Generic `fork()` compatibility or a POSIX conformance claim
- Arbitrary file-descriptor inheritance
- PTYs, sessions, process groups, or job control
- Multithreaded POSIX fork
- Preservation of connected sockets
- OpenShell integration
- Reusing a killed or otherwise poisoned VM
- Emulating shared memory with copied private values

## Prototype acceptance evidence

The focused `fork_prototype` integration test is the executable contract:

1. A guest calls `fork.prepare`, the call returns, and the guest reaches a
   timer-backed clean boundary before the host snapshots it.
2. The original VM receives a host virtual PID and a restored VM receives zero.
3. Both executions continue from the same in-flight call and mutate a
   pre-snapshot guest object to different values, proving private divergence.
4. A host task wrapper reports the child's completed status through `wait`.
5. Child stdout traverses a bounded synchronous channel; dropping the child VM
   and its handler closes the channel and produces EOF.
6. A restored child enters the guest `SleepCancel` function,
   `InterruptHandle::kill()` terminates the running entry, the poisoned VM is
   dropped, and the parent observes a deterministic terminated status.
7. Shared writable memory is reported as blocked by the exact Hyperlight API
   limitation above; no copied value is presented as shared memory.

Passing this test on Linux x86-64 KVM demonstrates the prototype only. It does
not promote the mechanism to supported HLUK behavior.
