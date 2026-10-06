# `hluk workerd-host` — the resident, multi-app Workerd host

`workerd-host` is a long-running, orchestrator-ready HTTP server for one or
more Workerd apps hosted in one process. It is additive to, and does not
change the behavior of, the existing single-app, single-request
`workerd-demo` example or the one-shot `hluk workerd` subcommand: those stay
disposable-only (one VM per request). `workerd-host` adds, on top of the same
hypervisor/sandbox primitives:

- **Resident VMs** that serve many requests before being recycled, alongside
  the unchanged disposable pool, selectable per app.
- **Multiple apps in one process**, routed by `Host` header and optional
  path prefix.
- **Connection affinity** (`sticky`), pinning every request on one keep-alive
  TCP connection to the same resident VM.
- A small **orchestrator contract**: liveness/readiness/status endpoints and
  deterministic exit codes, so an external process manager (not part of this
  repository) can supervise it.

See [`docs/azure-workerd-hyperlight-runbook.md`](../../docs/azure-workerd-hyperlight-runbook.md)
for how this fits into the wider build/runtime story, and
[`examples/workerd-executor/README.md`](../workerd-executor/README.md) for
the shared rootfs/executor this host restores every app's VMs against.

## Running it

```sh
hluk workerd-host --config apps.json --bind 127.0.0.1:8080
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--config <path>` | required | Path to the host configuration JSON (schema below). |
| `--bind <addr>` | `127.0.0.1:8080` | Address serving both app traffic and, unless `--admin-bind` is set, the `/__hyperlight/*` contract endpoints. |
| `--admin-bind <addr>` | shares `--bind` | Optional separate address serving only `/__hyperlight/*`, so an orchestrator can probe liveness/readiness on a port never exposed to app traffic. |
| `--drain-timeout-ms <ms>` | `10000` | How long to wait for in-flight requests to finish after `SIGTERM`/`SIGINT` before giving up (exit code 1). |

## Configuration schema (`--config`)

```jsonc
{
  "rootfs_path": "/abs/path/rootfs.img",
  "executor_path": "/abs/path/executor",
  "apps": [
    {
      "route": {
        "app_id": "storefront",
        "hostnames": ["shop.example.test"],
        "path_prefix": null
      },
      "bundle_path": "/abs/path/storefront-bundle.json",
      "scratch_memory_mb": 384,
      "execute_timeout_secs": 30,
      "capability_policy": {},
      "pool": {
        "kind": "resident",
        "capacity": 4,
        "queue_capacity": 64,
        "max_requests_per_vm": 10000,
        "max_lifetime_secs": null
      },
      "connection_affinity": "sticky"
    }
  ]
}
```

- `rootfs_path` / `executor_path`: the one shared guest kernel/executor image
  every app's VMs are restored against (only each app's Worker bundle
  differs).
- `route.app_id`: unique per app; used in `/__hyperlight/status` and in
  error messages. Config load fails (exit code 2) on a duplicate.
- `route.hostnames` / `route.path_prefix`: a request matches an app on an
  exact `Host` header match, optionally narrowed by a path prefix. The first
  full match wins; a config where two routes could match the same
  `(hostname, path)` pair fails to load (exit code 2) rather than picking
  one silently.
- `capability_policy`: reserved for future per-app capability tuning; today
  only `{}` (deny-all fetch/storage, matching every other `hluk` Workerd
  entry point) is supported.
- `pool`: either
  `{"kind": "disposable", "max_concurrent_sandboxes": N, "queue_capacity": N}`
  (unchanged disposable behavior — one VM per request), or
  `{"kind": "resident", "capacity": N, "queue_capacity": N, "max_requests_per_vm": N|null, "max_lifetime_secs": N|null}`
  (a bounded pool of resident VMs, each serving multiple requests until it
  hits `max_requests_per_vm`/`max_lifetime_secs` or errors, then is
  recycled).
- `connection_affinity`: `"none"` (default) submits every request on a
  connection to the app's shared pool, same fairness as today; `"sticky"`
  only applies to a `resident` app and reserves one resident VM for an
  inbound keep-alive connection's whole lifetime, so repeated requests on
  that connection are guaranteed to observe the same VM's state. A
  `disposable` app ignores `connection_affinity` (disposable VMs are never
  reused across requests, by design).

## Orchestrator contract

| Endpoint | Meaning |
| --- | --- |
| `GET /__hyperlight/healthz` | `200 {"status":"ok"}` as soon as the process is listening — before any app has finished initializing. Use this for a liveness probe. |
| `GET /__hyperlight/readyz` | `200 {"status":"ready"}` once every app's VM/pool is initialized; `503 {"status":"initializing"}` until then (or `503` forever if an app failed to initialize — see exit code 3 below). Use this for a readiness probe. |
| `GET /__hyperlight/status` | `200` with a JSON array, one object per app (`app_id`, `kind`, pool-specific counters such as `active`/`queued`/`retirements`/`resident_requests_served`), or `503` while initializing/failed. |

All three ignore the request's `Host` header and always close the connection
after responding (they are infrequent control-plane calls, not part of the
app-traffic keep-alive/affinity path).

Sending `SIGTERM` or `SIGINT` stops accepting new connections and waits up to
`--drain-timeout-ms` for in-flight requests to finish, then drops every app's
pool (each pool's existing, already-tested `Drop` behavior: resident pools
retire every resident VM, disposable pools stop admitting and join their
owners) before exiting.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Clean shutdown: drained every in-flight request within the timeout. |
| `1` | Drain timeout exceeded with requests still in flight, or a panic. |
| `2` | Config load or validation failure (bad JSON, duplicate app id, ambiguous route). Happens before any port is bound. |
| `3` | An app failed to initialize (bad bundle, VM restore failure). Can only be observed after the listener is already serving `/__hyperlight/healthz`, since every app's VM is built on a background thread so one slow or broken app cannot delay the others or the health endpoint. |
| `4` | Listener bind failure (`--bind`/`--admin-bind` address already in use, etc.). |

## Tests and demos

- `tests/workerd_resident.rs`, `tests/workerd_resident_pool.rs` — the
  resident VM and bounded resident pool in isolation (library-level).
- `tests/workerd_app_registry.rs` — multi-app routing and config
  validation.
- `tests/workerd_host_contract.rs` — the real `workerd-host` binary's
  health/ready/status/exit-code contract.
- `tests/workerd_http_affinity.rs` — keep-alive and `sticky` connection
  affinity against the real binary.
- `tools/hyperlight-demo --demo resident`, `--demo multi-app`,
  `--demo orchestrator-contract` — guided, narrated runs of the same
  behavior end to end (run `just setup-workerd-demo` once first).
