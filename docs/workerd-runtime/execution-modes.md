# Execution modes and native invocations

[Canonical demo runbook](../azure-workerd-hyperlight-runbook.md)


For a conventional bounded resident pool, replace `pool` with:

```json
{
  "kind": "resident",
  "capacity": 2,
  "queue_capacity": 8,
  "max_requests_per_vm": null,
  "max_lifetime_secs": null
}
```

Fetch, scheduled and queue invocations use the same bounded admission in both
modes. Invocation style is not an app eligibility restriction. Queue wait
and restore time consume the admitted lifetime budget, rather than creating
a fresh handler deadline. A queue response preserves native acknowledgement
and retry decisions; it is not a promise of external exactly-once delivery.

The management invocation endpoint accepts the contract's tagged envelope:

```bash
curl --fail http://127.0.0.1:8081/v1/apps/hello/invoke \
  -H 'Content-Type: application/json' --data-binary \
  '{"protocol_version":1,"kind":"scheduled","request":{"protocol_version":1,"request_id":"schedule-1","scheduled_time_unix_ms":1767225600000,"cron":"0 0 * * *"}}'
```

The response is `{"protocol_version":1,"kind":"scheduled","response":{...}}`.
Fetch and queue use the corresponding unchanged v1 request/result envelope.
Outer HTTP 200 is invocation-transport success, not necessarily a Worker
HTTP `response.status` of 200. The operator control body can carry one bounded
60 KiB envelope; this does not raise the guest's buffered 32 KiB body limit.

## Switch modes without pretending to migrate heaps

There is no hot-reload or automatic adaptive mode switch. Select a mode in
an immutable app/revision home configuration and roll out that configuration.
The same package can use either mode; scheduled and queue apps are not
restricted to disposable VMs.

| Transition | What is retained | What is deliberately not retained |
|---|---|---|
| Request -> resident | Same verified package and admitted external service data | Independent heaps from past request VMs cannot be merged |
| Resident -> request | Same verified package and admitted external service data | The resident heap is not cloned into every disposable invocation |
| Rollback to resident | The prior revision/config and explicitly parked checkpoints | No silent checkpoint fallback or speculative heap conversion |

Before changing routing, stop **new** admission in the external operator
gateway. Finish admitted handlers and tracked `waitUntil`/event work. An open
SSE/WebSocket lifetime is active work, not idle. Allow clients/application
logic to close it, or deliberately cancel it under the admitted deadline.
A drain timeout is a failed transition, not a successful scale-to-zero claim.

For a standalone process replacement on the same fixed ports:

```bash
# Request-mode host; record only this process's PID.
target/release/hluk workerd-host --config app-request.json \
  --bind 127.0.0.1:8080 --admin-bind 127.0.0.1:8081 \
  --drain-timeout-ms 30000 &
old_pid=$!

# After the operator gateway stops new traffic:
kill -TERM "$old_pid"
if ! wait "$old_pid"; then
  printf 'Mode change blocked: old host did not drain cleanly.\n' >&2
  exit 1
fi

# Same immutable bundle and external bindings; resident pool configuration.
target/release/hluk workerd-host --config app-resident.json \
  --bind 127.0.0.1:8080 --admin-bind 127.0.0.1:8081 \
  --drain-timeout-ms 30000
```

Prepare and validate both configs with `workerd-identity --config` first.
Compare actual bundle/policy identities, then poll the new `readyz` and its
loaded revision before reopening the gateway. This simple standalone recipe
has a deliberate availability gap; staged home rollout/routing is a platform
operation, not a hidden standalone feature.

For resident -> request, first decide the heap disposition. To retain a
specific resident instance for rollback, explicitly `park` its current
generation through its configured durable instance home, record the committed
checkpoint identity, and confirm `live_vms == 0`. Preserve the authoritative
volume/key and prior app/revision policy. Start the request-mode host from the
**initialized template**, not the changed checkpoint.

Do not delete external SQL/KV/provider data on either transition. Keep the
same explicitly authorized references, or make a separately reviewed schema
migration. Both old and new code may temporarily observe the same external
data during a platform rollout; application-level compatibility still matters.

Rollback closes admission to the failed new revision, drains/terminates it
with confirmed ownership, and restarts the prior config. Resume a saved
resident only with its authoritative generation/checkpoint and compatible
artifacts/policy; a missing or corrupt checkpoint fails explicitly. If state
was deliberately discarded rather than parked, rollback starts fresh heap
state and must be described that way.
