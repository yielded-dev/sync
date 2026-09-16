# @yielded/sync-platform-cloudflare

Cloudflare hosting and authoritative persistence for Effect Sync.

`Cloudflare.durableObject(server, options)` hosts a public source in a SQLite-backed
Durable Object. `Cloudflare.worker(contract, options)` mounts its authenticated
gateway. See the [counter example](../../examples/cloudflare/README.md).

The host speaks the source's native Effect RPC protocol with `RpcSerialization.layerJson`.
HTTP POST serves snapshot, execute, and result lookup. WebSocket RPC also supports
subscribe and connection-scoped publishMessage. Each socket has one subscription;
its request id, acknowledgement count, source address, and authenticated session
survive hibernation. Slow consumers close with code 1013 and recover by cursor.
Session expiry or revoked authorization closes with code 4403.

The gateway authenticates every mount and replaces its internal identity headers.
The Durable Object binding is a trusted capability: expose it through this gateway,
and apply equivalent authentication to any additional route or service binding.
Application services and plugin Layers live in each platform event's scope and
finalize when that event finishes. Sockets retain Schema-encoded metadata, never
live service handles. A bounded source gate serializes bootstrap, commits and
publication; network delivery runs outside that gate and SQLite transactions.

SQLite format 1 uses `sync_metadata`, `sync_head`, `sync_events`, `sync_receipts`,
and `sync_outbox`. One object owns one source address and storage namespace.
Namespace, source schema-version, and format mismatches refuse access;
they never reset authoritative data. There is no implicit migration from another
application's tables. Consumers migrating existing authorities must explicitly
migrate private state and retain receipts and pending delivery evidence.

Event retention removes old replay rows only. Receipts are not pruned. Commands
admitted without an authority require `allowUnboundCommands: true`; their identities
remain protected across authority generations. Outbox ids include authority,
actor, command and obligation index. Claims have durable leases and attempt fencing;
only `Delivered` removes an obligation. Permanent failures remain available in
`sync_outbox` with a reason. Retry and indeterminate dispositions retain evidence.
A durable alarm is armed before accepting work and before external delivery.
Sources without an outbox delivery configuration refuse plans containing outbox work.

`SqliteStorage.layer` exposes the same storage port for custom hosts. Operations use
the released Effect SQLite driver and `effect-cf` scheduler integration. Workerd
tests cover rollback on interruption and injected failure, exact results after
eviction, stable outbox claims, replay retention, and native RPC hibernation.
They exercise the local Cloudflare runtime; no remote deployment is required by checks.
