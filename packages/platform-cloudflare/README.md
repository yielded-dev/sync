# @yielded/sync-platform-cloudflare

Shared Cloudflare source hosting and authoritative persistence for Effect Sync.

Use [platform-effect-cf](../platform-effect-cf/README.md) or
[platform-alchemy-cf](../platform-alchemy-cf/README.md) with your framework's native
Worker and Durable Object APIs. This package contains their shared implementation.
`Cloudflare.make(server, state, options)` acquires a SQLite-backed source once per
object and returns fetch, WebSocket lifecycle, and alarm handlers. The instance
scope owns the RPC server; each source operation acquires its own scoped runtime.
`Cloudflare.prepareRequest` validates and attaches an authenticated identity to a
native request. The application passes it to its typed stub's `fetch` method.

`RpcServer` and `RpcSerialization` own request decoding, dispatch, typed responses,
stream chunks, acknowledgements, and interruption. HTTP uses Effect RPC's HTTP
implementation. effect-cf supplies its native `DurableObjectRpcWebSocket` transport;
Alchemy uses a native socket bridge implementing `RpcServer.Protocol`.

Clients use the source's RPC group with `RpcSerialization.layerJson`.
HTTP POST serves snapshot, execute, and result lookup. WebSocket RPC also supports
subscribe and connection-scoped publishMessage. Each socket has one subscription.
Source addresses and authenticated sessions survive in socket attachments. Lost
activations reset ordinary RPC streams with code 1012; clients reconnect and replay
from their retained cursor. Slow consumers close with code 1013 and recover by cursor.
Operations racing a reset can fail with `Unavailable`; retry retained commands with
their original identity after the client reconnects.
Session expiry or revoked authorization closes with code 4403.
If a command commits but publication is interrupted or fails, the host closes the
affected connections with code 1013 so they recover from their durable positions.

The application authenticates every mount; request preparation replaces internal identity headers.
The Durable Object binding is a trusted capability: expose it through authenticated routes,
and apply equivalent authentication to any additional route or service binding.
Framework-native Layers own instance services. Use `Server.provide` to acquire and
release services within each source operation. Sockets retain Schema-encoded metadata, never
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

`SqliteStorage.layer(nativeStorage, options)` exposes the same storage port for
custom hosts. Operations use the released Effect SQLite driver and prevent
timer-backed scheduler yields while native transactions hold the input gate. Workerd
tests cover rollback on interruption and injected failure, exact results after
eviction, stable outbox claims, replay retention, and expired sessions after hibernation.
They exercise the local Cloudflare runtime; no remote deployment is required by checks.
