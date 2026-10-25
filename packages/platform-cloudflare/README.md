# @yielded/sync-platform-cloudflare

Shared Cloudflare protocol and SQLite Durable Object storage for Sync.
Use [effect-cf](../platform-effect-cf/README.md) or
[Alchemy](../platform-alchemy-cf/README.md) for framework integration.

For custom hosts, `Cloudflare.make(server, state, options)` acquires one source
per object and returns fetch and WebSocket lifecycle handlers plus an `alarm`
Effect. Run the same Effect for each alarm notification; framework adapters supply
the native callback.
`Cloudflare.prepareRequest` attaches the authenticated source address and identity
to a native request. `SqliteStorage.layer(nativeStorage, options)` exposes the
storage port separately.

## Authentication and transport

Authenticate every route before preparing and forwarding its request. Request
preparation replaces internal identity headers. The object binding is a trusted
capability; additional routes and service bindings need equivalent authentication.
Use `Server.provide` for services acquired and released within each source operation.

Clients use the source's Effect RPC group with `RpcSerialization.layerJson`.
HTTP serves snapshot, execute, and result lookup. WebSockets also support
subscription and connection-scoped messages, with one subscription per socket.

| Socket close                                   | Client action                                 |
| ---------------------------------------------- | --------------------------------------------- |
| 1012: activation lost                          | Reconnect and replay from the retained cursor |
| 1013: slow consumer or interrupted publication | Reconnect and recover durable progress        |
| 4403: expired session or revoked authorization | Reauthenticate before reconnecting            |

Operations racing a reset can fail with `Unavailable`. Retain their command ids
and retry after recovery.

## Durable data

Each object owns one source address and storage namespace. Namespace, schema, or
format mismatches refuse access without resetting data. Preserve object identity
and reconcile state, receipts, and pending delivery before changing storage.
Event retention removes replay rows only; receipts are retained for exact retry.
`allowUnboundCommands` defaults to false. Enabling it reserves those command
identities across authority generations.

Outbox delivery is at least once. Destinations must deduplicate by the stable
obligation id. Only `Delivered` removes an obligation; permanent, retryable, and
indeterminate failures retain evidence. Plans containing outbox work require a
delivery configuration. Sync owns the object's alarm, so unrelated alarm jobs
need an application scheduler.

Storage schemas and transaction details live in
[SqliteStorage.ts](src/SqliteStorage.ts); host options live in
[SourceHub.ts](src/SourceHub.ts).
