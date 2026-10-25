# @yielded/sync-platform-cloudflare

Shared Cloudflare protocol and SQLite Durable Object storage for Sync.
Use [effect-cf](../platform-effect-cf/README.md) or
[Alchemy](../platform-alchemy-cf/README.md) for framework integration.

For custom hosts, `Cloudflare.make(server, state, options)` acquires one source
per object from an application-provided `SourceStorage`. It returns fetch and
WebSocket lifecycle handlers plus an `alarm` Effect. Acquire the storage layer and
handlers in the native object's scope. Run the same alarm Effect for each native
notification; framework adapters supply the callback.

`SqliteStorage.layer(nativeStorage, { namespace, replayWindow })` explicitly
selects SQLite. Custom stores need only implement the
[transaction contract](../sync/src/server/SourceStorage.ts); durable delivery adds
the optional `outbox` capability.

## Authentication and transport

Authenticate every route before preparing and forwarding its request. Request
preparation with `Cloudflare.prepareRequest` replaces internal identity headers.
The object binding is a trusted capability; additional routes and service bindings
need equivalent authentication.
Use `Server.provide` for services acquired and released within each source operation.

For authenticated server-side calls,
`Cloudflare.client(contract, { address, identity, fetch })` creates a scoped HTTP
client with `snapshot`, `execute`, and `result`. Supply an effectful `fetch` that
forwards to the selected native stub. Retain the exact command id, admitted
generation, and payload for retries and result lookup.

The transport uses the source's Effect RPC group with `RpcSerialization.layerJson`.
WebSockets support subscriptions and optional connection-scoped messages, with
one subscription per socket. Presence messages do not advance durable cursors.

| Socket close                                   | Client action                                 |
| ---------------------------------------------- | --------------------------------------------- |
| 1012: activation lost                          | Reconnect and replay from the retained cursor |
| 1013: slow consumer or interrupted publication | Reconnect and recover durable progress        |
| 4403: expired session or revoked authorization | Reauthenticate before reconnecting            |

Operations racing a reset can fail with `Unavailable`. Retain their command ids
and retry after recovery.

## Durable data

Each object owns one source address. The SQLite layer also owns its storage
namespace and replay retention. Namespace, schema, or format mismatches refuse
access without resetting data. Preserve object identity and reconcile state,
receipts, and pending delivery before changing storage. Event retention removes
replay rows only; receipts are retained for exact retry.
`allowUnboundCommands` defaults to false. Enabling it reserves those command
identities across authority generations.

Outbox delivery is optional. For SQLite, set `maxPendingOutbox` to a positive
capacity on the storage layer and supply the host's `outbox` options:
`batchSize`, `retryMillis`, and `deliver`. Without a storage outbox capability,
nonempty outbox plans fail before commit. Host acquisition requires delivery
configuration and the storage capability to be enabled together.

Delivery is at least once. Destinations must deduplicate by the stable obligation
id. Only `Delivered` removes an obligation; permanent, retryable, and indeterminate
failures retain evidence. Drain pending delivery before disabling it. Disabling
SQLite outbox support retains its records. Sync owns the object's alarm when
delivery is enabled, so unrelated alarm jobs need an application scheduler.

Storage schemas and transaction details live in
[SqliteStorage.ts](src/SqliteStorage.ts); host options live in
[SourceHub.ts](src/SourceHub.ts).
