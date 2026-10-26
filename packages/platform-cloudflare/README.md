# @yielded/sync-platform-cloudflare

Cloudflare request, WebSocket, and alarm integration around Sync's portable
`Server.host`, with SQLite Durable Object storage by default.
Use [effect-cf](../platform-effect-cf/README.md) or
[Alchemy](../platform-alchemy-cf/README.md) for framework integration.

`Cloudflare.make(server, state, options)` acquires platform services and adapts
one source per object. It returns fetch and WebSocket lifecycle handlers plus an
`alarm` Effect. Acquire it in the native object's scope. Run the same alarm
Effect for each native notification; framework
adapters supply the callback. For other environments, use `Server.host` and
`ServerRpc` directly from `@yielded/sync/server`.

`Cloudflare.layer(nativeStorage, options)` exposes the shared `SourceStorage` and
`ServerCrypto` defaults for custom composition. Set `storage` to SQLite options
or a replacement `SourceStorage` Layer; a replacement prevents SQLite from being
opened. `crypto` accepts a replacement `ServerCrypto` Layer.
Custom stores implement the
[transaction contract](../sync/src/server/SourceStorage.ts); durable delivery adds
the optional `outbox` capability.

| Option                                 | Default                                     |
| -------------------------------------- | ------------------------------------------- |
| `storage.namespace`                    | `sync`                                      |
| `storage.replayWindow`                 | `replay.maxEvents`, or 512                  |
| `storage.maxPendingOutbox`             | 1,000 with delivery configured; otherwise 0 |
| `replay`                               | 512 events / 600,000 bytes                  |
| `sockets`                              | 100 connections / 256 queued frames         |
| `maxQueuedTurns` / `turnTimeoutMillis` | 128 / 5,000 ms                              |
| `outbox`                               | Disabled                                    |

Use `SqliteStorage.layer(nativeStorage, options)` when only the storage service
is needed. Its required namespace and replay window are explicit.

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

The adapter connects the source's Effect RPC group through `ServerRpc`. The core
host owns ordering, authorization, publication, and connection limits; the
adapter owns native request decoding, socket closure, and alarm scheduling.
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

Outbox delivery is optional. Supply `outbox: { batchSize, retryMillis, deliver }`;
the default SQLite store enables bounded capacity automatically. Override it with
`storage.maxPendingOutbox`. A custom store must supply its own outbox capability.
Without a storage outbox capability,
nonempty outbox plans fail before commit. Host acquisition requires delivery
configuration and the storage capability to be enabled together.

Delivery is at least once. Destinations must deduplicate by the stable obligation
id. Only `Delivered` removes an obligation; permanent, retryable, and indeterminate
failures retain evidence. Drain pending delivery before disabling it. Disabling
SQLite outbox support retains its records. Sync owns the object's alarm when
delivery is enabled, so unrelated alarm jobs need an application scheduler.

Storage schemas and transaction details live in
[SqliteStorage.ts](src/SqliteStorage.ts). The [portable host](../sync/src/server/Host.ts)
owns synchronization options; `Cloudflare.Options` maps `sockets` to its
connection limits and supplies durable alarm scheduling for outbox delivery.
