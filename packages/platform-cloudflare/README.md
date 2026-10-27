# @yielded/sync-platform-cloudflare

Define Cloudflare Durable Objects around Sync's portable `Server.host`, with
SQLite storage, request handling, WebSockets, and alarms supplied by default.

`DurableObject.make(server, options)` creates a framework-independent definition.
At application composition, call `definition.provide(EffectCfServices)` for
[effect-cf](../platform-effect-cf/README.md), or
`definition.provide(AlchemyServices("CounterObject"))` for
[Alchemy](../platform-alchemy-cf/README.md). These return the framework's native
class or resource declaration. Use `typeof CounterObject.Type` to extract its
instance type. Storage opens only when an object activates.

For custom native integration, `Cloudflare.make(server, state, options)` acquires platform services and adapts
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
| `outbox.batchSize`                     | 10 records per drain and per command        |
| `outbox.retryMillis`                   | 60,000 ms                                   |

Use `SqliteStorage.layer(nativeStorage, options)` when only the storage service
is needed. Its required namespace and replay window are explicit.

## Authentication and transport

`Cloudflare.handler(source, { authenticate, fetch, objectName? })` returns a function
that handles a native request as an Effect. It matches the source's shared path and
ID schema, calls `authenticate(request, address)`, then forwards through
`fetch(objectName, request)`.
The default path is `/sync/<kind>/<id>` and the default object name is `<kind>:<id>`.
The source's `path` overrides the base path; `objectName` overrides storage identity
independently. Unmatched paths return 404; protocol failures become HTTP responses.

Authentication returns `actorId`, a Schema JSON `principal`, and `expiresAtMillis`,
or fails with `ProtocolError`. The handler replaces internal identity headers.
For custom routing, call `Cloudflare.prepareRequest` with an authenticated identity
before forwarding. The object binding is a trusted capability; additional routes
and service bindings need equivalent authentication.
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

Outbox delivery is optional. Supply `outbox: { deliver }`;
the default SQLite store enables bounded capacity automatically. Override it with
`storage.maxPendingOutbox`. A custom store must supply its own outbox capability.
Without a storage outbox capability,
nonempty outbox plans fail before commit. Host acquisition requires delivery
configuration and the storage capability to be enabled together.

`retryMillis` controls the claim lease and automatic retry delay. Delivery attempts
time out after half that interval. Delivery is at least once. Destinations must
deduplicate by the stable obligation id. Only `Delivered` removes an obligation;
permanent, retryable, and indeterminate
failures retain evidence. Drain pending delivery before disabling it. Disabling
SQLite outbox support retains its records. Sync owns the object's alarm when
delivery is enabled, so unrelated alarm jobs need an application scheduler.

Storage schemas and transaction details live in
[SqliteStorage.ts](src/SqliteStorage.ts). The [portable host](../sync/src/server/Host.ts)
owns synchronization options; `Cloudflare.Options` maps `sockets` to its
connection limits and supplies durable alarm scheduling for outbox delivery.
