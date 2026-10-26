# Public API contracts

These boundaries apply to changes in the library and its adapters. For usage,
start with [getting started](src/content/docs/guide/getting-started.md), the
[counter example](src/content/docs/api/consumer.md), or the
[client guide](src/content/docs/client.mdx). Schemas and source interfaces own
wire formats and implementation details.

## Package boundaries

| Entry point                         | Responsibility                                                                     |
| ----------------------------------- | ---------------------------------------------------------------------------------- |
| `@yielded/sync`                     | Shared sources, actions, plugins, identities, envelopes, and derived RPC contracts |
| `@yielded/sync/server`              | Private state, portable hosting, RPC handlers, storage and outbox contracts        |
| `@yielded/sync/client`              | Scoped replicas, transport, recovery, and persistence ports                        |
| `@yielded/sync/atom`                | Atom bindings over the headless client                                             |
| `@yielded/sync-platform-cloudflare` | Native Cloudflare transport, lifecycle, scheduling, and SQLite storage             |
| `@yielded/sync-platform-effect-cf`  | Native effect-cf integration                                                       |
| `@yielded/sync-platform-alchemy-cf` | Native Alchemy integration                                                         |
| `@yielded/sync-persistence`         | Optional durable client persistence policy and driver contracts                    |
| `@yielded/sync-local-indexeddb`     | Browser client persistence                                                         |

Adapters depend inward on core. Root and client imports never expose server
implementations or platform dependencies. The Cloudflare adapters use the same
core host and do not import each other's framework.
Applications own domain models, authentication, authorization policy, routes,
bindings, projection destinations, bootstrap workflows, and UI.

Public asynchronous operations return `Effect` or `Stream`, with typed errors
and visible service requirements. Effect Schema owns transported and persisted
values. Shared codecs require no services; handlers acquire their dependencies
through scoped Layers. Composition must preserve each action's inferred payload,
result, rejection, and service types.

## Source composition

`Source.make` defines a kind, positive schema version, public snapshot, events,
messages, actions, and plugins. `address(id)` identifies one source authority.
Application entity revisions are separate from source cursors.

Optional `id` supplies a string identity schema, including a branded type; its
type-side validation applies to addresses and URL segments. The default is a
nonempty string. Optional `path` supplies a fixed absolute base path without a
trailing slash; it defaults to `/sync/<encoded kind>`. `Source.pathFor` encodes one
ID segment, and `Source.matchPath` decodes it with the same schema, returning
`Option.none` for unmatched paths. Invalid matched IDs fail with `ProtocolError`.
HTTP IDs must be valid Unicode and cannot be `.` or `..`. HTTP routing metadata
never changes source identity or storage keys.

Omit `message` when there are no ephemeral messages; it defaults to `Schema.Never`.
Omit `plugins` when a source has none. If the contract declares plugins,
`Server.make` and `Client.definition` require matching implementations.

`Action.make` correlates its payload, success, and declared rejection schemas.
`Plugin.make` defines a reusable public capability; `Server.plugin` supplies its
private implementation and `Client.plugin` its pure reducers. Composed snapshots
have `{ source, plugins }` slots. A handler or reducer owns only its slot.
Cross-slot domain transactions belong in a source capability.

Root operations use the `$source` namespace; plugin operations use the plugin id.
All share the source's authorization, cursor, and lifetime. Reject reserved or
duplicate registrations, missing or extra handlers, and unknown optimistic reducers
before resource acquisition. Literal duplicates also fail typechecking.
Acquire plugins in declaration order and finalize them in reverse order.

Contract schemas and RPC definitions live in [Source.ts](../packages/sync/src/Source.ts),
[Action.ts](../packages/sync/src/Action.ts), and [Model.ts](../packages/sync/src/Model.ts).

## Authority and exact retry

A source position combines an authority generation and a cursor. Cursors are
ordered only within one generation. Ordinary snapshots cannot rewind live state
or replace its authority; replacement requires an explicit reset or advertised
resynchronization. Events are contiguous and ordered. Ephemeral messages and
connection departures never advance durable progress.

Clients allocate a command id once and capture its encoded payload before I/O.
The server binds that identity to the authenticated actor, source authority,
action, schema version, and payload. An identical retry returns the original
outcome; conflicting reuse fails without running the handler. Result lookup is
authorized for the original actor and exact request. Never trust an actor id
from a command as authentication.

Success stores the full typed result. Declared rejection is also durable.
Authentication failure, unavailable storage, defects, interruption, and uncertain
transport delivery do not become domain rejections. `Unknown` or `Expired` lookup
results do not authorize allocating a replacement command id.

An event confirms only its authenticated `(actorId, commandId)` pair. Confirmation
can remove optimism before the result arrives, so retain `ConfirmedAwaitingResult`
evidence until lookup resolves it. An authority change parks unresolved work for
application reconciliation. Commands admitted without authority require explicit
opt-in; ambiguous submissions cannot be rebound to another generation.

Receipt retention is independent of replay retention. Replay or cache limits must
not delete retry identities. Any receipt-pruning policy needs durable expired-id
protection that prevents re-execution.

## Server and host

`Server.make` supplies private state, initialization, snapshot projection,
authorization, and handlers. Authorized viewers of a source receive the same
public snapshot. Viewer-private projections need a separate authorization boundary.
The portable host requires `SourceStorage` and `ServerCrypto`. Cloudflare
integrations supply these through a per-object platform layer, using native
SQLite by default. A custom storage layer replaces SQLite without opening it.
Durable outbox delivery is an optional storage capability;
omitting it must reject any nonempty outbox commit before state is committed.

`Server.host(definition, options)` supplies platform-independent ordering,
subscriptions, publication, and optional outbox delivery. Use one host per
storage authority. Its scope captures `SourceStorage`, `ServerCrypto`, and the
definition's services; `Server.open` acquires operation resources for each turn.
`connect({ address, session })` binds a trusted session to a scoped live connection.
Its `closed` Effect requests transport closure on failure; ordinary connection
scope disposal completes it successfully. Subscription scopes are independent
of connection scopes.

The host provides `snapshot`, `dispatch`, `lookup`, `subscribe`, `publishMessage`,
and `checkConnection`. `ServerRpc.handlers(host)` returns the source's Effect RPC
group and handler services. Applications supply `ServerRpc.CurrentConnection`
from authenticated context and choose an Effect RPC transport. Native socket,
request, and process APIs stay outside core.

Action handlers return `{ state, events, result, outbox? }`. `Server.commit` is an
optional helper that defaults `outbox` to an empty array. The runtime validates the
plan and atomically commits private state, contiguous events, the exact outcome, receipts, and outbox work
before publication or reply. Changed state must emit an event. Authorization,
receipt lookup, handler evaluation, and commit share one serialized source turn;
a storage adapter cannot introduce a check/commit race.

[SourceStorage](../packages/sync/src/server/SourceStorage.ts) is separate from
client persistence. Transactions roll back on failure or interruption, initialize
authority once, compare the current position on commit, and provide consistent
snapshots and bounded replay. Replay gaps are explicit. Transaction callbacks do
bounded source-local work; external effects belong in the outbox and are not
rerun automatically after a conflict.

Outbox delivery is at least once, with stable ids and durable claim fencing.
Applications own destination idempotency and classification of delivery results.
Only completed delivery removes an obligation; failures retain evidence. Outbox
users supply `deliver` and a durable `schedule(atMillis | undefined)` callback,
then run `host.drainOutbox` on wakeup. `batchSize` defaults to 10 records per drain
and per command; `retryMillis` defaults to 60,000 ms for claims and automatic
retries. Delivery attempts time out after half that interval. Hosts arm wakeups
before accepting retryable work. A wakeup is not proof that the destination
committed.

Cloudflare adapters bind one source to each object and adapt the core host's
connection closure and wakeup requests to native sockets and alarms. See the
[Cloudflare host API](../packages/platform-cloudflare/README.md) for shared
handlers and framework integration.

`EffectCf.make(definition, options)` returns the native Durable Object class;
its `binding(name)` supplies the typed namespace client and Worker layer.
`AlchemyCf.make(name, definition, options)` returns Alchemy's native declaration.
Both use the same Cloudflare platform defaults, with optional storage and crypto
layer overrides. Platform layers are also available for custom composition.
Storage opens only in the object runtime, never during infrastructure planning.

Generated Effect CF bindings expose `handler({ authenticate, objectName? })`;
Alchemy declarations expose a handler factory that acquires their native binding.
Both delegate to the shared Cloudflare request handler. It matches the source's
path, validates the ID, authenticates the request, replaces trusted metadata, and
forwards HTTP or WebSocket requests. Unmatched paths return 404; protocol failures
retain their HTTP status. The default object key is `<kind>:<id>`; `objectName`
preserves application-specific keys independently of public paths.

Applications supply authentication and authorization policy. Custom routes may
use `prepareRequest` directly with a trusted identity and source address. Only
authenticated boundaries may expose the binding. Check authorization for
snapshots, subscriptions, actions, results, and messages, and revalidate bound
identity and expiry after hibernation.

The instance scope owns RPC and socket resources; `Server.provide` scopes services
to source operations. Bootstrap, commits, and publication share the core host's
bounded source gate; network delivery stays outside it and storage transactions.
Lost activations reset streams so clients reconnect and replay. When outbox
delivery is enabled, Sync uses the object's alarm; other jobs require application
scheduling.

## Client and persistence

`Client.make` captures `CurrentActor`, transport, and the explicit persistence
choice for its lifetime. Concurrent scoped opens share one coordinator per address.
Closing a lease cancels its operations; closing the last releases the connection.
Close the old session before changing actors. All resources and queues are bounded.

`Client.layerWebSocketTransport(source, { origin, id, query? })` derives the URL
from the shared path and ID schema, including percent encoding and WebSocket
scheme selection. The existing `{ url }` form supports custom connection setup.
Applications supply credentials and the socket constructor.

`Client.view(lease)` exposes root snapshot data through `read`, `changes`, and
`ready`; `Client.view(lease, id)` exposes a declared plugin's data. Views share the
lease's lifetime and source-wide status, pending commands, and failures. The root
view's shape is independent of plugin count; composite snapshots retain their
`{ source, plugins }` shape.

Reducers are pure; omitted optimistic reducers leave the snapshot unchanged.
Apply complete valid event batches atomically, ignore duplicates, and recover on
gaps or overflow. Cache restores remain provisional. A successful
result keeps optimism until its events arrive or authority covers its position;
a declared rejection removes it. Dismissing a failure never deletes unresolved work.

Persistent mode requires `ReplicaPersistence` and never falls back to volatile
mode. Its required port is an actor-bound journal with `load` and atomic `commit`;
the snapshot cache is optional. Handles fence operations against actor and generation
changes; mismatched actors are rejected.
Journal admission commits before transport. Capacity failure refuses new work
instead of evicting unresolved evidence. Snapshot caches are separate and disposable.
Unsupported intents remain quarantined, reserving their exact identities and capacity.

Journal batches apply atomically with generation checks. Inserts reserve new
identities and replacements require existing rows. Conflicts are explicit.
Unreadable journals and unknown physical formats remain intact for recovery.
Cache recovery never resets journal data or permits stale-generation writes.

Cache failures appear separately from synchronization/journal failures and cannot
park an otherwise healthy replica. They remain observable through `cacheError`
and explicit `flush` failures.

The application owns namespaces, background hooks, and destructive cleanup.
Close all writers before deleting their storage. Disposal retains durable data;
`flush` reports pending save failures. Changing namespace is
not migration: reconcile old journals, receipts, and outbox evidence before
switching. Wire, source-schema, storage-format, authority, and local persistence
generations remain distinct.

[ReplicaPersistence.ts](../packages/sync/src/client/ReplicaPersistence.ts) defines
the ports. Physical formats and cache bounds belong to the optional
`@yielded/sync-persistence` package. The browser driver lives separately in
`@yielded/sync-local-indexeddb`. Applications may implement the ports directly with
their own persistence strategy.

`SourceAtom.bind(definition, { runtime, address, persistence })` binds one source
to the application's Atom runtime. It lazily acquires one client and lease per
registry, using the runtime's actor, transport, persistence, and definition
services. `replica` projects root data; `action.<name>` exposes typed writable
atoms. Declared plugins have the same bindings under `plugins.<id>`. `retry` and
`recover` use the same lease and exact command identities. Action atoms follow
Effect Atom's cancellation behavior; `actions.<name>.concurrent` opts into
concurrent writes. Application workflows can await them with `get.setResult`.

`presence(payloadAtom)` optionally publishes ephemeral heartbeats while observed
and lists other actors' latest payloads by connection. Peers expire, leave on
departure or disconnect, and are bounded independently of durable state. Its
payload and rendering remain application-owned. Registry disposal releases the
lease and all presence work.

`SourceAtom.make` binds an existing headless runtime. Active atoms hold leases;
passive reads never connect. UI code owns no second command queue, retry loop,
or replica store.
