# Public API contracts

These boundaries apply to changes in the library and its adapters. For usage,
start with [getting started](src/content/docs/guide/getting-started.md), the
[counter example](src/content/docs/api/consumer.md), or the
[client guide](src/content/docs/client.md). Schemas and source interfaces own
wire formats and implementation details.

## Package boundaries

| Entry point                         | Responsibility                                                                     |
| ----------------------------------- | ---------------------------------------------------------------------------------- |
| `@yielded/sync`                     | Shared sources, actions, plugins, identities, envelopes, and derived RPC contracts |
| `@yielded/sync/server`              | Private state, authorization, execution, storage and outbox contracts              |
| `@yielded/sync/client`              | Scoped replicas, transport, recovery, and client persistence                       |
| `@yielded/sync/atom`                | Atom bindings over the headless client                                             |
| `@yielded/sync-platform-cloudflare` | Shared Cloudflare protocol and SQLite storage                                      |
| `@yielded/sync-platform-effect-cf`  | Native effect-cf integration                                                       |
| `@yielded/sync-platform-alchemy-cf` | Native Alchemy integration                                                         |
| `@yielded/sync-local-indexeddb`     | Browser client persistence                                                         |
| `@yielded/sync-local-expo`          | Native Expo client persistence                                                     |

Adapters depend inward on core. Root and client imports never expose server
implementations or platform dependencies. The two Cloudflare framework adapters
share the lower-level runtime and do not import each other's framework.
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

`Action.make` correlates its payload, success, and declared rejection schemas.
`Plugin.make` defines a reusable public capability; `Server.plugin` supplies its
private implementation and `Client.plugin` its pure reducers. Composed snapshots
have `{ source, plugins }` slots. A handler or reducer owns only its slot.
Cross-slot domain transactions belong in a source capability.

Root operations use the `$source` namespace; plugin operations use the plugin id.
All share the source's authorization, cursor, and lifetime. Reject reserved or
duplicate registrations, duplicate catalog kinds, and missing or extra handlers
and reducers before resource acquisition. Literal duplicates also fail typechecking.
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

`Server.commit` constructs a plan. The runtime validates it and atomically commits
private state, contiguous events, the exact outcome, receipts, and outbox work
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
Only completed delivery removes an obligation; failures retain evidence. Hosts
arm durable wakeups before accepting retryable work. An alarm is a wakeup, not
proof that the destination committed.

Cloudflare hosts bind one source to each object. Applications authenticate callers,
select typed stubs, and pass the request, address, and identity to `prepareRequest`.
It replaces internal identity headers. Only authenticated boundaries may expose
the binding. Check authorization for snapshots, subscriptions, actions, results,
and messages, and revalidate bound identity and expiry after hibernation.

The instance scope owns RPC and socket resources; `Server.provide` scopes services
to source operations. Bootstrap, commits, and publication share a bounded source
gate; network delivery stays outside it and SQLite transactions. Lost activations
reset streams so clients reconnect and replay. Sync owns the object's alarm;
other alarm jobs require application scheduling.

## Client and persistence

`Client.make` captures `CurrentActor`, transport, and the explicit persistence
choice for its lifetime. Concurrent scoped opens share one coordinator per address.
Closing a lease cancels its operations; closing the last releases the connection.
Close the old session before changing actors. All resources and queues are bounded.

Reducers are pure. Apply complete valid event batches atomically, ignore duplicates,
and recover on gaps or overflow. Cache restores remain provisional. History and
unversioned observations do not advance cursors or settle commands. A successful
result keeps optimism until its events arrive or authority covers its position;
a declared rejection removes it. Dismissing a failure never deletes unresolved work.

Persistent mode requires `ReplicaPersistence` and never falls back to volatile
mode. Handles capture actor and generation; mismatched actors are rejected.
Journal admission commits before transport. Capacity failure refuses new work
instead of evicting unresolved evidence. Snapshot caches are separate and disposable.
Unsupported intents remain quarantined, reserving their exact identities and capacity.

Journal callbacks run once and commit with atomic revision/generation checks.
Conflicts are explicit; callbacks are not replayed. Wipe rotates the generation
with journal deletion, invalidating old handles. Delayed cache writes and cleanup
cannot affect a newer generation. Unreadable journals and unknown physical formats
remain intact for recovery. Cache recovery never resets journal data.

The application owns namespaces, background hooks, and logout deletion. Disposal
retains durable data; `flush` reports pending save failures. Changing namespace is
not migration: reconcile old journals, receipts, and outbox evidence before
switching. Wire, source-schema, storage-format, authority, and local persistence
generations remain distinct.

[ReplicaPersistence.ts](../packages/sync/src/client/ReplicaPersistence.ts) defines
the port; [Persistence.ts](../packages/sync/src/client/Persistence.ts) defines
shared durable-record policy. Memory persistence is process-local. Durable adapters
implement the same ownership and recovery contract using their native stores.

`SourceAtom.make` uses the same runtime. Active atoms hold leases; passive reads
never connect. Registry disposal releases leases. UI code owns no second command
queue, retry loop, or replica store.
