---
title: Headless clients and Atom
---

`Client.definition(contract, reducers)` supplies `applyEvent`, an `optimistic`
reducer for every action, and matching `Client.plugin` registrations. Use an
identity reducer when an action has no optimistic behavior. Registration is
checked before resource acquisition. Root and plugin reducers receive only their
own public snapshot slot. Optional `mergeSnapshot` enriches newer snapshots;
`mergeHistory` enriches that slot without changing the cursor or settling commands.

`Client.provide` and `providePlugin` attach scoped Layers. They retain unsatisfied
Effect requirements, acquire plugins in declaration order, and finalize them in
reverse order. UI, authentication, and projection destinations remain application
concerns.

```ts
const session =
  yield *
  Client.make(definition, {
    actorId,
    transport,
    persistence: { mode: "volatile" },
  });
const source = yield * session.open(contract.address("demo"));
yield * source.ready;
const result = yield * source.execute(contract.actions.set, { value: 7 });
```

Both construction and opening require `Scope`. Keep the session scope alive for
the authenticated actor. Close it before constructing a different actor's session.
Effect scopes own actor, source, and lease lifetimes. Each open holds a source lease;
concurrent opens share a coordinator until the
last lease closes. Closed leases cannot dispatch, restore state, or publish messages.
Closing one lease cancels its in-flight operations while other leases keep the source
connected. Journal transactions already admitted finish atomically for that coordinator;
their command identities remain available for recovery.
`source.read` and `source.changes` expose the public replica, including the optimistic
value, authoritative position, provisional cache value, pending phases, failures,
connection state, and the last operational error.

## Ordering and recovery

The client checks JSON through the contract's Schema codecs. It applies contiguous
events exactly once, validates a whole batch before committing any part, and
recovers after a gap or queue overflow. Ordinary snapshots cannot rewind live
state or replace a different authority. An explicit `Reset`, or snapshot recovery
for an advertised `ResyncRequired` generation, can install a replacement authority.
Late cache loads remain provisional and cannot overwrite live state.

`hydrate` accepts a versioned snapshot using the same monotonic rules.
`mergeObservation` and `applyHistory` never advance the cursor or confirm commands.
`messages`, `publishMessage`, and `publishPluginMessage` are ephemeral and do not
enter the journal or advance a durable position. Slow view and message observers
use bounded, sliding buffers; view updates can coalesce. Durable transport frames
instead trigger disconnection and recovery if their buffer overflows.

Commands capture their Schema-encoded payload before transport I/O. The source's
command gate serializes submissions and result lookups, including concurrent
retries. A declared rejection removes its overlay and appears in `failures`.
`dismissFailure(id)` only hides that terminal failure.

An operational `ClientError` retains `commandId` when an admitted outcome is
unknown. `retry(id)` first asks for the exact result. An unknown lookup can resend
the original envelope only within its known authority; expired results,
confirmed commands without a result, and ambiguous unbound commands remain
unresolved. It never allocates a replacement identity. A bare id may belong to any
source action, so retry exposes the union of their result and rejection types;
`execute` retains the chosen action's exact types.

An event confirms only the authenticated `(actorId, commandId)` pair. Confirmation
removes the overlay while retaining `ConfirmedAwaitingResult` journal evidence.
A success received before its events retains its overlay until a correlated event
arrives or the authoritative cursor covers the accepted position. On authority replacement, unresolved old
commands remain visible as `AuthorityChanged` and require application reconciliation.

Defaults bound the runtime to 32 retained sources, 100 pending commands per source,
256 buffered frames, and eight consecutive connection attempts. Reconnection uses
250 ms exponential backoff capped at 30 seconds. Operations time out after ten
seconds. Independent 30-second snapshot probes detect missed durable progress;
automatic result reconciliation has a per-command attempt bound. Applications can
configure `limits` and `retry`; `recover` explicitly restarts a parked source and
its recovery budget. Applying a valid durable subscription frame resets the reconnect
budget and backoff; snapshot probes and ephemeral messages do not. Authentication
failures park instead of reconnecting forever.
Inactive sources without unresolved work may be evicted to admit a new source.

## Transport and persistence

`Client.Transport<R>` is the portable JSON transport port. Its snapshot, execute,
result and publish operations return typed Effects; subscription returns a Stream.
Transport implementations must include bootstrap/replay in subscription and scope
their resources to stream consumption. The subscription supplies initial authority;
snapshot requests serve recovery and periodic probes. Runtime construction captures their Effect
requirements. The client performs its own Schema decoding and address checks.

`Client.rpcTransport(contract)` implements the port through native Effect RPC and
requires `RpcClient.Protocol` and `Scope`. The application supplies its socket
protocol, JSON serialization, route, and credentials. Build those Layers in the
session scope, or provide them around the whole session lifetime. A protocol bound
to one route serves that route's source address; applications hosting several
addresses can implement a routing transport. Core imports no browser, Node, Expo,
or Cloudflare implementation.

Persistence must be explicit: `{ mode: "volatile" }` or
`{ mode: "persistent", storage }`. Failure never changes that selection.
`ReplicaPersistence` is the actor-scoped service; its handle captures a generation
and exposes a disposable snapshot cache, transactional intent journal,
`purgeSource`, and `wipe`. Journal admission commits before transport. Storage or
capacity failure refuses new work, and unresolved evidence is never evicted.

Journal rows retain an address, reserved command id, and Schema-encoded `Intent`
format 1. That record contains the immutable command, local order, authority
binding, confirmation position, and any known outcome. Unrecognized formats,
source versions, codecs or inconsistent evidence are quarantined and continue
reserving their identity and capacity. Cache corruption discards only the cache.
Snapshot saves coalesce for 100 ms; `session.flush` reports save failure explicitly.
The journal's first write never depends on a background save or unload callback.

`memory().open(actorId)` supplies bounded process-local storage. Reuse a memory
factory to retain records across scopes. `ReplicaPersistence.layerMemory({ actorId })`
is a convenience Layer. A wipe invalidates old handles; reopen the factory to
capture the new generation. The memory adapter is not restart durability.

`IndexedDb.open({ namespace, actorId })` and `ExpoSqlite.open({ namespace, actorId })`
acquire durable storage in an Effect scope. Their `layer(options)` constructors
provide `ReplicaPersistence`. Durable handles add bounded oldest-write-first
`snapshotCache.scan(limit)` metadata. Configure `maxJournalRows`, `maxSnapshots`
and `maxBytes` explicitly when the defaults do not fit the application.

Adapters use separate cache/journal databases and atomic generation/revision checks.
A concurrent journal commit returns `Conflict` without replaying the callback.
Journal formats fail closed and retain evidence. Each actor has one bounded,
generation-tagged cache record; cache format 2 resets known format-1 cache data
without touching the journal. Malformed cache records can be replaced.
Applications own background lifecycle hooks and can await `session.flush`;
they need no unload write for initial journal admission. Session disposal retains
journal evidence, and logout deletion remains an explicit application policy.

See the [IndexedDB](https://github.com/yielded-dev/sync/blob/main/packages/local-indexeddb/README.md) and
[Expo SQLite](https://github.com/yielded-dev/sync/blob/main/packages/local-expo/README.md)
adapter guides for physical names,
format versions, reset scope and platform proof. There is no automatic migration
from application-owned databases or namespaces. The portable `Persistence` helper
is available for adapter authors implementing atomic string-record stores; custom
consumers may continue implementing the smaller `PersistenceHandle` directly.

## Atom bindings

`SourceAtom.make(session)` creates stable address-keyed bindings over this runtime.
`replica(address)` acquires a lease and returns an `AsyncResult` replica atom.
`passiveReplica(address)` returns an `AsyncResult` without acquiring a lease;
`status(address)` is also passive. Registries observe the same replica even when
several registries hold leases. Registry unmount/disposal releases its leases.

`execute`, `retry`, and `recover` return Effects over the same client. They acquire
a temporary lease for the operation and wait for authority. React only reads atoms
and dispatches these Effects through the application's runtime; it owns no socket,
retry loop, cursor or second replica store.
