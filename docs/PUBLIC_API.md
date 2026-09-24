# Public API contracts

This document defines the contracts for extracting the synchronization library.
The root entry point implements `Source`, `Action`, `Plugin`, `SourceCatalog`,
shared identities, envelopes, exact outcome codecs, and derived Effect RPC contracts.
The server and Cloudflare adapter implement authoritative execution. The headless
client and Atom bindings are implemented, with a persistence port and process-local
memory adapter. Durable IndexedDB and Expo SQLite adapters implement separate
cache/journal storage and generation fencing. The packages remain private.

Read the [complete consumer sketch](api/consumer.md) alongside these decisions.
It defines a counter and a reusable label capability, server-private state,
authorization, a Cloudflare host, a persistent headless client, and Atom bindings.
The [external contracts example](../examples/contracts/README.md) now compiles the
shared composition, action types, and native RPC Effect requirements and runs a
JSON codec example. The [Cloudflare counter](../examples/cloudflare/README.md)
runs the server through public exports and is exercised by workerd tests. The
local persistence adapters provide the browser/Expo storage assembly. See
the [client runtime guide](client.md) for the implemented API.

The source review is pinned to Kommunikasie commit
[`b13ca58e`](https://github.com/reve-ai/kommunikasie/tree/b13ca58ef08e9ec608d2902f05e3bce1660b81f3):
[ownership](https://github.com/reve-ai/kommunikasie/blob/b13ca58ef08e9ec608d2902f05e3bce1660b81f3/docs/realtime-state.md),
[source definitions](https://github.com/reve-ai/kommunikasie/blob/b13ca58ef08e9ec608d2902f05e3bce1660b81f3/packages/realtime-extension/src/RealtimeExtension.ts),
and [persistence contracts](https://github.com/reve-ai/kommunikasie/blob/b13ca58ef08e9ec608d2902f05e3bce1660b81f3/packages/realtime-state/src/ReplicaPersistence.ts).

## Package boundary

| Entry point                         | Public responsibility                                                                                                                     |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `@yielded/sync`                     | `Source`, `Action`, `Plugin`, `SourceCatalog`; Schema identities, positions, envelopes, exact action outcomes, and derived RPC contracts  |
| `@yielded/sync/server`              | `Server` definitions and runtime; `SourceStorage`, authorization, commit plans, outbox delivery contracts                                 |
| `@yielded/sync/client`              | `Client` definitions and scoped headless runtime; replica transitions, transport/recovery ports, `ReplicaPersistence`, memory persistence |
| `@yielded/sync/atom`                | `SourceAtom` bindings to the same headless runtime and its lifecycle                                                                      |
| `@yielded/sync-platform-cloudflare` | `Cloudflare` Durable Object hosting, SQLite storage, sockets, alarms, migrations, request mounting                                        |
| `@yielded/sync-local-indexeddb`     | `IndexedDb.layer({ namespace, actorId })` for client persistence                                                                          |
| `@yielded/sync-local-expo`          | `ExpoSqlite.layer({ namespace, actorId })` for client persistence                                                                         |

Root imports are safe for a shared browser/server contract. Root and client never
re-export server implementations. Core uses Effect's portable services and RPC;
applications supply platform HTTP/socket layers. React is not a core dependency.
`SourceAtom` uses Effect Atom; a consuming application chooses its UI bindings.
Adapters depend inward on core and never on each other.

The repository remains `yielded-dev/sync` and the four manifests stay private
until built exports and beta publication are configured. This design adds no
compatibility re-exports under `@kommunikasie/*`.

## Source, action, and plugin composition

1. `Source.make` defines a literal `kind`, positive integer `schemaVersion`, public
   snapshot, durable event, ephemeral message, actions, and plugin registrations.
   `address(id)` yields `{ kind, id }`. An address has one authority lineage;
   application entity/history revisions never stand in for source cursors.
2. `Action.make(name, { payload, success, error })` keeps each action's input,
   result, and declared rejection correlated. Actions are registered as arrays
   so duplicate names can be detected before constructing a lookup object.
   No untyped action bag or common `unknown` result replaces this mapping.
3. Use native Effect Schema codecs, including their JSON transformations, for
   transported and persisted values. Shared codecs must require no services;
   service acquisition belongs in handlers. Rich values such as `DateTime.Utc`
   encode and decode through the same JSON codec.
4. `Plugin.make(id, spec)` supplies a reusable capability's public snapshot,
   events, messages, and actions. Server-private state and handlers are supplied
   separately with `Server.plugin`; client reducers with `Client.plugin`.
5. Composed state is `{ source: RootState, plugins: { [id]: PluginState } }`.
   Public snapshots use the same shape with each slot's **public** schema.
   Each server handler owns its slot; each client reducer receives that slot.
   The runtime replaces the changed slot and preserves the others. Cross-slot
   domain transactions belong in a source capability, not implicit plugin hooks.
6. Root frames use namespace `$source`; plugin frames use the plugin id. An event
   contains `{ namespace, payload }` inside one shared sequenced envelope.
   Actions likewise carry a namespace and local name. Local names may repeat in
   different plugins; they cannot shadow another registration in the same slot.
7. Reject empty/reserved plugin ids, duplicate plugin ids, duplicate local action
   names, duplicate source kinds in a catalog, and missing/extra server handlers
   or client reducers. Reject duplicate literal registrations at typecheck time;
   repeat validation at construction for dynamic registrations. Do not use
   last-write-wins merging. Composition has no I/O and fails before resources
   are acquired. Plugin declaration order determines lifecycle acquisition order;
   finalizers run in reverse. It never changes event order or conflict resolution.

A slot's server initializer and actions are Effects. Its client event, optimistic,
and optional snapshot/history reducers are pure functions. Optimism operates on
public state only. A plugin cannot inspect sibling private state or bypass the
source's authorization. Plugin lifetimes are scoped to the source runtime;
optional acquisition is a scoped Layer, not an unbounded callback registration.

For a handler returning `Effect<A, E, R>`, `E` must be a declared domain rejection
or an explicitly classified infrastructure error; only the former becomes a
terminal action outcome. Layer provision subtracts supplied services from `R`.
Composition unions remaining requirements and errors without widening them to
`any` or `unknown`. Schema types and literal action names are inferred from the
contract, not manually repeated generic parameters at each call site.

## Authority, wire protocol, and exact results

`SourcePosition = { sourceAuthorityGeneration, cursor }` retains the UUID authority
generation and nonnegative safe-integer cursor from the existing kernel. Ordering
is defined only within a generation. Reset/replacement is an explicit frame;
ordinary snapshots cannot rewind a replica or replace newer live state.

`source.rpc` is a native Effect `RpcGroup` with five operation families:
`snapshot`, `execute`, `result`, streaming `subscribe`, and `publishMessage`.
Each bound action exposes `execute` and `result` RPC definitions with tags
`execute/<namespace>/<action>` and `result/<namespace>/<action>`. Segments escape
`%` as `%25` and `/` as `%2F`, so arbitrary registration names cannot collide.
Separate action operations retain each action's exact payload/result/rejection
mapping in native RPC clients and handlers. Other tags are the operation name.
Each request includes source address, protocol version, and source schema version.
Host routes and Durable Object names are application configuration, not source
identity. Authentication is carried by the transport and never trusted from an
actor id in the command payload.

`source.snapshot`, `event`, and `message` are composed schemas. `source.envelope`
contains snapshot, sequenced event/batch, explicit reset, replay-gap, message, and
connection-departure schemas. Event confirmation uses an optional
`command: { actorId, commandId }` stamped by the server, so equal command ids from
different actors cannot settle one another's intents. Message and departure frames
have no source position. Convert schemas with `Schema.toCodecJson` at JSON
boundaries; derived RPC payloads and results already use that conversion.

A submitted command records `{ commandId, address, admittedGeneration, namespace,
action, schemaVersion, payload }`. The client allocates the id once, snapshots the
encoded payload before I/O, and retains that exact envelope for every retry.
Normal admission requires known authority. A definition may explicitly allow
admission before hydration; its first authoritative acceptance binds the command
to a generation. It must not be rebound to a new generation after an ambiguous
submission.

Each bound action exposes `command`, `outcome`, `lookup`, and `receipt` schemas.
The receipt contains the exact command, authenticated actor, accepted authority
generation, a lowercase SHA-256 fingerprint, and the exact typed outcome.
These are value contracts; fingerprint computation and atomic receipt persistence
belong to the authoritative runtime stage.

The server deduplication key is `(source authority generation, authenticated
actor id, command id)`. A fingerprint of the canonical Schema-encoded request
binds that key to the action, version, and payload. An identical retry returns the
stored outcome; a different fingerprint yields `CommandIdConflict` without
invoking the handler. The library owns correlation, hashing, and codec use.

```ts
type ActionOutcome<Success, Rejection> =
  | { readonly _tag: "Succeeded"; readonly result: Success; readonly position: SourcePosition }
  | { readonly _tag: "Rejected"; readonly error: Rejection };

type ResultLookup<Success, Rejection> =
  | { readonly _tag: "Found"; readonly outcome: ActionOutcome<Success, Rejection> }
  | { readonly _tag: "Unknown" }
  | { readonly _tag: "Expired" };
```

`result` is authorized for the original actor and exact request identity. Success
stores the original typed result, not just `{ duplicate, acceptedPosition }`.
Declared rejection is also durable so later retries cannot become a different
outcome. Authentication, unavailable storage, defects, interruption, or uncertain
transport delivery do not manufacture a domain rejection.

An event confirming a command may settle its optimistic overlay, but does not
supply its missing return value. Preserve `ConfirmedAwaitingResult` evidence and
query/retry the exact command until its result is known. `Unknown`/`Expired` never
mean rejected or safe to assign a new command id. On a changed authority, old work
is parked for reconciliation; do not blindly re-execute it against new state.

Receipt retention is independent of event replay retention. The first runtime
must not delete a receipt/deduplication identity merely to meet a replay or cache
budget. A future pruning policy requires an explicit recovery horizon and durable
expired-identity protection; after that horizon `Expired` must prevent re-execution.

Ephemeral messages are schema-checked, authorized and connection-scoped. They do
not enter the event log, receipt ledger, snapshot cursor, or pending-intent journal.
Dropping an ephemeral message cannot advance durable progress.

## Server state and storage

`Server.make(contract, definition)` defines private state Schema, scoped services,
initializer, public snapshot projection, authorization, and action handlers.
`Server.commit({ state, events, result, outbox })` is a **plan**, not a persistence
operation. The runtime validates and commits it before publishing or replying.

`Server.provide(definition, layer)` and `Server.providePlugin(plugin, layer)`
subtract supplied requirements and acquire services in the caller's source scope.
`Server.open(definition, address, limits)` acquires the portable runtime from
`SourceStorage` and `ServerCrypto`. Its typed `execute(session, action, command)`
and `result(session, action, command)` retain the action's outcome/lookup types.
Operational errors use `ProtocolError`; application handlers and authorization
classify infrastructure and policy failures into that channel. `dispatch` and
`lookup` are Schema-encoded seams for hosts. Snapshot, bootstrap and message
operations use the same authorization and serialized storage turn.

The initial runtime retains all receipts. `allowUnboundCommands` defaults to false;
opting in also reserves unbound identities across subsequent authority generations.
Changed private state must emit at least one durable event. Initialization, commits,
replay, source turns, socket acknowledgements, pending host turns, and outbox work
have explicit bounds. The host validates the complete plan before exposing it.

Private state never appears in shared source definitions or generated RPC results.
Authorized viewers of one source receive the same public snapshot. Viewer-private
projections remain application endpoints or separate authorization boundaries;
they must not silently share one public cursor with differing contents.

`SourceStorage` is separate from client `ReplicaPersistence`:

| Server storage operation                        | Required semantics                                                                                                                                                 |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `transaction(body)`                             | One source-scoped, serializable transaction with typed errors; rollback on failure/interruption                                                                    |
| Transaction state and receipt access            | Read consistent private state, authority, and exact outcome/fingerprint; initialize state and generation atomically once                                           |
| Transaction `commit(plan)`                      | Compare authority/current position; atomically store all changed private slots, contiguous events, exact outcome, and outbox obligations; return assigned position |
| `readSnapshot` / `readEvents({ after, limit })` | Consistent head and bounded ordered replay; signal a retained-history gap explicitly                                                                               |
| `readResult(identity)`                          | Exact durable outcome or protected unknown/expired status; never infer success from current state                                                                  |
| Outbox claim/ack/retry                          | Persist delivery status and generation-qualified stable ids; acknowledge only completed destination work                                                           |

Receipt lookup, authorization against source state, handler evaluation, and commit
share the serialized source turn. Adapters may implement this with a lock plus a
transaction, but cannot create a check/commit race. Transaction callbacks contain
bounded source-local work; external effects are represented as outbox obligations,
not run inside a transaction or automatically rerun after conflict.

The Cloudflare adapter owns atomic SQLite statements, the replay log, receipt
ledger, outbox, hibernating socket attachments, and alarm scheduling. Arm durable
retry wakeups before accepting retryable work; recovery re-scans durable obligations.
An alarm is a wakeup, not proof that a projection committed. Outbox delivery is at
least once with stable ids; applications own destination idempotency, projections,
and typed classification of transient/permanent/indeterminate results.

Authentication and authorization remain distinct. Applications authenticate the
request and supply principal/session expiry. Core checks authorization for
snapshot, subscribe, action, result lookup, and inbound messages. Source-state
checks run in the serialized source turn. After socket hibernation, revalidate the
bound identity/expiry before delivery; expired sessions close and require remount.
Policy failures park a client instead of reconnecting forever.

## Headless client, persistence, and Atom

`Client.definition` supplies root and plugin reducers, optional merge/history
policies, and explicit pre-authority admission policy. `Client.make` acquires an
actor-scoped runtime from that definition, transport, retry/buffer limits, and an
explicit persistence choice. `open(address)` is scoped and exposes the replica
stream, correlated `execute`/`retry`, `recover`, and ephemeral messaging.

There is one coordinator per address per actor runtime. Concurrent scoped opens
share it until the final lease closes. Subscriptions replay in order, ignore
duplicates, stop on gaps, and recover through an explicit snapshot/replacement.
History merges and unversioned observations do not advance cursors or settle work.
Queue overflow disconnects and recovers; heartbeats detect stale durable progress.
Retry count, backoff, queue sizes, pending work, and source leases are bounded.

`ready` waits for a live authoritative view. `execute(action, payload)` returns the action's exact success or typed rejection
plus a typed operational error retaining the command id when outcome is unknown.
`retry(commandId)` accepts no replacement payload and returns the source's union of
exact action results/rejections, since an arbitrary id does not identify an action
at typecheck time. Definite rejection removes the
optimistic overlay; an uncertain result retains retry evidence. Dismissal hides a
terminal failure from the UI without deleting unresolved work.

Persistence is required as one of `{ mode: "persistent", storage }` or
`{ mode: "volatile" }`; there is no implicit fallback. Memory storage is an explicit
adapter for tests/process-local use and does not promise restart durability.

The table describes the implemented persistence boundary. The port and adapters
are documented in the [client guide](client.md); physical formats and migration
limits are documented in each adapter README.

Durable adapters expose scoped `open({ namespace, actorId, ...limits })` and
`layer(options)` constructors. Their handles add `snapshotCache.scan(limit)`,
returning oldest-first address, save-time and encoded-size metadata. The existing
minimal handle remains valid for custom storage. Default limits are 4,096 journal
rows, 128 snapshots, and 8 MiB of encoded JSON per actor per database.

The portable `Persistence` adapter helper owns Schema records, capacity,
snapshot eviction and journal staging. `Persistence.make(options, drivers)` accepts
lazy journal and cache acquisition Effects, validates configuration before opening
either store, and keeps the journal usable if the disposable cache cannot open.
An `AtomicStore` driver supplies atomic read/modify operations over individual
string keys in a separate cache and journal database.
Journal callbacks run once against a bounded copy, then commit with an
atomic generation/revision comparison. A competing write returns `Conflict`;
callbacks are never replayed automatically. Failed or interrupted callbacks commit
nothing, and escaped transaction operations return `Conflict`. Every journal
commit, including read validation, checks the captured generation. Wipe increments
that generation atomically with clearing the journal. Each actor has one bounded,
generation-tagged cache record. Reads check the journal fence before and after
loading; writes and cleanup preserve newer generations. Delayed retired writes
cannot accumulate unreachable cache records or affect a later generation's cache.

Unknown physical or journal record formats fail closed and preserve all evidence.
Unknown intent formats remain opaque to the adapters and are quarantined by the
client. There is no automatic journal migration or reset. Journal format 1 remains
unchanged; cache format 2 resets only the known disposable cache format 1.
Databases use application-configured namespaces; migration from application-owned
databases requires explicit reconciliation before switching namespaces.

| Client persistence component | Contract                                                                                                                                                             |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ReplicaPersistence`         | Actor-scoped handle with captured persistence generation, `snapshotCache`, `intentJournal`, `purgeSource`, and explicit `wipe`                                       |
| Snapshot cache               | Separate disposable database; opaque Schema-encoded snapshots and metadata; oldest-first bounded scans and eviction; never deletes journal evidence                  |
| Intent journal               | Separate durable database; transactional get/scan/put/replace/delete and metadata; preserve exact pending and confirmed-awaiting-result envelopes                    |
| Lifecycle fence              | Wipe rotates actor generation in the journal transaction; stale handles cannot write journal evidence; stale cache rows cannot become readable in a new generation   |
| Runtime                      | Restore snapshots as provisional state; reconcile retained command identities with authority/results; bounded coalesced snapshot saves and explicit background flush |

Journal admission commits before transport I/O. Capacity/storage failure refuses
new durable work; never evict possibly committed work to make room. Cache corruption
may discard/rebuild only the cache. Version-mismatched or undecodable journal rows
stay quarantined with their command ids reserved. Reconciliation may settle one
only with a definitive result, never because of a codec mismatch. The current
client returns `Quarantined` from `retry`; a quarantine reconciliation API remains
future work.

Disposal cancels stale restore/save callbacks and releases sockets, fibers, database
handles, and Atom subscriptions. Identity change closes the old actor scope before
opening the next. Logout policy, whether to retain or wipe local data, belongs to
the application. Namespace selection is explicit per application/environment;
actor identity scopes records within it. Wipe cleanup must not delete rows written
by a later generation. Background flush reports failure and remains bounded;
unresolved intents never depend on an unload callback for their first write.

`SourceAtom.make(runtime)` exposes active replica atoms (acquire a source lease),
passive replica/status atoms (never connect), and mutation/recovery Effects over
the same client. Atom registry disposal releases its leases. React only renders
and dispatches; it owns no socket, cursor, retry loop, or second replica store.

## Existing exports and their destination

Paths below refer to the pinned source tree. Rows cover the modules re-exported
by the existing `realtime-extension` root/client/server/rpc and `realtime-state`
root barrels; they do not promise every helper a new public export.

| Existing module / export family                                              | Destination and treatment                                                                                                      |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `realtime-extension/RealtimeExtension` (`Spec`, `Contract`, `make`)          | Root `Source`; actions/plugins add correlated schemas, routing stays outside                                                   |
| `realtime-extension/catalog`                                                 | Root `SourceCatalog`; preserve registration collision checks                                                                   |
| `realtime-extension/envelope`                                                | Root Schema envelopes and derived RPC contracts; codec helpers private where possible                                          |
| `realtime-extension/protocol`, `source-command`, shared `errors`             | Root identities, fingerprints, exact outcomes; connection constants/error classification to client/server owner                |
| `realtime-extension/rpc/source-methods`, `source-checkpoint`                 | Root contract-derived RPC; generic snapshot/recovery operations retained, application checkpoint payloads stay in Kommunikasie |
| `realtime-state/model`                                                       | Shared address/position/frames to root; replica state/transitions and local evidence types to client                           |
| `realtime-state/ports`, `replica`, `ReplicaStore`                            | Client transport/store ports, pure transitions and observations; internal store helpers need not be public                     |
| `realtime-state/IntentDispatcher`, `CommandExecutionGate`, `SyncCoordinator` | Headless client execution, retry/gating and scoped synchronization                                                             |
| `realtime-state/ReplicaPersistence`, `PersistenceRuntime`                    | Client persistence contracts, memory implementation, restore/reconcile and flush scheduling                                    |
| `realtime-extension/client/definition`, `SnapshotLoadError`                  | Client reducer definitions and typed operational errors                                                                        |
| `realtime-extension/client/SourceClient`                                     | Split headless `Client` from `SourceAtom`; remove registry-global persistence configuration                                    |
| `realtime-extension/client/SourceSocketTransport`, `SourceWebSocket`         | Client portable transport/recovery; platform socket constructor supplied through Effect services                               |
| `realtime-extension/server/mount`, `source-durable-object`                   | Portable authorization/execution to server; HTTP/DO mounting and bindings to Cloudflare adapter                                |
| `realtime-extension/server/source-event-log`, `source-outbox`                | Server storage/delivery contracts; SQL implementations to Cloudflare adapter                                                   |
| `realtime-extension/server/source-socket-hub`, `SourceSocketAuthorization`   | Server subscription/auth lifecycle; socket attachments and hibernation to Cloudflare adapter                                   |
| `realtime-extension/server/alarm-jobs`, `migrations`                         | Cloudflare alarm/migration implementation; application jobs and schemas stay with the application                              |
| `realtime-extension/server/source-checkpoint`, `SourceCheckpointDelivery`    | Generic recovery seam only; product checkpoint delivery/reporting remains in Kommunikasie                                      |
| `server/initialization-failure-latch` (internal dependency)                  | Private server implementation detail, not a new consumer API                                                                   |
| `apps/web/src/data/replica-persistence.ts`                                   | IndexedDB adapter; remove fixed product database/channel names                                                                 |
| `apps/mobile/src/data/replica-persistence.ts`                                | Expo SQLite adapter; explicit namespace, scoped connections, no native imports in core                                         |

Domain models, auth providers, projection sinks/reactivity keys, application alarm
jobs, telemetry reporting policy, and UI remain in consuming applications. Extraction
must not copy them into generic packages just to make source imports compile.

## Persisted-format and migration scope

The Cloudflare adapter writes SQLite format 1 inside each configured Durable Object.
Its metadata, head, replay, receipt and outbox tables are documented in the
[adapter README](../packages/platform-cloudflare/README.md). It performs no implicit
migration from existing application authorities and resets no formats. Shared wire version,
source authority generation, private-state migration version, and local actor
persistence generation remain distinct. One must never substitute for another.

A disposable cache may be reset within its configured namespace. The server's
private state, receipt ledger, outbox, and client's unresolved journal are not
covered by that permission. A new package name or changed codec is not permission
to wipe retry evidence. Adapter implementations must document physical key/schema
versions and provide an explicit migration or quarantine path. If a new namespace
is chosen, old unresolved journals require reconciliation or migration before
migration is considered complete. Review product reset policy as part of each
consumer migration.

## Verification boundaries

Use the [testing skill](../.agents/skills/testing/SKILL.md) to choose the cheapest
sufficient proof for the requested change. Existing real-adapter and public-consumer
checks protect authority, transaction and recovery boundaries; they do not impose
new tests or a feature matrix. Run `vp run ready` before handoff and identify any
unavailable required proof.

Memory remount checks do not establish process-restart durability. Chromium checks
exercise real IndexedDB; the desktop SQLite suite does not establish the Expo
native bridge. Use the [native probe](../examples/persistence-expo/README.md) when
the change requires native or application-restart proof.

The independent list and board consumer exercises the public runtime and its
IndexedDB assembly. The slide-deck pilot maps the product boundary but has not
replaced the product source or projection destination. Validate that integration
before publishing a beta.
