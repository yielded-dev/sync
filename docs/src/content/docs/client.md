---
title: Headless clients and Atom
---

Create one client session per authenticated actor. It owns the source connections,
optimistic state, and recovery; UI bindings read that same state.

## Open a source

`Client.definition` supplies a pure event reducer for the shared contract.
Add optimistic reducers only for actions that need immediate local feedback;
other actions leave the view unchanged until authority arrives. Register matching `Client.plugin` definitions for
declared plugins, or omit `plugins` when there are none. Each reducer receives
only its own snapshot slot.

Using the `Counter` from [getting started](./guide/getting-started.md):

```ts
import { Client } from "@yielded/sync/client";
import { Effect } from "effect";

import { Counter } from "./counter";

const definition = Client.definition(Counter, {
  applyEvent: (_snapshot, event) => ({ value: event.value }),
});

export const program = Effect.gen(function* () {
  const session = yield* Client.make(definition, { persistence: { mode: "volatile" } });
  const lease = yield* session.open(Counter.address("demo"));
  const counter = Client.view(lease);

  yield* counter.ready;
  yield* lease.execute(Counter.actions.set, { value: 7 });
  return (yield* counter.read).value;
});
```

Provide `Client.CurrentActor`, `Client.Transport`, and a `Scope` around the whole
workflow. `CurrentActor` contains `{ actorId }`; it selects local ownership and
does not authenticate the user. Close the old session before opening one for a
different actor. Do not return a live client from an already-completed scope.

Each `open` holds a scoped lease. Concurrent leases share one coordinator; closing
the last disconnects it. Closing a lease cancels its in-flight operations while
retaining admitted commands for recovery.

`Client.view(lease)` exposes root data through `read`, `changes`, and `ready`.
Here, `counter.read` returns a replica whose `value` is `{ value: number }` or
`undefined`. Its authoritative and provisional snapshots use the same shape,
even when plugins are added. `Client.view(lease, id)` selects a declared plugin
with its inferred snapshot type. All views share the lease's lifetime, connection
state, pending commands, and failures. Commands still run through the lease.

Use `lease.read`, `lease.changes`, and `lease.ready` when you need the complete
`{ source, plugins }` snapshot. Views do not change wire or persisted data.

## Retry and recovery

`execute` captures the payload and journals the command before sending it.
For local-first interactions, `enqueue` returns the command id after journal
admission and delivers it in the background. Observe `pending`, `failures`, and
`error` on the replica for delivery progress; a returned id is not server acceptance. A
declared rejection removes its optimistic overlay and appears in `failures`;
`dismissFailure(id)` hides that terminal failure.

When an admitted outcome is unknown, `ClientError` retains its `commandId`.
Call `retry(id)` to look up the result and, when safe, resend the original command.
It accepts no replacement payload and never allocates a new identity. Because an
id may belong to any action, retry returns the union of action result/error types;
`execute` retains the selected action's exact types.

An event can confirm the edit before its return value arrives. The overlay then
clears, but `ConfirmedAwaitingResult` stays pending until the result is known.
Expired results and ambiguous commands remain unresolved. An authority change
marks old work `AuthorityChanged`; the application must reconcile it before
reissuing edits.

The client applies ordered events and recovers after gaps or buffer overflow.
Cached snapshots remain provisional and cannot overwrite live state. A restored cache retains its authority identity
for offline command admission without treating its contents as fresh. Connection
retry exhaustion does not prevent these edits; authorization, storage, and
protocol failures still refuse new work. Reconnect with `recover` to resume
delivery. Commands remain bound to their original authority after recovery.

`hydrate` accepts a snapshot frame under the same authority and ordering rules.
Keep application history and other projections outside the replica. Ephemeral `messages`, `publishMessage`, and
`publishPluginMessage` never enter the durable journal. Slow observers may receive
coalesced view updates or miss ephemeral messages.

Configure bounds through `Client.make` options:

| Option                                  | Default                                                        |
| --------------------------------------- | -------------------------------------------------------------- |
| `limits.maxSources`                     | 32 retained sources                                            |
| `limits.maxPendingPerSource`            | 100 commands                                                   |
| `limits.frameBuffer`                    | 256 frames                                                     |
| `limits.operationTimeout`               | 10 seconds                                                     |
| `limits.heartbeatInterval`              | 30 seconds between snapshot probes                             |
| `retry.maxAttempts`                     | 8 consecutive connection attempts; also bounds result recovery |
| `retry.initialDelay` / `retry.maxDelay` | 250 ms exponential backoff, capped at 30 seconds               |

When supplying `limits` or `retry`, include its required fields. Authentication
failures or exhausted connection attempts park the source. `recover` restarts it
and resets result-recovery attempts. Sources with unresolved work are retained
when inactive.

## Transport

`Client.layerWebSocketTransport(contract, { url })` provides `Client.Transport`
with JSON RPC. Supply `Socket.WebSocketConstructor`, such as
`Socket.layerWebSocketConstructorGlobal` in a browser. The application owns the
authenticated URL and session lifetime. A route serving one address needs one
transport; multi-address applications need a routing transport.

Use `Client.layerRpcTransport(contract)` with your own `RpcClient.Protocol` for
other protocol composition, or scoped `Client.rpcTransport` to decorate it.

Custom transports provide typed Effects for snapshot, execute, result, and publish,
and a scoped Stream for subscription. Subscription must include bootstrap/replay.
Capture transport dependencies when constructing its Layer so operations have no
remaining service requirements. Core imports no platform transport implementation.

## Persistence

Choose `{ mode: "volatile" }` or `{ mode: "persistent" }` explicitly. Persistent
mode requires `ReplicaPersistence`; failures never silently switch to volatile
mode. Provide the actor once around the client, transport, and persistence Layers.
Core requires only an actor-bound journal; a snapshot cache is optional.

For your own persistence, provide a `ReplicaPersistence` service with `actorId`
and `intentJournal`. Its `load(address, limit)` reads retained rows and
`commit(address, { insert, replace, remove })` applies one atomic batch. Inserts
must reserve unused identities; replacements require existing rows. Fence old
handles after an actor switch or destructive reset, and fail without partial
writes. Add `snapshotCache` with `get`, `put`, and `remove` only when needed.
The [port types](https://github.com/yielded-dev/sync/blob/main/packages/sync/src/client/ReplicaPersistence.ts)
define the full contract.

Use [IndexedDB](https://github.com/yielded-dev/sync/blob/main/packages/local-indexeddb/README.md)
in browsers or [Expo SQLite](https://github.com/yielded-dev/sync/blob/main/packages/local-expo/README.md)
on native Expo. The optional `@yielded/sync-persistence` package exports
`layerMemory()` for process-local storage and `Persistence.make` for shared
durable-record policy. Reuse a `memory()` factory to retain its records across
scopes. Core does not require these implementations or their storage formats.

Snapshot caches are disposable. Journals retain exact unresolved commands; storage
or capacity failure refuses new work instead of evicting them. Unsupported journal
entries remain quarantined and reserve their identities and capacity. `retry`
returns `Quarantined` for these entries; keep them for explicit recovery.

Snapshot saves coalesce. Cache failures appear in `replica.cacheError` and do not
park synchronization or prevent journal admission. Await `session.flush` when the
application backgrounds to report pending save failures. Initial journal admission is already durable and
does not depend on an unload callback. Session disposal retains stored data.
Concrete persistence handles expose `purgeSource` and `wipe` for explicit deletion; a wipe
invalidates old handles, so reopen afterward. Reconcile unresolved work before
wiping data or changing namespaces.

## Atom bindings

`SourceAtom.make(session)` binds UI state to the same headless client:

| Binding                       | Behavior                                                             |
| ----------------------------- | -------------------------------------------------------------------- |
| `replica(address)`            | Acquires a source lease and exposes an `AsyncResult` replica         |
| `passiveReplica(address)`     | Reads an `AsyncResult` without connecting                            |
| `status(address)`             | Reads connection status without connecting                           |
| `execute`, `retry`, `recover` | Return Effects that acquire a temporary lease and wait for authority |

Registry disposal releases its leases. React reads atoms and dispatches Effects
through the application's runtime; it needs no second replica store or retry loop.
The [React board](https://github.com/yielded-dev/sync/tree/main/examples/list-board)
shows `RegistryProvider`, an actor-scoped `Atom.runtime`, IndexedDB, and presence.
