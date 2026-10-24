# @yielded/sync-local-indexeddb

Scoped browser persistence for Sync clients, with separate snapshot and
pending-command stores.

```ts
import { IndexedDb } from "@yielded/sync-local-indexeddb";

const PersistenceLive = IndexedDb.layer({ namespace: "my-app:production" });
```

Provide this Layer to a persistent client session. It requires `Client.CurrentActor`;
provide the actor once around the client, transport, and persistence Layers.
`IndexedDb.open(options)` acquires the handle directly in an Effect scope.
See the [client guide](../../docs/src/content/docs/client.md#persistence) and
[board session](../../examples/list-board/src/browser/atoms.ts) for composition.

## Options and lifecycle

| Option           | Default / meaning                                         |
| ---------------- | --------------------------------------------------------- |
| `namespace`      | Required, application/environment storage boundary        |
| `maxJournalRows` | 4,096 unresolved entries per actor                        |
| `maxSnapshots`   | 128 cached snapshots per actor                            |
| `maxBytes`       | 8 MiB per actor per store, including JSON record overhead |

A full journal refuses work; it never evicts unresolved commands. Snapshot caches
evict oldest writes first. `snapshotCache.scan(limit)` returns address, `savedAt`,
and encoded `bytes` metadata in eviction order.

Keep the handle alive for the actor's session and close it before switching actors.
Await `client.flush` when backgrounding to report cache-save failures; journal
admission is already durable. Disposal retains data. Closing the scope or an
IndexedDB version change makes the handle unusable.

## Recovery

`databaseNames(namespace)` returns the journal and cache database names. Corrupt
cache records are treated as misses. An unavailable cache leaves the journal
usable, but cache saves and flush report failure. Corrupt or unsupported journals
fail closed without deletion. Unknown intent formats retain their command ids and
capacity for client quarantine.

Concurrent journal changes can return `Conflict`; transaction callbacks are not
replayed automatically. `purgeSource` and `wipe` are explicit destructive
operations. A wipe invalidates the handle before cache cleanup, so a cleanup
error can still require reopening it.

Never delete the journal as cache recovery. There is no automatic journal reset
or migration from application databases. Reconcile unresolved work before changing
namespaces. Browser storage remains subject to quota and eviction; persistent
storage requests belong to the application's browser lifecycle.

Physical database versions live in [IndexedDb.ts](src/IndexedDb.ts); shared record
schemas live in [Persistence.ts](../sync/src/client/Persistence.ts).
