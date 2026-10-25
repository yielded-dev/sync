# @yielded/sync-persistence

Optional browser and native storage for Sync clients. Pending commands and
disposable snapshot caches use separate databases, partitioned by actor.

```sh
bun add @yielded/sync @yielded/sync-persistence effect
```

| Entry point                           | Export        | Use it for                                       |
| ------------------------------------- | ------------- | ------------------------------------------------ |
| `@yielded/sync-persistence/indexeddb` | `IndexedDb`   | Browsers, including Expo web                     |
| `@yielded/sync-persistence/expo`      | `ExpoSqlite`  | Native Expo with the optional `expo-sqlite` peer |
| `@yielded/sync-persistence`           | `Persistence` | Shared record policy for custom storage drivers  |

## Open a session

```ts
import { IndexedDb } from "@yielded/sync-persistence/indexeddb";

const PersistenceLive = IndexedDb.layer({ namespace: "my-app:production" });
```

Provide this Layer around a persistent client's complete session. Supply the same
`Client.CurrentActor` around the client, transport, and persistence acquisition.
See the [client guide](../../docs/src/content/docs/client.mdx#persistence) and
[board session](../../examples/list-board/src/browser/atoms.ts) for composition.

For native Expo, install `expo-sqlite`, import `ExpoSqlite` from
`@yielded/sync-persistence/expo`, and call `ExpoSqlite.layer` with the same options.
The root and IndexedDB imports do not load native Expo code. Expo also accepts
`directory`, defaulting to SQLite's application database directory.
Both adapters expose `open(options)` for direct access within an Effect scope.

Keep a session's handles alive until its work finishes; close it before switching
actors. Disposal closes database connections and retains stored data. Applications
own browser visibility and native AppState subscriptions. Await `client.flush`
when backgrounding to report pending cache-save failures; journal admission is
already durable.

## Options and recovery

| Option           | Default / meaning                                         |
| ---------------- | --------------------------------------------------------- |
| `namespace`      | Required, application/environment storage boundary        |
| `maxJournalRows` | 4,096 unresolved entries per actor                        |
| `maxSnapshots`   | 128 cached snapshots per actor                            |
| `maxBytes`       | 8 MiB per actor per store, including JSON record overhead |

Bounds must be positive integers. A full journal refuses new work without
evicting unresolved commands. Unsupported intents retain their identities and
capacity for recovery. Snapshot caches evict oldest writes first.

Corrupt cache records become misses. Cache failures appear through the client's
`cacheError` without preventing journal use. Unreadable or unsupported journals
remain intact for recovery; never delete one to repair a cache. Close sessions
and reconcile unresolved commands before deleting local data or changing namespaces.

Browser storage remains subject to quota and eviction; persistent-storage requests
belong to the application. IndexedDB version changes close affected connections;
reopen the session to reconnect. Native SQLite refuses unsupported versions and
nonempty unversioned databases without modifying them. The
[native probe](../../examples/persistence-expo/README.md) demonstrates restart recovery.

## Custom storage

Implement the core [`ReplicaPersistence` port](../sync/src/client/ReplicaPersistence.ts)
directly, or use `Persistence.make(options, { journal, cache })` with two scoped
`AtomicStore` drivers. Each supplies `read(key)` and `modify(key, callback)`.
`modify` must atomically read, apply the synchronous callback, and commit its
returned record; failure preserves the previous record. Drivers own namespace
isolation and resource cleanup. The core requires an actor-bound journal and
accepts an optional snapshot cache.
