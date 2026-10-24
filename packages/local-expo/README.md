# @yielded/sync-local-expo

Scoped SQLite persistence for Sync clients on native Expo. Use the
[IndexedDB adapter](../local-indexeddb/README.md) for Expo web.

```ts
import { ExpoSqlite } from "@yielded/sync-local-expo";

const PersistenceLive = ExpoSqlite.layer({ namespace: "my-app:production" });
```

Provide this Layer to a persistent client session. It requires `Client.CurrentActor`;
provide the actor once around the client, transport, and persistence Layers.
`ExpoSqlite.open(options)` acquires the handle directly in an Effect scope.
See the [client guide](../../docs/src/content/docs/client.md#persistence).

## Options and lifecycle

| Option           | Default / meaning                                         |
| ---------------- | --------------------------------------------------------- |
| `namespace`      | Required, application/environment storage boundary        |
| `directory`      | Expo's document SQLite directory                          |
| `maxJournalRows` | 4,096 unresolved entries per actor                        |
| `maxSnapshots`   | 128 cached snapshots per actor                            |
| `maxBytes`       | 8 MiB per actor per store, including JSON record overhead |

Keep the handle alive for the actor's session and close it before switching actors.
The application owns AppState subscriptions and logout policy. Await `client.flush`
when backgrounding to report cache-save failures; journal admission is already
durable. Disposal closes connections and retains data.

## Recovery

`databaseNames(namespace)` returns separate journal and cache filenames. Cache
records may be discarded and rebuilt. An unavailable cache leaves the journal
usable, but cache saves and flush report failure. Unsupported database versions,
nonempty unversioned databases, and unreadable journals remain untouched for
explicit recovery. Unknown intent formats retain their identities for quarantine.

A full journal refuses work instead of evicting unresolved commands. Concurrent
journal changes can return `Conflict` without replaying the transaction callback.
`purgeSource` and `wipe` are explicit destructive operations; reopen after a wipe,
even if it reports a cache-cleanup failure. Changing namespaces does not migrate
old data. Reconcile unresolved work before switching.

Physical database versions and connection settings live in
[ExpoSqlite.ts](src/ExpoSqlite.ts); record schemas live in
[Persistence.ts](../sync/src/client/Persistence.ts). The
[native probe](../../examples/persistence-expo/README.md) provides a repeatable
process-restart check. Android execution remains unverified.
