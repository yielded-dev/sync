# @yielded/sync-local-expo

Scoped SQLite persistence for Sync clients on native Expo. Use the
[IndexedDB adapter](../local-indexeddb/README.md) for Expo web.

```sh
bun add @yielded/sync @yielded/sync-local-expo effect expo-sqlite
```

```ts
import { ExpoSqlite } from "@yielded/sync-local-expo";

const PersistenceLive = ExpoSqlite.layer({ namespace: "my-app:production" });
```

Provide this Layer around a persistent client's complete session, including its
use. It requires `Client.CurrentActor`; supply the same actor around client,
transport, and persistence acquisition. `ExpoSqlite.open(options)` acquires a
concrete handle directly in an Effect scope. See the
[client guide](../../docs/src/content/docs/client.md#persistence).

Options, capacity limits, journal batching, and explicit `purgeSource`/`wipe`
management are shared with
[`@yielded/sync-persistence`](../persistence/README.md). Expo adds `directory`,
which defaults to Expo SQLite's `defaultDatabaseDirectory`. The core journal has
`load` and atomic `commit`; these concrete handles also provide a snapshot cache
and management operations. Reopen after a wipe, including a failed cache cleanup.

Keep the handle alive for the actor's session and close that session before
switching actors. The application owns AppState subscriptions and logout policy.
Await `client.flush` when backgrounding to report cache-save failures; journal
admission is already durable. Disposal closes connections and retains data.

`databaseNames(namespace)` returns separate journal and cache filenames. Cache
records may be discarded and rebuilt. An unavailable cache leaves the journal
usable and appears through the client's separate `cacheError` diagnostic.
Unsupported database versions, nonempty unversioned databases, and unreadable
journals remain untouched for explicit recovery. Never delete a journal to
repair a cache, and reconcile unresolved commands before changing namespaces.

Database operations live in [ExpoSqlite.ts](src/ExpoSqlite.ts); shared record
policy lives in [Persistence.ts](../persistence/src/Persistence.ts). The
[native consumer](../../examples/persistence-expo/README.md) shows scoped access
and restart recovery in an Expo app.
