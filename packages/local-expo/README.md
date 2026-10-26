# @yielded/sync-local-expo

Scoped SQLite persistence for Sync clients on native Expo. Use
[`@yielded/sync-local-indexeddb`](../local-indexeddb/README.md) for Expo web.

```sh
bun add @yielded/sync @yielded/sync-local-expo effect expo-sqlite
```

```ts
import { ExpoSqlite } from "@yielded/sync-local-expo";

const PersistenceLive = ExpoSqlite.layer({ namespace: "my-app:production" });
```

Provide this Layer around a persistent client's complete session. It requires
`Client.CurrentActor`; supply the same actor around the client, transport, and
persistence Layers. `ExpoSqlite.open(options)` acquires the handle directly in an
Effect scope. See the [client guide](../../docs/src/content/docs/client.mdx#persistence).

The [shared policy](../persistence/README.md#options-and-recovery) describes
capacity options and recovery. `directory` optionally selects the SQLite database
directory. `databaseNames(namespace)` returns separate journal and cache filenames.
Unsupported database versions and nonempty unversioned databases remain untouched
for explicit recovery. Never delete the journal to repair a cache.

Keep handles alive until the session finishes and close it before switching
actors. Disposal closes connections and retains stored data. The application
owns AppState subscriptions and logout deletion. Await `client.flush` when
backgrounding to report pending cache-save failures; journal admission is already
durable. The [native probe](../../examples/persistence-expo/README.md) demonstrates
restart recovery on a device.
