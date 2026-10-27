# @yielded/sync-local-indexeddb

Scoped browser persistence for Sync clients, with separate snapshot and
pending-command databases partitioned by actor.

```sh
bun add @yielded/sync @yielded/sync-local-indexeddb effect
```

```ts
import { IndexedDb } from "@yielded/sync-local-indexeddb";

const PersistenceLive = IndexedDb.layer({ namespace: "my-app:production" });
```

Provide this Layer around a persistent client's complete session. It requires
`Client.CurrentActor`; supply the same actor around the client, transport, and
persistence Layers. `IndexedDb.open(options)` acquires the handle directly in an
Effect scope. See the [client guide](../../docs/src/content/docs/client.mdx#persistence)
and [board session](../../examples/list-board/src/browser/atoms.ts) for composition.

The [shared policy](../persistence/README.md#options-and-recovery) describes
capacity options and recovery. `databaseNames(namespace)` returns the stable
journal and cache database names. Browser storage remains subject to quota and
eviction; persistent-storage requests belong to the application.

Keep handles alive until the session finishes and close it before switching
actors. Disposal closes database connections and retains stored data. An
IndexedDB version change closes affected connections; reopen the session to
reconnect. Await `client.flush` when backgrounding to report pending cache-save
failures; journal admission is already durable. Never delete the journal to
repair a cache.
