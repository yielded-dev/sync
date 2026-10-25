# @yielded/sync-local-indexeddb

Scoped browser persistence for Sync clients, with separate IndexedDB databases
for pending commands and disposable snapshot caches.

```sh
bun add @yielded/sync @yielded/sync-local-indexeddb effect
```

```ts
import { IndexedDb } from "@yielded/sync-local-indexeddb";

const PersistenceLive = IndexedDb.layer({ namespace: "my-app:production" });
```

Provide this Layer around a persistent client's complete session, including its
use. It requires `Client.CurrentActor`; supply the same actor around client,
transport, and persistence acquisition. `IndexedDb.open(options)` acquires a
concrete handle directly in an Effect scope. See the
[client guide](../../docs/src/content/docs/client.md#persistence) and
[board session](../../examples/list-board/src/browser/atoms.ts) for composition.

Options, capacity limits, journal batching, and explicit `purgeSource`/`wipe`
management are shared with
[`@yielded/sync-persistence`](../persistence/README.md). The core journal has
`load` and atomic `commit`; these concrete handles also provide a snapshot cache
and management operations. Reopen after a wipe, including a failed cache cleanup.

Keep the handle alive for the actor's session and close that session before
switching actors. Await `client.flush` when backgrounding to report cache-save
failures; journal admission is already durable. Disposal closes connections and
retains data. IndexedDB version changes close the affected database connection;
reopen the session to reconnect.

`databaseNames(namespace)` returns the journal and cache database names. Corrupt
cache records become misses. An unavailable cache leaves the journal usable and
appears through the client's separate `cacheError` diagnostic. Unreadable or
unsupported journals remain intact for recovery. Never delete the journal to
repair a cache, and reconcile unresolved commands before changing namespaces.

Browser storage remains subject to quota and eviction. Persistent storage
requests belong to the application's browser lifecycle. Database operations live
in [IndexedDb.ts](src/IndexedDb.ts); shared record policy lives in
[Persistence.ts](../persistence/src/Persistence.ts).
