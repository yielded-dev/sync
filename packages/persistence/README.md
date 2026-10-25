# @yielded/sync-persistence

Memory storage and shared durable-record policy for Sync clients. The core
[`ReplicaPersistence` port](../sync/src/client/ReplicaPersistence.ts) requires an
actor-bound journal with `load(address, limit)` and atomic
`commit(address, { insert, replace, remove })`; its snapshot cache is optional.
Applications can implement that port directly.

```sh
bun add @yielded/sync @yielded/sync-persistence effect
```

For an existing client `definition` and source `address`, use memory storage
around the complete workflow:

```ts
import { layerMemory } from "@yielded/sync-persistence";
import { Client } from "@yielded/sync/client";
import { Effect } from "effect";

const program = Effect.gen(function* () {
  const client = yield* Client.make(definition, {
    persistence: { mode: "persistent" },
  });
  const lease = yield* client.open(address);

  yield* lease.ready;
  return yield* lease.read;
}).pipe(Effect.scoped, Effect.provide(layerMemory()));
```

The application supplies `Client.CurrentActor` and transport around this program.
Client creation and use share one scope. Each memory layer acquisition creates
its own storage. To retain records across session scopes, create one `memory()`
factory and reuse its `open` effect. Memory storage does not survive process exit.

For durable storage, use the [IndexedDB](../local-indexeddb/README.md) or
[Expo SQLite](../local-expo/README.md) adapter. Adapter authors can use
`Persistence.make(options, { journal, cache })` with two scoped `AtomicStore`
drivers. Each driver supplies `read(key)` and `modify(key, callback)`; `modify`
must read, apply the synchronous callback, and commit its returned record
atomically. A failed update preserves the previous record. Drivers own namespace
isolation and resource cleanup; journals and disposable caches use separate stores.

| Option           | Default / meaning                                         |
| ---------------- | --------------------------------------------------------- |
| `namespace`      | Required, application/environment storage boundary        |
| `maxJournalRows` | 4,096 unresolved entries per actor                        |
| `maxSnapshots`   | 128 cached snapshots per actor                            |
| `maxBytes`       | 8 MiB per actor per store, including JSON record overhead |

`memory` and `layerMemory` accept only `maxJournalRows` and `maxSnapshots`, with
these defaults; either may be zero. Durable adapter bounds must be positive
integers. A full journal refuses new work without evicting unresolved commands.
Unsupported intent formats retain their identities and capacity for recovery.
Snapshot caches evict oldest writes first.

Built-in handles provide a cache plus `generation`, `purgeSource(address)`, and
`wipe`. These management operations are separate from the core port.
`purgeSource` deletes one source's journal and cache; `wipe` removes the actor's
journal and invalidates existing handles. Reopen after a wipe, even if cache
cleanup reports failure. Reconcile unresolved commands before deleting data or
changing namespaces. Ordinary session disposal retains stored data.

An unavailable cache leaves the journal usable. Clients expose cache failures
through `cacheError`; `flush` reports pending cache-save failures. Unreadable or
unsupported journals remain intact for explicit recovery. See the
[client guide](https://yielded.dev/sync/client/) for session and retry behavior.
