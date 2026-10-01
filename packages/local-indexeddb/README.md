# @yielded/sync-local-indexeddb

Scoped browser persistence for Effect Sync clients. Import `IndexedDb` from this
package and acquire `IndexedDb.open({ namespace })` in the actor's scope.
It requires `Client.CurrentActor`; `IndexedDb.layer(options)` provides the same
handle as `ReplicaPersistence`.

```ts
import { IndexedDb } from "@yielded/sync-local-indexeddb";
import { Client } from "@yielded/sync/client";
import { Context, Layer } from "effect";

class Session extends Context.Service<Session, Client.Runtime<typeof contract.spec>>()(
  "app/Session",
) {
  static readonly layer = Layer.effect(
    this,
    Client.make(definition, { persistence: { mode: "persistent" } }),
  );
}

const SessionLive = Session.layer.pipe(
  Layer.provide([
    IndexedDb.layer({ namespace: "my-app:production" }),
    Client.layerRpcTransport(contract),
  ]),
  Layer.provide(socketProtocol),
  Layer.provide(Layer.succeed(Client.CurrentActor, { actorId })),
);
```

The application owns `contract`, its client `definition`, `socketProtocol`,
authentication, namespace selection and logout policy. Provide `SessionLive`
around the whole workflow or to the Atom runtime. Await `client.flush` when the
application backgrounds; initial journal writes are already durable. Constructors
capture the actor once. Close the old session before building one for another actor.
Disposal closes connections and retains data. `purgeSource(address)` and `wipe`
are explicit destructive operations. Reopen after a wipe to capture its new generation.
Closing the scope or an IndexedDB version change makes that handle unusable.

`maxJournalRows` (4,096), `maxSnapshots` (128) and `maxBytes` (8 MiB per actor per
store, including JSON record overhead) bound storage. A full journal refuses work;
it never evicts unresolved evidence. Cache writes evict oldest writes first.
`snapshotCache.scan(limit)` returns bounded address, `savedAt` and UTF-8 JSON
payload `bytes` metadata in eviction order.

## Format and recovery

`databaseNames(namespace)` exposes both names:
`yielded-sync:<encodeURIComponent(namespace)>:journal` and `:cache`.
The journal has physical version 1; the cache has physical version 2. Both use a
`records` object store with Schema-encoded JSON strings keyed by JSON `[actorId]`.
One journal record contains `{ format: 1, generation, revision, rows }`; one cache
record contains `{ format: 2, generation, rows }`. Journal rows
retain their address, command id and opaque JSON value. Cache rows additionally
record save time and encoded size. The bounded journal is read and rewritten as
one actor document; this implementation favors a small, explicit atomic boundary.

Strict IndexedDB transactions serialize commits across tabs. Journal callbacks
stage changes once, then compare revision and generation atomically before commit.
Concurrent modification returns `Conflict` without replaying the callback. A wipe
clears the journal and rotates its generation in one transaction. Each actor has
one cache record carrying its generation; reads check the journal fence before and
after loading, and cache modifications compare the stored generation atomically.
Delayed writes/cleanup cannot overwrite or remove a newer generation's cache.

Opening the known physical cache version 1 upgrades it to version 2 by recreating
only its `records` object store. This discards legacy generation-keyed snapshots
without touching journal evidence. IndexedDB closes older adapter connections on
the version change. Unknown future cache versions are retained and open cacheless.

Malformed or unsupported **cache records only** are treated as misses and replaced
on subsequent saves. If the cache database cannot open, journal operations still
work, cache reads miss, and saves/flush report the cache failure. Journal corruption,
unknown record formats or unknown database versions fail closed without deleting
anything. Unknown intent versions remain available for client quarantine, retaining
command identities and capacity. Wipe commits its journal fence before cache cleanup;
a cleanup error can therefore be returned after the old handle was invalidated.

There is no automatic migration from application databases and no automatic journal
reset. Never delete the journal database as cache recovery. To change namespace or
format, reconcile/migrate old unresolved evidence first. Browser origin storage
remains subject to browser quota and eviction policy; applications may request
persistent storage through their own browser lifecycle.

`vp run test` uses real Chromium IndexedDB for transaction rollback, generation
fencing across tabs, background flush and exact retry recovery after reload. Install
its browser with `vp exec playwright install chromium` from this package.
