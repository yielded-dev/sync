---
title: Getting started
---

Yielded Sync separates a source's public contract from its authoritative host and
its clients. Install the core package and its Effect peer:

```sh
bun add @yielded/sync@{{SYNC_VERSION}} effect
```

For Cloudflare hosting, choose one framework adapter:

```sh
bun add @yielded/sync-platform-effect-cf@{{SYNC_VERSION}} effect-cf
# Or:
bun add @yielded/sync-platform-alchemy-cf@{{SYNC_VERSION}} alchemy
```

For durable client storage, choose the adapter for your platform:

```sh
bun add @yielded/sync-local-indexeddb@{{SYNC_VERSION}}
# Or, for native Expo:
bun add @yielded/sync-local-expo@{{SYNC_VERSION}} expo-sqlite
```

Keep Sync packages on the same exact version and satisfy the peer ranges in
their manifests. See [packages and hosts](../reference/packages.md) for platform
constraints. Expo web uses IndexedDB.

## Define a shared contract

`Source.make` defines a public snapshot, durable event, ephemeral message, and
registered actions. Effect Schema owns values that cross the wire or are persisted.

```ts
import { Action, Source } from "@yielded/sync";
import { Schema } from "effect";

export const Counter = Source.make({
  kind: "counter",
  schemaVersion: 1,
  snapshot: Schema.Struct({ value: Schema.Number }),
  event: Schema.Struct({ value: Schema.Number }),
  message: Schema.Never,
  actions: [
    Action.make("set", {
      payload: Schema.Struct({ value: Schema.Number }),
      success: Schema.Struct({
        previous: Schema.Number,
        current: Schema.Number,
      }),
      error: Schema.Never,
    }),
  ],
  plugins: [],
});
```

The root import is portable between server and client. Server implementations
come from `@yielded/sync/server`; headless clients come from
`@yielded/sync/client`. The [consumer example](../api/consumer.md) shows the
matching server definition and links to runnable applications.

## Connect server and client

Use `Server.make` to supply private state, action handlers, authorization, and
initialization. An accepted action commits state, events, its exact outcome, and
outbox obligations atomically. The [counter example](../api/consumer.md) explains
the server and host boundaries.

On the client, `Client.definition` supplies pure event and optimistic reducers.
`Client.make` opens a session for one authenticated actor, with an application
transport and explicit persistence. Open a source address, wait for `ready`, then
execute a registered action. `SourceAtom.make` adds UI bindings over that session.

Use `{ mode: "volatile" }` for disposable client state or `{ mode: "persistent" }`
with a `ReplicaPersistence` Layer. Persistent journals retain unresolved command
identities across restarts; snapshot caches can be rebuilt. The
[client guide](../client.md) covers session lifetime and safe retry.
