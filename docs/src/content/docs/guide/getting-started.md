---
title: Getting started
---

Yielded Sync separates a source's public contract from its authoritative host and
its clients. Install the core package and the Effect peer at the exact beta versions:

```sh
npm install @yielded/sync@{{SYNC_VERSION}} effect@4.0.0-rc.112
```

Add only the adapters your application runs:

```sh
npm install @yielded/sync-platform-cloudflare@{{SYNC_VERSION}}
npm install @yielded/sync-local-indexeddb@{{SYNC_VERSION}}
npm install @yielded/sync-local-expo@{{SYNC_VERSION}} expo-sqlite@57.0.3
```

The Expo adapter targets native SQLite. Expo web uses the IndexedDB adapter.

## Define a shared contract

`Source.make` defines a public snapshot, durable event, ephemeral message, and
registered actions. Effect Schema owns values that cross the wire or are persisted.

```ts
import { Schema } from "effect";
import { Action, Source } from "@yielded/sync";

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
matching server handlers, authorization, Cloudflare host, client reducers, and
Atom bindings.

## Host the authority

Use `Server.make` to supply private state, action handlers, authorization, and
initialization. An accepted action commits state, events, its exact outcome, and
outbox obligations atomically. For Cloudflare, add
`@yielded/sync-platform-cloudflare` to host the server in a SQLite Durable Object.
The [runnable counter](https://github.com/yielded-dev/sync/tree/main/examples/cloudflare)
and [list and board consumer](https://github.com/yielded-dev/sync/tree/main/examples/list-board)
show public package imports and host assembly.

Your application still owns authentication, domain rules, bootstrap data, and
projection destinations. Supply its auth provider at the Worker boundary.

## Open a client session

`Client.make` creates a scoped session for one authenticated actor. Give it the
shared client definition, an application transport, and an explicit persistence
choice. Open a source address, wait for authority, then execute a registered
action. Use `SourceAtom.make` when a UI needs Atom bindings over the same session.

Choose `{ mode: "volatile" }` for disposable state or
`{ mode: "persistent", storage }` with the browser or Expo adapter. Persistent
journals retain unresolved command identities across restarts; snapshot caches
can be rebuilt. The [client guide](../client.md) covers lifecycle, recovery, and
exact retry behavior.

:::caution[Beta compatibility]
Pin all four packages to the same exact beta version. Read the
[release guide](../RELEASE.md) before upgrading persisted formats or changing a
source schema.
:::
