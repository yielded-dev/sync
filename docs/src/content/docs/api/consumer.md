---
title: Counter example
---

A source has three parts: a shared contract, server handlers, and client reducers.
This example implements the `Counter` contract from
[getting started](../guide/getting-started.md). For complete applications, use the
[effect-cf counter](https://github.com/yielded-dev/sync/tree/main/examples/cloudflare),
[Alchemy counter](https://github.com/yielded-dev/sync/tree/main/examples/alchemy-cloudflare),
or [React board](https://github.com/yielded-dev/sync/tree/main/examples/list-board).

## Implement the server

`Server.make` keeps private state separate from the public snapshot. Here,
`lastActorId` is stored with the counter but is never sent to clients.

```ts
import { Server } from "@yielded/sync/server";
import { Effect, Schema } from "effect";

import { authorize, Principal } from "./auth";
import { Counter } from "./counter";

export const CounterServer = Server.make(Counter, {
  principal: Principal,
  state: Schema.Struct({ value: Schema.Number, lastActorId: Schema.NullOr(Schema.String) }),
  initialize: Effect.succeed({ value: 0, lastActorId: null }),
  snapshot: (state) => ({ value: state.value }),
  authorize,
  actions: {
    set: ({ state, payload, principal }) =>
      Effect.succeed(
        Server.commit({
          state: { value: payload.value, lastActorId: principal.actorId },
          events: [{ value: payload.value }],
          result: { previous: state.value, current: payload.value },
        }),
      ),
  },
});
```

The application supplies `Principal`, a Schema with an `actorId` field, and
`authorize`, which checks the principal, source address, state, and operation.
It returns an Effect that fails with `ProtocolError` when access is denied.
Authentication happens at the Worker boundary; an actor id supplied by a client
is not proof of identity.

`Server.commit` builds a plan. Sync validates and atomically commits the state,
events, result, and optional outbox obligations. Changed state must emit an event. Retrying
a set from 0 to 7 returns `{ previous: 0, current: 7 }` even after another command
changes the counter. Actions with a declared error Schema can return a typed
domain rejection; that rejection is also retained for exact retry.

## Host the source

Choose `EffectCf.make` or `AlchemyCf.make` from the
[framework adapters](../reference/packages.mdx) and supply a `SourceStorage` layer.
The native `sqlite` helper explicitly selects SQLite:

```ts
import { EffectCf } from "@yielded/sync-platform-effect-cf";
import { Layer } from "effect";
import { DurableObject } from "effect-cf";

const sync = EffectCf.make(CounterServer, {
  replay: { maxEvents: 512, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 100, bufferSize: 256 },
});

export const CounterObject = DurableObject.make(
  sync.layer.pipe(Layer.provide(EffectCf.sqlite({ namespace: "counter-v1", replayWindow: 512 }))),
  sync.handlers,
);
```

With Alchemy, build `AlchemyCf.sqlite` inside the returned runtime Effect using
`Layer.build`; its resources belong to the native object's scope. The
[Alchemy example](https://github.com/yielded-dev/sync/tree/main/examples/alchemy-cloudflare)
shows that composition.

The application constructs its Worker, selects the object for the source address,
and authenticates the request. Pass `{ request, address, identity }` to the
adapter's `prepareRequest`, then forward the returned request to the selected stub.

The identity includes `actorId`, a Schema JSON `principal`, and
`expiresAtMillis`. Request preparation replaces internal identity headers.
Expose the Durable Object binding only through authenticated routes.
Use `Server.provide` for application services acquired within each source
operation; supply instance services through the framework's Layer composition.

For external projections, set a positive `maxPendingOutbox` on the SQLite layer,
return outbox obligations, and configure `outbox: { batchSize, retryMillis, deliver }`
on the host. Stores without the optional outbox capability reject nonempty outbox
plans before commit. The destination must deduplicate by the stable obligation id.
Drain pending delivery before disabling it. Sync uses the object's alarm for
delivery; sharing it with other jobs requires an application scheduler.

## Connect a client

Define pure reducers with `Client.definition`: `applyEvent` reads the event's new
value, and an optional `set` optimistic reducer reads the submitted value.
Actions without an optimistic reducer wait for the authoritative event.

Create one session per authenticated actor with `Client.make`, then open
`Counter.address("demo")`, await `ready`, and call
`execute(Counter.actions.set, { value: 7 })`. See the [client guide](../client.mdx)
for transport, persistence, and Atom setup.

For server-side calls, `EffectCf.client` and `AlchemyCf.client` provide scoped
HTTP clients with `snapshot`, `execute`, and `result`. Supply the address,
authenticated identity, and an effectful `fetch` forwarding to the selected stub.
Keep the command id, admitted generation, and payload unchanged for retries and
result lookup. Live subscriptions and optional presence use WebSockets.

A reusable capability uses `Plugin.make`. Add it to the source's `plugins` list
and provide matching `Server.plugin` and `Client.plugin` definitions. Omit
`plugins` in all three constructors when the source has none. Each plugin owns
its snapshot slot and actions; all share the source's authorization, ordered
cursor, and lifetime. The runnable counter includes a label plugin, and the board
uses one for its title.
