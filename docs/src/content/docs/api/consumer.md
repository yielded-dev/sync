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
          outbox: [],
        }),
      ),
  },
  plugins: [],
});
```

The application supplies `Principal`, a Schema with an `actorId` field, and
`authorize`, which checks the principal, source address, state, and operation.
It returns an Effect that fails with `ProtocolError` when access is denied.
Authentication happens at the Worker boundary; an actor id supplied by a client
is not proof of identity.

`Server.commit` builds a plan. Sync validates and atomically commits the state,
events, result, and outbox obligations. Changed state must emit an event. Retrying
a set from 0 to 7 returns `{ previous: 0, current: 7 }` even after another command
changes the counter. Actions with a declared error Schema can return a typed
domain rejection; that rejection is also retained for exact retry.

## Host the source

Choose `EffectCf.make` or `AlchemyCf.make` from the
[framework adapters](../reference/packages.md). The application constructs its
Worker and Durable Object, selects the object for the source address, and
authenticates the request. Pass `{ request, address, identity }` to the adapter's
`prepareRequest`, then forward the returned request to the selected stub.

The identity includes `actorId`, a Schema JSON `principal`, and
`expiresAtMillis`. Request preparation replaces internal identity headers.
Expose the Durable Object binding only through authenticated routes.
Use `Server.provide` for application services acquired within each source
operation; supply instance services through the framework's Layer composition.

For external projections, return outbox obligations and configure a delivery
Effect on the host. The destination must deduplicate by the stable obligation id.
Sync uses the object's alarm for delivery; sharing it with other jobs requires an
application scheduler.

## Connect a client

Define pure reducers with `Client.definition`: `applyEvent` reads the event's new
value, and the `set` optimistic reducer reads the submitted value. Supply an
optimistic reducer for every action; use an identity reducer when an action
should wait for authority.

Create one session per authenticated actor with `Client.make`, then open
`Counter.address("demo")`, await `ready`, and call
`execute(Counter.actions.set, { value: 7 })`. See the [client guide](../client.md)
for transport, persistence, and Atom setup.

A reusable capability uses `Plugin.make` with matching `Server.plugin` and
`Client.plugin` definitions. Each plugin owns its snapshot slot and actions;
all share the source's authorization, ordered cursor, and lifetime. The runnable
counter includes a label plugin, and the board uses one for its title.
