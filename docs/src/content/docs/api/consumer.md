---
title: Counter example
---

Use the shared `Counter` from [getting started](../guide/getting-started.md).
The [React board](https://github.com/yielded-dev/sync/tree/main/examples/list-board)
provides a complete runnable app with authentication wiring and persistence.

## Implement the server

```ts title="counter-server.ts"
import { Server } from "@yielded/sync/server";
import { Effect, Schema } from "effect";

import { authorize, Principal } from "./auth";
import { Counter } from "./counter";

export const CounterServer = Server.make(Counter, {
  principal: Principal,
  state: Schema.Struct({ value: Schema.Finite, lastActorId: Schema.NullOr(Schema.String) }),
  initialize: Effect.succeed({ value: 0, lastActorId: null }),
  snapshot: (state) => ({ value: state.value }),
  authorize,
  actions: {
    set: ({ state, payload, principal }) =>
      Effect.succeed({
        state: { value: payload.value, lastActorId: principal.actorId },
        events: [{ value: payload.value }],
        result: { previous: state.value, current: payload.value },
      }),
  },
});
```

Your auth module supplies `Principal`, a Schema with an `actorId` field, and
`authorize`, which checks the principal, address, state, and operation. Deny access
with `ProtocolError`; an actor ID from a client is not proof of identity.

The private `lastActorId` stays on the server. Sync commits state, events, and the
result atomically. A state change must emit an event. Keep external side effects
in an [outbox](https://github.com/yielded-dev/sync/blob/main/packages/platform-cloudflare/README.md#durable-data),
with a destination that deduplicates deliveries.

## Host on Cloudflare

Install the [effect-cf adapter](../reference/packages.mdx#cloudflare), then define
the object and its authenticated route:

```ts title="worker.ts"
import { DurableObject } from "@yielded/sync-platform-cloudflare";
import { EffectCfServices } from "@yielded/sync-platform-effect-cf";
import { Worker } from "effect-cf";

import { authenticate } from "./auth";
import { CounterServer } from "./counter-server";

export const CounterObject = DurableObject.make(CounterServer, {
  storage: { namespace: "counter-v1" },
}).provide(EffectCfServices);

const Counters = CounterObject.binding("COUNTERS");

export default Worker.makeFetchHandler(Counters.layer, {
  fetch: Counters.handler({ authenticate }),
});
```

`authenticate` verifies the request and returns `{ actorId, principal, expiresAtMillis }`.
The adapter supplies SQLite, crypto, WebSockets, and alarms. Configure the
`COUNTERS` binding and SQLite migration as in the
[board's Wrangler config](https://github.com/yielded-dev/sync/blob/main/examples/list-board/wrangler.jsonc).
Keep object identities and storage namespaces stable when retaining data.

For Alchemy or a portable `Server.host`, see [packages and hosts](../reference/packages.mdx).

## Reduce events on the client

```ts title="counter-client.ts"
import { Client } from "@yielded/sync/client";

import { Counter } from "./counter";

export const CounterClient = Client.definition(Counter, {
  applyEvent: (_snapshot, event) => ({ value: event.value }),
  optimistic: {
    set: (_snapshot, payload) => ({ value: payload.value }),
  },
});
```

`applyEvent` applies confirmed changes. The optional `optimistic` reducer predicts
the value while an action is pending. Continue with
[React or headless clients](../client.mdx) to display and edit the counter.
