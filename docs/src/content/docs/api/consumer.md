---
title: Server actions
---

Using the shared `Counter` from [getting started](../guide/getting-started.md),
you can keep server state that clients never receive. This example records the
last actor to call `set`:

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

The auth module supplies `Principal`, a Schema with an `actorId` field, and
`authorize`, which checks the principal, address, state, and operation. Deny
access with `ProtocolError`; an actor ID from a client is not proof of identity.

Only the server sees `lastActorId`: `snapshot` selects the public view. Each
event updates connected clients, while the result goes to the caller. Sync
commits state, events, and the result atomically, and every state change must
emit an event. Put external side effects
in an [outbox](https://github.com/yielded-dev/sync/blob/main/packages/platform-cloudflare/README.md#durable-data)
and make sure the destination deduplicates deliveries.

For typed outbox work, share an obligation definition between the action and
its delivery callback:

```ts title="counter-outbox.ts"
import { Outbox } from "@yielded/sync/server";
import { Effect, Schema } from "effect";

import { projectCounter } from "./projection";

export const CounterChanged = Outbox.make(
  "CounterChanged",
  Schema.Struct({ value: Schema.Finite }),
);

export const deliver = Outbox.handlers([CounterChanged], {
  CounterChanged: ({ id, payload }) =>
    Effect.as(projectCounter(id, payload.value), { _tag: "Delivered" }),
});
```

In an action's `Effect.gen`, include
`outbox: [yield* Outbox.encode(CounterChanged, { value: payload.value })]` in the
commit. Configure the host with `outbox: { deliver }`; portable hosts also need
a durable `schedule` callback. The application's `projectCounter` returns an
Effect, deduplicates by `id`, and reports retryable failures as `ProtocolError`.
Its service requirements flow into the host. Return a `DeliveryDisposition` from
the handler to choose a retry time or classify a permanent failure.

Unknown obligation names and invalid stored payloads fail delivery and remain
retryable. Keep definitions compatible with queued records across deployments.
Applications can explicitly return `PermanentFailure` to retain a failed record
without retrying it. Only `Delivered` removes it.

Use the [Cloudflare setup](../guide/getting-started.md#connect-a-server) to host
this definition. For Alchemy or a portable `Server.host`, see
[packages and hosts](../reference/packages.mdx).
