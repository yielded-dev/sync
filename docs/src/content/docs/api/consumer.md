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

Only the server sees `lastActorId`. Sync commits state, events, and the result
atomically, and every state change must emit an event. Put external side effects
in an [outbox](https://github.com/yielded-dev/sync/blob/main/packages/platform-cloudflare/README.md#durable-data)
and make sure the destination deduplicates deliveries.

Use the [Cloudflare setup](../guide/getting-started.md#connect-a-server) to host
this definition. For Alchemy or a portable `Server.host`, see
[packages and hosts](../reference/packages.mdx).
