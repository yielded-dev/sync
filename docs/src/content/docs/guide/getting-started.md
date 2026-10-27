---
title: Getting started
---

```sh
bun add @yielded/sync
```

## Define a source

Share this counter's data and actions between the server and client:

```ts title="counter.ts"
import { Action, Source } from "@yielded/sync";
import { Schema } from "effect";

export const CounterId = Schema.NonEmptyString.pipe(Schema.brand("CounterId"));

export const Counter = Source.make({
  kind: "counter",
  id: CounterId,
  schemaVersion: 1,
  snapshot: Schema.Struct({ value: Schema.Finite }),
  event: Schema.Struct({ value: Schema.Finite }),
  actions: [
    Action.make("set", {
      payload: Schema.Struct({ value: Schema.Finite }),
      success: Schema.Struct({ previous: Schema.Finite, current: Schema.Finite }),
      error: Schema.Never,
    }),
  ],
});
```

## Connect a server

Define what the `set` action does:

```ts title="counter-server.ts"
import { Server } from "@yielded/sync/server";
import { Effect } from "effect";

import { authorize, Principal } from "./auth";
import { Counter } from "./counter";

export const CounterServer = Server.make(Counter, {
  principal: Principal,
  state: Counter.spec.snapshot,
  initialize: Effect.succeed({ value: 0 }),
  snapshot: (state) => state,
  authorize,
  actions: {
    set: ({ state, payload }) =>
      Effect.succeed({
        state: payload,
        events: [payload],
        result: { previous: state.value, current: payload.value },
      }),
  },
});
```

Host it in a Cloudflare Durable Object using the
[effect-cf adapter](../reference/packages.mdx#cloudflare):

```ts title="worker.ts"
import { DurableObject } from "@yielded/sync-platform-cloudflare";
import { EffectCfServices } from "@yielded/sync-platform-effect-cf";
import { Worker } from "effect-cf";

import { authenticate } from "./auth";
import { CounterServer } from "./counter-server";

export const CounterObject = DurableObject.make(CounterServer, {
  storage: { namespace: "counter" },
}).provide(EffectCfServices);

const Counters = CounterObject.binding("COUNTERS");

export default Worker.makeFetchHandler(Counters.layer, {
  fetch: Counters.handler({ authenticate }),
});
```

Your app supplies `authenticate`, `authorize`, and `Principal` in `auth.ts`.
Add the `COUNTERS` binding and SQLite migration to your Worker config, as in the
[runnable board](https://github.com/yielded-dev/sync/blob/main/examples/list-board/wrangler.jsonc).

## Connect a client

Tell the client how to apply confirmed changes and show edits while they are pending:

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

Use the [browser adapter](../reference/packages.mdx#browser-clients) to connect
to the `demo` counter on the same origin:

```ts title="counter-atoms.ts"
import { BrowserSource } from "@yielded/sync-platform-browser";

import { CounterId } from "./counter";
import { CounterClient } from "./counter-client";

export const counter = BrowserSource.bind(CounterClient, {
  id: CounterId.make("demo"),
  actorId: "alice",
  persistence: "volatile",
});
```

Use your signed-in actor's ID. The server still authenticates the request.

```tsx title="App.tsx"
import { RegistryProvider, useAtom, useAtomValue } from "@effect/atom-react";

import { counter } from "./counter-atoms";

function CounterView() {
  const replica = useAtomValue(counter.replica);
  const [result, set] = useAtom(counter.action.set);

  if (replica._tag === "Failure") return <p role="alert">Could not load the counter.</p>;
  const value = replica._tag === "Success" ? replica.value.value?.value : undefined;

  return (
    <>
      <output>{value ?? "Connecting…"}</output>
      <button disabled={value === undefined || result.waiting} onClick={() => set({ value: 7 })}>
        Set to 7
      </button>
      {result._tag === "Failure" && <p role="alert">Could not confirm the change.</p>}
    </>
  );
}

export function App() {
  return (
    <RegistryProvider>
      <CounterView />
    </RegistryProvider>
  );
}
```

For persistence, retry controls, or a client without React, continue with the
[client guide](../client.mdx). The
[React board](https://github.com/yielded-dev/sync/tree/main/examples/list-board)
has the complete app setup.
