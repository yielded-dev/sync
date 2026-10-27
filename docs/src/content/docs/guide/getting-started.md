---
title: Getting started
---

Build a counter shared by a Cloudflare Worker and a React client. The server
keeps the current value; connected clients receive changes and can send a `set`
action. For a complete, runnable application, see the
[React board](https://github.com/yielded-dev/sync/tree/main/examples/list-board).

```sh
bun add @yielded/sync effect
```

## Define a source

First, define the counter's data and actions in a contract shared by the server
and client:

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

The snapshot is what clients read, the event updates that view, and `set` is a
request to change it. The action's result is returned to the caller.

## Connect a server

On the server, define what happens when someone calls `set`:

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

The handler returns the next server state, an event for clients, and a result
for the caller. Sync commits them together before publishing the event.

Host it in a Cloudflare Durable Object using the
[effect-cf adapter](../reference/packages.mdx#cloudflare):

```sh
bun add @yielded/sync-platform-cloudflare @yielded/sync-platform-effect-cf effect-cf
```

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

Provide `authenticate`, `authorize`, and `Principal` in your app's `auth.ts`.
Then add the `COUNTERS` binding and SQLite migration to your Worker config, as in the
[runnable board](https://github.com/yielded-dev/sync/blob/main/examples/list-board/wrangler.jsonc).

## Connect a client

Tell the client how to apply confirmed changes and what to show while an edit is
pending. The optimistic reducer is optional; the event reducer keeps the local
view in step with the server:

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

```sh
bun add @yielded/sync-platform-browser @effect/atom-react react react-dom
```

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

Replace `"alice"` with your signed-in actor's ID. The server still authenticates
the request.

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

For persistence, retry controls, or a client without React, see the
[client guide](../client.mdx). The
[React board](https://github.com/yielded-dev/sync/tree/main/examples/list-board)
has the complete app setup.
