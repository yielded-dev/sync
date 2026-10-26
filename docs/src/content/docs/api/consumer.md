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
Authentication happens at the application boundary; an actor id supplied by a
client is not proof of identity.

`Server.commit` builds a plan. Sync validates and atomically commits the state,
events, result, and optional outbox obligations. Changed state must emit an event.
Retrying a set from 0 to 7 returns `{ previous: 0, current: 7 }` even after another command
changes the counter. Actions with a declared error Schema can return a typed
domain rejection; that rejection is also retained for exact retry.

## Host the source

`Server.host(CounterServer)` owns ordered turns, subscriptions, publication, and
optional outbox delivery. Provide `SourceStorage`, `ServerCrypto`, and the
server definition's services in its scope. Keep one host per storage authority.
Within that scope, with an authenticated `session`:

```ts
const readCounter = Effect.gen(function* () {
  const host = yield* Server.host(CounterServer);
  const address = Counter.address("demo");
  const connection = yield* host.connect({ address, session });

  return yield* host.snapshot(connection, address);
});
```

`connect` returns a scoped connection. Keep its scope alive for the transport
connection. A failure of `connection.closed` requests transport closure; normal
scope disposal completes it successfully. Cancelling a subscription does not
close the connection. The host opens and releases definition resources for each operation;
`Server.provide` supplies services with that lifetime.

For a complete workflow with snapshot, subscription, dispatch, and exact retry,
run the [core server example](https://github.com/yielded-dev/sync/tree/main/examples/contracts):

```sh
vp run @yielded/example-contracts#server
```

`ServerRpc.handlers(host)` returns the source's `group` and handler `services` for
an Effect `RpcServer`. Supply `ServerRpc.CurrentConnection` from authenticated
application context, never client-supplied RPC headers. The application chooses
its RPC transport and closes native connections when requested by the host.
`ServerRpc.serialization(maxBytes)` supplies bounded JSON serialization.

Host options default to:

| Option                                                  | Default                      |
| ------------------------------------------------------- | ---------------------------- |
| `replay.maxEvents` / `replay.maxBatchBytes`             | 512 events / 600,000 bytes   |
| `connections.maxConnections` / `connections.bufferSize` | 100 connections / 256 frames |
| `maxQueuedTurns`                                        | 128                          |
| `turnTimeoutMillis`                                     | 5,000 ms                     |
| `outbox`                                                | Disabled                     |

For durable delivery, supply a storage outbox capability and
`outbox: { batchSize, retryMillis, deliver, schedule }`. `schedule(atMillis)` must
persist the next wakeup; `schedule(undefined)` cancels it. Run `host.drainOutbox`
on each wakeup. The destination must deduplicate stable obligation ids. Drain
pending delivery before disabling it.

The optional [Cloudflare adapters](../reference/packages.mdx) wrap this same host
with native request, WebSocket, and alarm handling. Their `sqlite` helpers select
storage explicitly. The application still owns routing, typed object bindings,
and authentication before calling `prepareRequest` and forwarding to a stub.

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
