---
title: Consumer API sketch
---

This sketch illustrates [how Sync works](../guide/concepts.md). Shared
`Source`, `Action`, and `Plugin` definitions are implemented and exercised by the
[external contracts example](https://github.com/yielded-dev/sync/tree/main/examples/contracts).
The server and Cloudflare host also run in the
[counter example](https://github.com/yielded-dev/sync/tree/main/examples/cloudflare).
`Client`, Atom, IndexedDB and Expo SQLite are implemented. The persistence sections
use scoped adapters with explicit application namespaces. Effect Schema/Layer/Scope are the underlying
primitives.

The application owns access checks and HTTP authentication. These are the only
application-specific dependencies in the sketch; their contracts are shown below.
The example has two composed capabilities: a counter and a reusable label.

## Shared contract (`counter.ts`)

```ts
import { Action, Plugin, Source } from "@yielded/sync";
import { Schema } from "effect";

export class InvalidValue extends Schema.TaggedError<InvalidValue>()("InvalidValue", {
  maximum: Schema.Number,
}) {}

export const Label = Plugin.make("label", {
  snapshot: Schema.Struct({ text: Schema.String }),
  event: Schema.Struct({ text: Schema.String }),
  message: Schema.Never,
  actions: [
    Action.make("rename", {
      payload: Schema.Struct({ text: Schema.String }),
      success: Schema.Struct({ text: Schema.String }),
      error: Schema.Never,
    }),
  ],
});

export const Counter = Source.make({
  kind: "counter",
  schemaVersion: 1,
  snapshot: Schema.Struct({ value: Schema.Number }),
  event: Schema.Struct({ value: Schema.Number }),
  message: Schema.Struct({ editing: Schema.Boolean }),
  actions: [
    Action.make("set", {
      payload: Schema.Struct({ value: Schema.Number }),
      success: Schema.Struct({ previous: Schema.Number, current: Schema.Number }),
      error: InvalidValue,
    }),
  ],
  plugins: [Label],
});

// Counter.snapshot describes:
// { source: { value: number }, plugins: { label: { text: string } } }
// Counter.actions.set and Counter.plugins.label.actions.rename retain their
// own payload/success/error types. Neither imports server-private state.
```

## Application auth services (`access.ts`)

```ts
import type { Server } from "@yielded/sync/server";
import { Context, Effect, Schema } from "effect";

export const Principal = Schema.Struct({ actorId: Schema.String });
export type Principal = typeof Principal.Type;

export class AccessDenied extends Schema.TaggedError<AccessDenied>()("AccessDenied", {}) {}

export class Access extends Context.Service<
  Access,
  {
    readonly authorize: (input: {
      readonly principal: Principal;
      readonly address: { readonly kind: string; readonly id: string };
      readonly operation: Server.AuthorizationOperation;
    }) => Effect.Effect<void, AccessDenied>;
  }
>()("example/Access") {}

// AuthorizationOperation distinguishes snapshot, subscribe, action, result,
// and message. Action/result operations include their namespaced action and
// command identity. The application provides AccessLive from its auth model.
```

## Private server definition (`counter-server.ts`)

```ts
import { ProtocolError } from "@yielded/sync";
import { Server } from "@yielded/sync/server";
import { Effect, Schema } from "effect";

import { Access, Principal } from "./access";
import { Counter, InvalidValue, Label } from "./counter";

const LabelServer = Server.plugin(Label, {
  state: Schema.Struct({ text: Schema.String, changedBy: Schema.NullOr(Schema.String) }),
  initialize: Effect.succeed({ text: "Untitled", changedBy: null }),
  snapshot: (state) => ({ text: state.text }),
  actions: {
    rename: ({ state, payload, principal }) =>
      Effect.succeed(
        Server.commit({
          state: { ...state, text: payload.text, changedBy: principal.actorId },
          events: [{ text: payload.text }],
          result: { text: payload.text },
          outbox: [],
        }),
      ),
  },
  principal: Principal,
});

export const CounterServer = Server.make(Counter, {
  principal: Principal,
  state: Schema.Struct({ value: Schema.Number, lastActorId: Schema.NullOr(Schema.String) }),
  initialize: Effect.succeed({ value: 0, lastActorId: null }),
  snapshot: (state) => ({ value: state.value }),
  authorize: Effect.fn("Counter.authorize")(function* (input) {
    const access = yield* Access;
    yield* access
      .authorize(input)
      .pipe(
        Effect.mapError(() =>
          ProtocolError.make({ reason: "Forbidden", message: "Access denied" }),
        ),
      );
  }),
  actions: {
    set: Effect.fn("Counter.set")(function* ({ state, payload, principal }) {
      if (payload.value > 100) return yield* new InvalidValue({ maximum: 100 });
      return Server.commit({
        state: { value: payload.value, lastActorId: principal.actorId },
        events: [{ value: payload.value }],
        result: { previous: state.value, current: payload.value },
        outbox: [],
      });
    }),
  },
  plugins: [LabelServer],
});
```

The runtime runs the root authorization for plugin operations too. Private
`changedBy`/`lastActorId` fields are persisted but excluded by the public projections.
A set changes only the root slot; a rename changes only `plugins.label`. The runtime
assigns one source cursor across both event namespaces. The source's `Access`
requirement remains visible until the application supplies its Layer.

A handler that fails with `InvalidValue` records the exact typed rejection without
changing state. A successful plan atomically commits state, event, and full result.
For example, retrying a successful set from 0 to 7 still returns
`{ previous: 0, current: 7 }` even if another command has since changed the counter.

## effect-cf assembly (`worker.ts`)

```ts
import { ProtocolError } from "@yielded/sync";
import { EffectCf } from "@yielded/sync-platform-effect-cf";
import { Server } from "@yielded/sync/server";
import { Context, Effect } from "effect";
import { DurableObject, DurableObjectNamespace, Worker } from "effect-cf";

import { AccessLive, authenticate } from "./application-auth";
import { CounterServer } from "./counter-server";

const sync = EffectCf.make(Server.provide(CounterServer, AccessLive), {
  storageNamespace: "counter-v1",
  replay: { maxEvents: 512, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 100, bufferSize: 256 },
});

export const CounterObject = DurableObject.make(sync.layer, sync.handlers);

class Counters extends Context.Service<
  Counters,
  DurableObjectNamespace.DurableObjectNamespaceEffectClient<InstanceType<typeof CounterObject>>
>()("Counters") {
  static readonly layer = DurableObjectNamespace.layer(this, { binding: "COUNTERS" });
}

const fetch = Effect.gen(function* () {
  const request = yield* Worker.NativeRequest;
  const match = /^\/sync\/counters\/([^/]+)\/?$/.exec(new URL(request.url).pathname);
  if (match === null) return new Response("Not found", { status: 404 });

  const id = yield* Effect.try({
    try: () => decodeURIComponent(match[1]),
    catch: () => ProtocolError.make({ reason: "UnsupportedVersion", message: "Invalid id" }),
  });
  const identity = yield* authenticate(request);
  const counters = yield* Counters;
  const stub = yield* counters.getByName(`counter:${id}`);

  const prepared = yield* EffectCf.prepareRequest(CounterServer.contract, {
    request,
    address: CounterServer.contract.address(id),
    identity,
  });

  return yield* counters.fetch(stub, prepared);
}).pipe(Effect.catchTag("ProtocolError", (error) => Effect.succeed(EffectCf.errorResponse(error))));

export default Worker.makeFetchHandler(Counters.layer, { fetch });
```

`COUNTERS` is the application's typed binding to `CounterObject`.
`authenticate(request)` returns `{ actorId, principal: { actorId }, expiresAtMillis }`
or a classified `ProtocolError`. Request preparation replaces internal identity headers;
only authenticated routes should expose the object binding. `AccessLive` is acquired
and finalized within each source operation through `Server.provide`. The instance
Layer owns the long-lived RPC server. Applications can use their
own router and other Worker handlers alongside this route.

## Alchemy v2 assembly

Use `@yielded/sync-platform-alchemy-cf` with Alchemy's native constructors:

```ts
import { AlchemyCf } from "@yielded/sync-platform-alchemy-cf";
import { Server } from "@yielded/sync/server";
import * as Cloudflare from "alchemy/Cloudflare";

import { AccessLive } from "./application-auth";
import { CounterServer } from "./counter-server";

export class CounterObject extends Cloudflare.DurableObject<CounterObject>()(
  "CounterObject",
  AlchemyCf.make(Server.provide(CounterServer, AccessLive), {
    storageNamespace: "counter-v1",
    replay: { maxEvents: 512, maxBatchBytes: 600_000 },
    sockets: { maxConnections: 100, bufferSize: 256 },
  }),
) {}
```

The native Alchemy Worker resolves `CounterObject`, selects a stub with
`getByName`, authenticates the request, and calls `AlchemyCf.prepareRequest` with
the native Effect HTTP request, address, and identity. It then calls `stub.fetch`
with the prepared request. The
[runnable Alchemy counter](https://github.com/yielded-dev/sync/tree/main/examples/alchemy-cloudflare)
shows the complete Worker and stack. Alchemy resolves infrastructure dependencies
in the construction Effect; the returned runtime Effect initializes SQLite in the
actual Durable Object.

Both adapters delegate to the same source runtime and JSON Effect RPC protocol.
HTTP serves unary snapshot/action/result operations; WebSockets also carry
subscriptions and ephemeral messages. `RpcServer` owns decoding, dispatch, typed
responses, stream acknowledgements, and interruption. effect-cf supplies its native
RPC socket transport; Alchemy uses a Cloudflare `RpcServer.Protocol` bridge. The
shared source implementation owns SQLite transactions, bounded publication, replay,
and outbox wakeups. An activation lost during a subscription resets the RPC stream;
the client reconnects and replays from its retained cursor. Keep named-object keys and
storage namespaces stable when changing assembly. A framework migration that
changes the Cloudflare namespace requires an explicit native migration.

A source needing projections adds Schema-encoded outbox records to its plans and
supplies a typed delivery Effect at this boundary. Its destination remains
application-owned. Sync uses the object's alarm; compose unrelated alarm jobs
through an explicit application scheduler.

## Client definition (`counter-client.ts`)

```ts
import { Client } from "@yielded/sync/client";

import { Counter, Label } from "./counter";

const LabelClient = Client.plugin(Label, {
  applyEvent: (_snapshot, event) => ({ text: event.text }),
  optimistic: { rename: (_snapshot, payload) => ({ text: payload.text }) },
});

export const CounterClient = Client.definition(Counter, {
  applyEvent: (_snapshot, event) => ({ value: event.value }),
  optimistic: { set: (_snapshot, payload) => ({ value: payload.value }) },
  plugins: [LabelClient],
});
```

Reducers do not allocate ids or perform I/O. The runtime correlates command ids in
sequenced envelopes, applies only contiguous events, and overlays unresolved local
intents on the authoritative snapshot. Plugin reducers operate only on their slot.

## Actor-scoped browser session (`session.ts`)

```ts
import { IndexedDb } from "@yielded/sync-local-indexeddb";
import { Client } from "@yielded/sync/client";
import { Context, Layer } from "effect";

import { Counter } from "./counter";
import { CounterClient } from "./counter-client";

export class CounterSession extends Context.Service<
  CounterSession,
  Client.Runtime<typeof Counter.spec>
>()("app/CounterSession") {
  static readonly layer = Layer.effect(
    this,
    Client.make(CounterClient, {
      persistence: { mode: "persistent" },
      retry: { maxAttempts: 8, initialDelay: "250 millis", maxDelay: "30 seconds" },
      limits: { maxSources: 32, maxPendingPerSource: 100, frameBuffer: 256 },
    }),
  );
}

export const browserSession = CounterSession.layer.pipe(
  Layer.provide([
    IndexedDb.layer({ namespace: "counter-demo-v1" }),
    Client.layerRpcTransport(Counter),
  ]),
);
```

`browserSession` requires `Client.CurrentActor` and `RpcClient.Protocol`. At the
application's session boundary, provide the authenticated socket protocol and
the actor once:

```ts
const SessionLive = browserSession.pipe(
  Layer.provide(socketProtocol),
  Layer.provide(Layer.succeed(Client.CurrentActor, { actorId })),
);
```

The application owns `socketProtocol`, including the route, credentials, socket,
and JSON RPC serialization. It can read `CurrentActor` while acquiring that
protocol. Pass `SessionLive` to the application's Atom runtime, or provide it
around the whole headless workflow. Layers retain their acquired resources for
that lifetime. Close the old session before building one for another actor; do
not return a live client from an already-completed `Effect.scoped` block.

For Expo, provide `ExpoSqlite.layer({ namespace: "counter-demo-v1" })` from
`@yielded/sync-local-expo`. Tests can provide
`ReplicaPersistence.layerMemory()`. Both read the same `CurrentActor`. A consumer
deliberately choosing no persistence passes `{ persistence: { mode: "volatile" } }`
to `Client.make`, which then requires no persistence service. Adapter failures do
not change that choice. Logout calls the storage handle's `wipe` only if application policy
requires deletion; ordinary session disposal leaves unresolved evidence intact.

## Headless use and result recovery

```ts
import { Effect, Stream } from "effect";

import { Counter } from "./counter";
import { CounterSession } from "./session";

// Provide SessionLive around this workflow and keep its scope alive.
const run = Effect.gen(function* () {
  const session = yield* CounterSession;
  const replica = yield* session.open(Counter.address("demo"));
  yield* replica.ready;
  yield* replica.changes.pipe(
    Stream.runForEach((state) => Effect.log(state)),
    Effect.forkScoped,
  );

  // Resolves to { previous: number, current: number }; InvalidValue stays typed.
  const result = yield* replica.execute(Counter.actions.set, { value: 7 });
  yield* replica.execute(Counter.plugins.label.actions.rename, { text: "Demo" });
  yield* replica.publishMessage({ editing: true });
  return result;
});
```

`execute` admits and journals an immutable command before sending it. If delivery
is uncertain, its typed operational error carries `commandId`; callers may use
`replica.retry(commandId)` or let the coordinator recover it. There is no payload
argument to `retry`. If an event arrives before the RPC response, the UI may show
the authoritative value while the same command remains in result recovery.
Reopening the session restores that evidence and queries the exact outcome.

## Atom view binding

```ts
import { SourceAtom } from "@yielded/sync/atom";

import { Counter } from "./counter";
import { CounterSession } from "./session";

// Inside the session's acquisition Effect: shares the same headless runtime.
const session = yield * CounterSession;
const atoms = SourceAtom.make(session);
const address = Counter.address("demo");
const replicaAtom = atoms.replica(address); // Active: holds a source lease.
const statusAtom = atoms.status(address); // Passive: never opens a source.
const cachedAtom = atoms.passiveReplica(address);
const setSeven = atoms.execute(address, Counter.actions.set, { value: 7 });
const recover = atoms.recover(address);
```

Active and passive replica atoms expose `AsyncResult` values; status atoms expose the
connection string. The UI reads them with its Effect Atom bindings and dispatches the mutation and
recovery Effects through its existing runtime. The binding creates a child scope
for each active lease; registry unmount/disposal releases it. Headless and Atom
leases share the same coordinator. Passive reads, snapshot eviction, and source
unmount never silently delete pending journal evidence.

For a complete frontend, see the [React board](https://github.com/yielded-dev/sync/tree/main/examples/list-board).
It uses `@effect/atom-react`, native WebSocket RPC, and IndexedDB, with a single
development command for both the UI and its Worker.
