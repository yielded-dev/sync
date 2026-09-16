# Consumer API sketch

This sketch illustrates the [public API contracts](../PUBLIC_API.md). Shared
`Source`, `Action`, and `Plugin` definitions are implemented and exercised by the
[external contracts example](../../examples/contracts/README.md). The server and
Cloudflare host also run in the [counter example](../../examples/cloudflare/README.md).
`Client`, Atom, and local persistence still describe the remaining runtime facade,
so this complete example is not runnable yet. Effect Schema/Layer/Scope are the underlying
primitives.

The application owns access checks and HTTP authentication. These are the only
application-specific dependencies in the sketch; their contracts are shown below.
The example has two composed capabilities: a counter and a reusable label.

## Shared contract (`counter.ts`)

```ts
import { Schema } from "effect";
import { Action, Plugin, Source } from "@yielded/sync";

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
import { Context, Effect, Schema } from "effect";
import type { Server } from "@yielded/sync/server";

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
import { Effect, Schema } from "effect";
import { Server } from "@yielded/sync/server";
import { ProtocolError } from "@yielded/sync";
import { Counter, InvalidValue, Label } from "./counter";
import { Access, Principal } from "./access";

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

## Cloudflare assembly (`worker.ts`)

```ts
import { Cloudflare } from "@yielded/sync-platform-cloudflare";
import { CounterServer } from "./counter-server";
import { AccessLive, authenticate } from "./application-auth";

export const CounterObject = Cloudflare.durableObject(CounterServer, {
  services: AccessLive,
  storageNamespace: "counter-v1",
  replay: { maxEvents: 512, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 100, bufferSize: 256 },
});

export default Cloudflare.worker(CounterServer.contract, {
  binding: "COUNTERS",
  path: "/sync/counters/:id",
  objectName: (id) => `counter:${id}`,
  authenticate,
});
```

`COUNTERS` is the application's binding to `CounterObject`. `authenticate(request)`
is an application Effect returning `{ actorId, principal: { actorId }, expiresAtMillis }`
or a classified `ProtocolError`. The worker forwards a trusted bound identity;
client headers/payloads cannot impersonate internal authorization data.

The adapter derives the five RPC operations and delegates execution to the server
runtime using JSON Effect RPC. HTTP supports unary snapshot/action/result operations;
WebSocket RPC also carries subscriptions and ephemeral messages. It owns SQLite
transactions, socket hibernation and outbox wakeups. This
example has no external effects. A source needing projections adds schema-encoded
outbox records to its plans and supplies a typed delivery Effect at this boundary;
its destination remains application-owned.

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
import { Effect } from "effect";
import { Client, ReplicaPersistence } from "@yielded/sync/client";
import { IndexedDb } from "@yielded/sync-local-indexeddb";
import { Counter } from "./counter";
import { CounterClient } from "./counter-client";

export const openSession = Effect.fn("openSession")(function* (actorId: string) {
  const storage = yield* ReplicaPersistence;
  const transport = yield* Client.rpcTransport(Counter, {
    url: "/sync/counters",
    credentials: "include",
  });
  return yield* Client.make(CounterClient, {
    actorId,
    transport,
    persistence: { mode: "persistent", storage },
    retry: { maxAttempts: 8, initialDelay: "250 millis", maxDelay: "30 seconds" },
    limits: { maxSources: 32, maxPendingPerSource: 100, frameBuffer: 256 },
  });
});

// Inside the application's authenticated session scope:
export const browserSession = (actorId: string) =>
  openSession(actorId).pipe(
    Effect.provide(IndexedDb.layer({ namespace: "counter-demo-v1", actorId })),
  );
```

`rpcTransport` uses contract-derived Effect RPC clients. Its platform HTTP/socket
requirements remain in the returned Effect and are supplied by the browser app's
Effect platform Layers. IndexedDB supplies only persistence. This session effect
requires `Scope`: the caller retains that scope for the session and closes it on
identity change. Do not return a live client from an already-completed
`Effect.scoped` block.

For Expo, provide `ExpoSqlite.layer({ namespace: "counter-demo-v1", actorId })` from
`@yielded/sync-local-expo`. Tests can provide
`ReplicaPersistence.layerMemory({ actorId })`. A consumer deliberately choosing
no persistence passes `{ mode: "volatile" }` to `Client.make`. Adapter failures do
not change that choice. Logout calls `storage.wipe` only if application policy
requires deletion; ordinary session disposal leaves unresolved evidence intact.

## Headless use and result recovery

```ts
import { Effect, Stream } from "effect";
import { Counter } from "./counter";

// session is the Client.make result above. This Effect runs in its child scope.
const run = Effect.gen(function* () {
  const replica = yield* session.open(Counter.address("demo"));
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

// Session-owned binding; does not create another headless runtime.
const atoms = SourceAtom.make(session);
const address = Counter.address("demo");
const replicaAtom = atoms.replica(address); // Active: holds a source lease.
const statusAtom = atoms.status(address); // Passive: never opens a source.
const cachedAtom = atoms.passiveReplica(address);
const setSeven = atoms.execute(address, Counter.actions.set, { value: 7 });
const recover = atoms.recover(address);
```

The UI reads atoms with its Effect Atom bindings and dispatches the mutation and
recovery Effects through its existing runtime. The binding creates a child scope
for each active lease; registry unmount/disposal releases it. Headless and Atom
leases share the same coordinator. Passive reads, snapshot eviction, and source
unmount never silently delete pending journal evidence.
