import { ProtocolError } from "@yielded/sync";
import { Cloudflare } from "@yielded/sync-platform-cloudflare";
import { Server } from "@yielded/sync/server";
import { Effect, Layer, Schema } from "effect";

import { Counter, InvalidValue, Label } from "./contract.ts";

const Principal = Schema.Struct({ actorId: Schema.String });

const LabelServer = Server.plugin(Label, {
  principal: Principal,
  state: Schema.Struct({ text: Schema.String, changedBy: Schema.NullOr(Schema.String) }),
  initialize: Effect.succeed({ text: "Untitled", changedBy: null }),
  snapshot: (state) => ({ text: state.text }),
  actions: {
    rename: ({ state, payload, principal }) =>
      Effect.succeed(
        Server.commit({
          state: { ...state, text: payload, changedBy: principal.actorId },
          events: [{ text: payload }],
          result: payload,
          outbox: [],
        }),
      ),
  },
});

export const CounterServer: Server.Definition<typeof Counter.spec> = Server.make(Counter, {
  principal: Principal,
  state: Schema.Struct({ value: Schema.Int, lastActorId: Schema.NullOr(Schema.String) }),
  initialize: Effect.succeed({ value: 0, lastActorId: null }),
  snapshot: (state) => ({ value: state.value }),
  authorize: ({ principal }) =>
    principal.actorId === "blocked"
      ? Effect.fail(ProtocolError.make({ reason: "Forbidden", message: "Access denied" }))
      : Effect.void,
  actions: {
    set: ({ state, payload, principal }) =>
      payload > 100
        ? Effect.fail(InvalidValue.make({ maximum: 100 }))
        : Effect.succeed(
            Server.commit({
              state: { value: payload, lastActorId: principal.actorId },
              events: [{ value: payload }],
              result: { previous: state.value, current: payload },
              outbox: [{ value: payload }],
            }),
          ),
  },
  plugins: [LabelServer],
});

export const CounterObject: Cloudflare.SourceObject = Cloudflare.durableObject(CounterServer, {
  services: Layer.empty,
  storageNamespace: "counter-v1",
  replay: { maxEvents: 4, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 32, bufferSize: 16 },
  outbox: {
    maxPending: 100,
    batchSize: 10,
    retryMillis: 60_000,
    deliver: () => Effect.succeed({ _tag: "Delivered" as const }),
  },
});

export type CounterObject = InstanceType<typeof CounterObject>;

// Local demonstration credentials. Replace this function with the application's auth provider.
const worker: Cloudflare.SourceWorker = Cloudflare.worker(Counter, {
  binding: "COUNTERS",
  path: "/sync/counters/:id",
  objectName: (id) => `counter:${id}`,
  authenticate: (request) => {
    const token = request.headers.get("authorization");

    const actorId =
      token === "Bearer alice-local" ? "alice" : token === "Bearer bob-local" ? "bob" : undefined;

    return actorId === undefined
      ? Effect.fail(
          ProtocolError.make({ reason: "Unauthenticated", message: "Use a local demo credential" }),
        )
      : Effect.succeed({ actorId, principal: { actorId }, expiresAtMillis: 4_000_000_000_000 });
  },
});

export default worker;
