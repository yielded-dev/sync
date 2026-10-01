import { Action, Source } from "@yielded/sync";
import { EffectCf } from "@yielded/sync-platform-effect-cf";
import { Server } from "@yielded/sync/server";
import { Effect, Schema } from "effect";
import { DurableObject } from "effect-cf";

export { CounterObject, default } from "../../../../examples/cloudflare/src/worker.ts";

export const Publication = Source.make({
  kind: "publication",
  schemaVersion: 1,
  snapshot: Schema.Int,
  event: Schema.Int,
  message: Schema.Never,
  actions: [Action.make("set", { payload: Schema.Int, success: Schema.Int, error: Schema.Never })],
  plugins: [],
});

const server = Server.make(Publication, {
  principal: Schema.Struct({ failure: Schema.String }),
  state: Schema.Int,
  initialize: Effect.succeed(0),
  snapshot: (state) => state,
  authorize: ({ principal, state, operation }) => {
    if (operation._tag === "subscribe" && state > 0) {
      if (principal.failure === "interrupt") return Effect.interrupt;
    }

    return Effect.void;
  },
  actions: {
    set: ({ payload }) =>
      Effect.succeed(
        Server.commit({ state: payload, events: [payload], result: payload, outbox: [] }),
      ),
  },
  plugins: [],
});

const sync = EffectCf.make(server, {
  storageNamespace: "publication",
  replay: { maxEvents: 4, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 4, bufferSize: 16 },
});

export const PublicationObject = DurableObject.make(sync.layer, sync.handlers);
