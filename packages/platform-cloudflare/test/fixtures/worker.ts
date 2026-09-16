import { Action, Source } from "@yielded/sync";
import { Cloudflare } from "@yielded/sync-platform-cloudflare";
import { Server } from "@yielded/sync/server";
import { Effect, Layer, Schema } from "effect";

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
      if (principal.failure === "defect") return Effect.die("Authorization defect after commit");
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

export const PublicationObject: Cloudflare.SourceObject = Cloudflare.durableObject(server, {
  services: Layer.empty,
  storageNamespace: "publication",
  replay: { maxEvents: 4, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 4, bufferSize: 16 },
});
