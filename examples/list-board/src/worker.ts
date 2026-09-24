import { ProtocolError } from "@yielded/sync";
import { Cloudflare } from "@yielded/sync-platform-cloudflare";
import { Server } from "@yielded/sync/server";
import { Effect, Layer, Schema } from "effect";

import { Board, CardRejected, Cards } from "./contract.ts";

const Principal = Schema.Struct({ actorId: Schema.String });

const board = Server.plugin(Board, {
  principal: Principal,
  state: Schema.Struct({ title: Schema.String }),
  initialize: Effect.succeed({ title: "Board" }),
  snapshot: (state) => state,
  actions: {
    rename: ({ state, payload }) =>
      Effect.succeed(
        Server.commit({
          state: { ...state, title: payload },
          events: [{ title: payload }],
          result: payload,
          outbox: [],
        }),
      ),
  },
});

const server = Server.make(Cards, {
  principal: Principal,
  state: Cards.spec.snapshot,
  initialize: Effect.succeed({ cards: [] }),
  snapshot: (state) => state,
  authorize: () => Effect.void,
  actions: {
    add: ({ state, payload }) => {
      if (state.cards.some((card) => card.id === payload.id))
        return Effect.fail(CardRejected.make({ reason: "Duplicate" }));
      const card = { ...payload, lane: "todo" as const };

      return Effect.succeed(
        Server.commit({
          state: { cards: [...state.cards, card] },
          events: [{ _tag: "Added" as const, card }],
          result: { id: card.id, count: state.cards.length + 1 },
          outbox: [],
        }),
      );
    },
    move: ({ state, payload }) => {
      if (!state.cards.some((card) => card.id === payload.id))
        return Effect.fail(CardRejected.make({ reason: "Missing" }));

      return Effect.succeed(
        Server.commit({
          state: {
            cards: state.cards.map((card) =>
              card.id === payload.id ? { ...card, lane: payload.lane } : card,
            ),
          },
          events: [{ _tag: "Moved" as const, ...payload }],
          result: payload,
          outbox: [],
        }),
      );
    },
  },
  plugins: [board],
});

export const BoardObject: Cloudflare.SourceObject = Cloudflare.durableObject(server, {
  services: Layer.empty,
  storageNamespace: "list-board-v1",
  replay: { maxEvents: 16, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 32, bufferSize: 32 },
});

export type BoardObject = InstanceType<typeof BoardObject>;

export default Cloudflare.worker(Cards, {
  binding: "BOARDS",
  path: "/sync/boards/:id",
  objectName: (id) => `board:${id}`,
  authenticate: (request) => {
    const token = request.headers.get("authorization");

    const actorId =
      token === "Bearer alice-local" ? "alice" : token === "Bearer bob-local" ? "bob" : undefined;

    return actorId === undefined
      ? Effect.fail(
          ProtocolError.make({ reason: "Unauthenticated", message: "Demo credentials required" }),
        )
      : Effect.succeed({ actorId, principal: { actorId }, expiresAtMillis: 4_000_000_000_000 });
  },
});
