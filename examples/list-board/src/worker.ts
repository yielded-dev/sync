import { ProtocolError } from "@yielded/sync";
import { EffectCf } from "@yielded/sync-platform-effect-cf";
import { Server } from "@yielded/sync/server";
import { Effect, Option, Schema } from "effect";
import { Worker } from "effect-cf";

import { Board, CardRejected, Cards } from "./contract.ts";
import { DemoActor } from "./demo.ts";

const Principal = Schema.Struct({ actorId: Schema.String }).pipe(Schema.brand("Principal"));

const board = Server.plugin(Board, {
  principal: Principal,
  state: Schema.Struct({ title: Schema.String }),
  initialize: Effect.succeed({ title: "Board" }),
  snapshot: (state) => state,
  actions: {
    rename: Effect.fn("Board.rename")(function* ({ state, payload }) {
      return {
        state: { ...state, title: payload },
        events: [{ title: payload }],
        result: payload,
      };
    }),
  },
});

const server = Server.make(Cards, {
  principal: Principal,
  state: Cards.spec.snapshot,
  initialize: Effect.succeed({ cards: [] }),
  snapshot: (state) => state,
  authorize: () => Effect.void,
  actions: {
    add: Effect.fn("Cards.add")(function* ({ state, payload }) {
      if (state.cards.some((card) => card.id === payload.id))
        return yield* CardRejected.make({ reason: "Duplicate" });
      const card = { ...payload, lane: "todo" as const };

      return {
        state: { cards: [...state.cards, card] },
        events: [{ _tag: "Added", card }],
        result: { id: card.id, count: state.cards.length + 1 },
      };
    }),
    move: Effect.fn("Cards.move")(function* ({ state, payload }) {
      if (!state.cards.some((card) => card.id === payload.id))
        return yield* CardRejected.make({ reason: "Missing" });

      return {
        state: {
          cards: state.cards.map((card) =>
            card.id === payload.id ? { ...card, lane: payload.lane } : card,
          ),
        },
        events: [{ _tag: "Moved", ...payload }],
        result: payload,
      };
    }),
  },
  plugins: [board],
});

export const BoardObject = EffectCf.make(server, {
  storage: { namespace: "list-board-v1" },
  replay: { maxEvents: 16, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 32, bufferSize: 32 },
});

export type BoardObject = InstanceType<typeof BoardObject>;

const Boards = BoardObject.binding("BOARDS");

const authenticate = Effect.fn("authenticate")(function* (request: Request) {
  const url = new URL(request.url);

  // Public demo identities only. Browser WebSockets cannot set Authorization.
  // Replace this selection with the application's auth provider before deployment.
  const token = request.headers.get("authorization");

  const actorId = Option.getOrUndefined(
    Schema.decodeUnknownOption(DemoActor)(
      token === "Bearer alice-local"
        ? "alice"
        : token === "Bearer bob-local"
          ? "bob"
          : token === null
            ? url.searchParams.get("actor")
            : undefined,
    ),
  );

  if (actorId === undefined)
    return yield* ProtocolError.make({
      reason: "Unauthenticated",
      message: "Demo credentials required",
    });

  return { actorId, principal: { actorId }, expiresAtMillis: 4_000_000_000_000 };
});

const fetch = Boards.handler({ authenticate, objectName: (id) => `board:${id}` });

export default Worker.makeFetchHandler(Boards.layer, { fetch });
