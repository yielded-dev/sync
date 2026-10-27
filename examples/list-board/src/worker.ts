import { ProtocolError } from "@yielded/sync";
import { DurableObject } from "@yielded/sync-platform-cloudflare";
import { EffectCfServices } from "@yielded/sync-platform-effect-cf";
import { Server } from "@yielded/sync/server";
import { Effect, Option, Schema } from "effect";
import { Worker } from "effect-cf";
import { HttpServerRequest } from "effect/http/HttpServerRequest";

import { Board, CardRejected, RenameRejected } from "./contract.ts";
import { DemoActor } from "./demo.ts";

const Principal = Schema.Struct({ actorId: Schema.String }).pipe(Schema.brand("Principal"));

const BoardServer = Server.make(Board, {
  principal: Principal,
  state: Board.spec.snapshot,
  initialize: Effect.succeed({ title: "Board", titleRevision: 0, cards: [] }),
  snapshot: (state) => state,
  authorize: () => Effect.void,
  actions: {
    addCard: Effect.fn("Board.addCard")(function* ({ state, payload }) {
      if (state.cards.some((card) => card.id === payload.id))
        return yield* CardRejected.make({ reason: "Duplicate" });
      const card = { ...payload, lane: "todo" as const };

      return {
        state: { ...state, cards: [...state.cards, card] },
        events: [{ _tag: "Added", card }],
        result: { id: card.id, count: state.cards.length + 1 },
      };
    }),
    moveCard: Effect.fn("Board.moveCard")(function* ({ state, payload }) {
      if (!state.cards.some((card) => card.id === payload.id))
        return yield* CardRejected.make({ reason: "Missing" });

      return {
        state: {
          ...state,
          cards: state.cards.map((card) =>
            card.id === payload.id ? { ...card, lane: payload.lane } : card,
          ),
        },
        events: [{ _tag: "Moved", ...payload }],
        result: payload,
      };
    }),
    rename: Effect.fn("Board.rename")(function* ({ state, payload }) {
      const { title, expectedRevision } =
        typeof payload === "string" ? { title: payload, expectedRevision: 0 } : payload;

      if (expectedRevision !== state.titleRevision)
        return yield* RenameRejected.make({ currentRevision: state.titleRevision });
      const titleRevision = state.titleRevision + 1;

      return {
        state: { ...state, title, titleRevision },
        events: [{ _tag: "Renamed", title, titleRevision }],
        result: title,
      };
    }),
  },
});

export const BoardObject = DurableObject.make(BoardServer, {
  storage: { namespace: "list-board-v1" },
  replay: { maxEvents: 16, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 32, bufferSize: 32 },
}).provide(EffectCfServices);

export type BoardObject = typeof BoardObject.Type;

const Boards = BoardObject.binding("BOARDS");

const authenticate = Effect.fn("authenticate")(function* () {
  const request = yield* HttpServerRequest;
  const url = new URL(request.originalUrl);

  // Public demo identities only. Browser WebSockets cannot set Authorization.
  // Replace this selection with the application's auth provider before deployment.
  const token = request.headers.authorization;

  const actorId = Option.getOrUndefined(
    Schema.decodeUnknownOption(DemoActor)(
      token === "Bearer alice-local"
        ? "alice"
        : token === "Bearer bob-local"
          ? "bob"
          : token === undefined
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

const fetch = Boards.handler({ authenticate });

export default Worker.makeFetchHandler(Boards.layer, { fetch });
