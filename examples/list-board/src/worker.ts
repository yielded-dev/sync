import { ProtocolError } from "@yielded/sync";
import { EffectCf } from "@yielded/sync-platform-effect-cf";
import { Server } from "@yielded/sync/server";
import { Effect, Option, Schema } from "effect";
import { Worker } from "effect-cf";

import { Board, CardRejected, Cards } from "./contract.ts";
import { DemoActor } from "./demo.ts";

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
        }),
      );
    },
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

const fetch = Effect.gen(function* () {
  const request = yield* Worker.NativeRequest;
  const url = new URL(request.url);
  const match = /^\/sync\/boards\/([^/]+)\/?$/.exec(url.pathname);

  if (match === null) return new Response("Not found", { status: 404 });

  const id = yield* Schema.decodeEffect(Schema.StringFromUriComponent)(match[1]).pipe(
    Effect.mapError(() =>
      ProtocolError.make({ reason: "UnsupportedVersion", message: "Invalid source id" }),
    ),
  );

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

  const stub = yield* Boards.getByName(`board:${id}`);

  const prepared = yield* EffectCf.prepareRequest(Cards, {
    request,
    address: Cards.address(id),
    identity: { actorId, principal: { actorId }, expiresAtMillis: 4_000_000_000_000 },
  });

  return yield* Boards.fetch(stub, prepared);
}).pipe(
  Effect.catchTag("DurableObjectFetchError", () =>
    Effect.fail(ProtocolError.make({ reason: "Unavailable", message: "Source unavailable" })),
  ),
  Effect.catchTag("ProtocolError", (error) => Effect.succeed(EffectCf.errorResponse(error))),
);

export default Worker.makeFetchHandler(Boards.layer, { fetch });
