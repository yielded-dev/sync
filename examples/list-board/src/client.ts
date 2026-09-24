import { IndexedDb } from "@yielded/sync-local-indexeddb";
import { Client } from "@yielded/sync/client";
import { Effect } from "effect";

import { Board, Cards } from "./contract.ts";

export const BoardClient = Client.definition(Cards, {
  applyEvent: (snapshot, event) => {
    switch (event._tag) {
      case "Added":
        return { cards: [...snapshot.cards, event.card] };
      case "Moved":
        return {
          cards: snapshot.cards.map((card) =>
            card.id === event.id ? { ...card, lane: event.lane } : card,
          ),
        };
    }
  },
  optimistic: {
    add: (snapshot, payload) => ({
      cards: [...snapshot.cards, { ...payload, lane: "todo" as const }],
    }),
    move: (snapshot, payload) => ({
      cards: snapshot.cards.map((card) =>
        card.id === payload.id ? { ...card, lane: payload.lane } : card,
      ),
    }),
  },
  plugins: [
    Client.plugin(Board, {
      applyEvent: (_snapshot, event) => event,
      optimistic: { rename: (_snapshot, title) => ({ title }) },
    }),
  ],
});

export const openBrowserSession = Effect.fn("Board.openBrowserSession")(function* (
  actorId: string,
) {
  const storage = yield* IndexedDb.open({ namespace: "list-board-demo-v1", actorId });
  const transport = yield* Client.rpcTransport(Cards);

  return yield* Client.make(BoardClient, {
    actorId,
    transport,
    persistence: { mode: "persistent", storage },
  });
});
