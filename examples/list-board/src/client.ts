import { Client } from "@yielded/sync/client";

import { Board } from "./contract.ts";

export const BoardClient = Client.definition(Board, {
  applyEvent: (snapshot, event) => {
    switch (event._tag) {
      case "Added":
        return { ...snapshot, cards: [...snapshot.cards, event.card] };
      case "Moved":
        return {
          ...snapshot,
          cards: snapshot.cards.map((card) =>
            card.id === event.id ? { ...card, lane: event.lane } : card,
          ),
        };
      case "Renamed":
        return { ...snapshot, title: event.title, titleRevision: event.titleRevision };
    }
  },
  optimistic: {
    addCard: (snapshot, payload) => ({
      ...snapshot,
      cards: [...snapshot.cards, { ...payload, lane: "todo" as const }],
    }),
    moveCard: (snapshot, payload) => ({
      ...snapshot,
      cards: snapshot.cards.map((card) =>
        card.id === payload.id ? { ...card, lane: payload.lane } : card,
      ),
    }),
    rename: (snapshot, payload) => {
      const { title, expectedRevision } =
        typeof payload === "string" ? { title: payload, expectedRevision: 0 } : payload;

      return expectedRevision === snapshot.titleRevision ? { ...snapshot, title } : snapshot;
    },
  },
});
