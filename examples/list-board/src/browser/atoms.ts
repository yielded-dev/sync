import type { ProtocolError } from "@yielded/sync";
import { IndexedDb } from "@yielded/sync-local-indexeddb";
import { BrowserSource } from "@yielded/sync-platform-browser";
import type { ClientError, PersistenceError } from "@yielded/sync/client";
import { Crypto, Effect, Option, Schema } from "effect";
import { Atom } from "effect/reactivity";

import { BoardClient } from "../client.ts";
import { BoardId, type CardRejected } from "../contract.ts";
import { DemoActor } from "../demo.ts";

const page = new URL(window.location.href);

export const actorId = Option.getOrElse(
  Schema.decodeUnknownOption(DemoActor)(page.searchParams.get("actor")),
  () => "alice" as const,
);

export const boardId = Option.getOrElse(
  Schema.decodeUnknownOption(BoardId)(page.searchParams.get("board")),
  () => BoardId.make("shared"),
);

export const otherActor = actorId === "alice" ? "bob" : "alice";

export const actorHref = (actor: typeof DemoActor.Type) => {
  const url = new URL(page);

  url.searchParams.set("actor", actor);
  url.searchParams.set("board", boardId);

  return url.href;
};

export const board = BrowserSource.bind(BoardClient, {
  id: boardId,
  actorId,
  query: { actor: actorId },
  persistence: IndexedDb.layer({ namespace: "list-board-demo-v1" }),
  actions: { moveCard: { concurrent: true } },
});

export const cardDraftAtom = Atom.make("");
export const titleDraftAtom = Atom.make<string | null>(null);
export const focusedCardAtom = Atom.make<string | null>(null);

export const addCardAtom = board.fn(function* () {
  const title = (yield* Atom.get(cardDraftAtom)).trim();

  if (title.length === 0) return;
  const crypto = yield* Crypto.Crypto;
  const id = yield* crypto.randomUUIDv4().pipe(Effect.orDie);

  yield* board.action.addCard({ id, title });
  yield* Atom.set(cardDraftAtom, "");
});

export const renameBoardAtom = board.fn(function* () {
  const title = (yield* Atom.get(titleDraftAtom))?.trim();

  if (title === undefined || title.length === 0) return;
  yield* board.action.rename(title);
  yield* Atom.set(titleDraftAtom, null);
});

export const presenceAtom = board.presence(
  Atom.make((get) => ({ editingCardId: get(focusedCardAtom) })),
);

export type Peer = Atom.Success<typeof presenceAtom>[number];
export type BoardError = ClientError | CardRejected | PersistenceError | ProtocolError;

export const errorMessage = (error: BoardError) => {
  if (error._tag === "CardRejected")
    return error.reason === "Duplicate"
      ? "That card already exists."
      : "That card no longer exists.";
  if (error._tag === "PersistenceError")
    return "Couldn’t open this browser’s saved session. Allow storage for this site, then reload.";

  if (error.reason === "OutcomeUnknown")
    return "We couldn’t confirm that change. Retry the saved change below when connected.";
  if (error.reason === "Storage")
    return "Browser storage is unavailable. Allow storage for this site, then reload.";
  if (error.reason === "Unauthorized")
    return "This session is no longer authorized. Reload to start a new demo session.";

  return error.message;
};
