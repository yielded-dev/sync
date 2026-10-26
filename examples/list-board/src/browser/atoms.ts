import { BrowserCrypto } from "@effect/platform-browser";
import type { ProtocolError } from "@yielded/sync";
import { IndexedDb } from "@yielded/sync-local-indexeddb";
import { SourceAtom } from "@yielded/sync/atom";
import { Client, type ClientError, type PersistenceError } from "@yielded/sync/client";
import { Crypto, Effect, Layer, Option, Schema } from "effect";
import { Atom } from "effect/reactivity";
import { Socket } from "effect/socket";

import { BoardClient } from "../client.ts";
import { Board, BoardId, type CardRejected } from "../contract.ts";
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

const transport = Client.layerWebSocketTransport(Board, {
  origin: page.origin,
  id: boardId,
  query: { actor: actorId },
}).pipe(Layer.provide(Socket.layerWebSocketConstructorGlobal));

const runtime = Atom.runtime(
  Layer.mergeAll(
    transport,
    IndexedDb.layer({ namespace: "list-board-demo-v1" }),
    BrowserCrypto.layer,
  ).pipe(Layer.provideMerge(Layer.succeed(Client.CurrentActor, { actorId }))),
);

export const board = SourceAtom.bind(BoardClient, {
  runtime,
  address: Board.address(boardId),
  persistence: { mode: "persistent" },
  actions: { moveCard: { concurrent: true } },
});

export const cardDraftAtom = Atom.make("");
export const titleDraftAtom = Atom.make<string | null>(null);
export const focusedCardAtom = Atom.make<string | null>(null);

export const addCardAtom = runtime.fn<void>()(
  Effect.fnUntraced(function* (_, get) {
    const title = get(cardDraftAtom).trim();

    if (title.length === 0) return;
    const crypto = yield* Crypto.Crypto;
    const id = yield* crypto.randomUUIDv4().pipe(Effect.orDie);

    yield* get.setResult(board.action.addCard, { id, title });
    get.set(cardDraftAtom, "");
  }),
);

export const renameBoardAtom = runtime.fn<void>()(
  Effect.fnUntraced(function* (_, get) {
    const title = get(titleDraftAtom)?.trim();

    if (title === undefined || title.length === 0) return;
    yield* get.setResult(board.action.rename, title);
    get.set(titleDraftAtom, null);
  }),
);

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
