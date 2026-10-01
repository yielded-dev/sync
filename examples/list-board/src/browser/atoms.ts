import { IndexedDb } from "@yielded/sync-local-indexeddb";
import { SourceAtom } from "@yielded/sync/atom";
import { Client, type ClientError, type PersistenceError } from "@yielded/sync/client";
import { Clock, Context, Effect, Layer, Option, Schedule, Schema, Stream } from "effect";
import { AsyncResult, Atom } from "effect/unstable/reactivity";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import { Socket } from "effect/unstable/socket";

import { BoardClient } from "../client.ts";
import { type CardRejected, Cards, type Lane } from "../contract.ts";
import { DemoActor } from "../demo.ts";

const page = new URL(window.location.href);

export const actorId = Option.getOrElse(
  Schema.decodeUnknownOption(DemoActor)(page.searchParams.get("actor")),
  () => "alice" as const,
);

export const boardId = Option.getOrElse(
  Schema.decodeUnknownOption(Schema.NonEmptyString)(page.searchParams.get("board")),
  () => "shared",
);

export const otherActor = actorId === "alice" ? "bob" : "alice";

export const actorHref = (actor: typeof DemoActor.Type) => {
  const url = new URL(page);

  url.searchParams.set("actor", actor);
  url.searchParams.set("board", boardId);

  return url.href;
};

const address = Cards.address(boardId);

const protocol = Layer.unwrap(
  Effect.gen(function* () {
    const { actorId } = yield* Client.CurrentActor;
    const socketUrl = new URL(`/sync/boards/${encodeURIComponent(boardId)}`, page.origin);

    socketUrl.protocol = page.protocol === "https:" ? "wss:" : "ws:";
    socketUrl.searchParams.set("actor", actorId);

    return RpcClient.layerProtocolSocket({ retryTransientErrors: false }).pipe(
      Layer.provide(RpcSerialization.layerJson),
      Layer.provide(Socket.layerWebSocket(socketUrl.href)),
      Layer.provide(Socket.layerWebSocketConstructorGlobal),
    );
  }),
);

const makeSession = Effect.gen(function* () {
  const client = yield* Client.make(BoardClient, { persistence: { mode: "persistent" } });
  const lease = yield* client.open(address);

  return { atoms: SourceAtom.make(client), lease };
});

class BoardSession extends Context.Service<BoardSession, Effect.Success<typeof makeSession>>()(
  "list-board/BoardSession",
) {
  static readonly layer = Layer.effect(this, makeSession);
}

// One registry owns the actor's socket, IndexedDB handle, and source leases.
const runtime = Atom.runtime(
  BoardSession.layer.pipe(
    Layer.provide([
      IndexedDb.layer({ namespace: "list-board-demo-v1" }),
      Client.layerRpcTransport(Cards).pipe(Layer.provide(protocol)),
    ]),
    Layer.provide(Layer.succeed(Client.CurrentActor, { actorId })),
  ),
);

const sessionAtom = runtime.atom(BoardSession);

export const boardAtom = Atom.make((get) =>
  AsyncResult.flatMap(get(sessionAtom), ({ atoms }) => get(atoms.replica(address))),
).pipe(Atom.setIdleTTL(0));

export const cardDraftAtom = Atom.make("");
export const titleDraftAtom = Atom.make<string | null>(null);
export const focusedCardAtom = Atom.make<string | null>(null);

export const addCardAtom = runtime.fn<void>()(
  Effect.fnUntraced(function* (_, get) {
    const title = get(cardDraftAtom).trim();

    if (title.length === 0) return;
    const { atoms } = yield* BoardSession;
    const id = yield* Effect.sync(() => crypto.randomUUID());

    yield* atoms.execute(address, Cards.actions.add, { id, title });
    get.set(cardDraftAtom, "");
  }),
);

export const renameBoardAtom = runtime.fn<void>()(
  Effect.fnUntraced(function* (_, get) {
    const title = get(titleDraftAtom)?.trim();

    if (title === undefined || title.length === 0) return;
    const { atoms } = yield* BoardSession;

    yield* atoms.execute(address, Cards.plugins.board.actions.rename, title);
    get.set(titleDraftAtom, null);
  }),
);

export const moveCardAtom = runtime.fn<{ readonly id: string; readonly lane: typeof Lane.Type }>()(
  Effect.fnUntraced(function* (payload) {
    const { atoms } = yield* BoardSession;

    return yield* atoms.execute(address, Cards.actions.move, payload);
  }),
  { concurrent: true },
);

export const retryChangeAtom = runtime.fn<string>()(
  Effect.fnUntraced(function* (commandId) {
    const { atoms } = yield* BoardSession;

    return yield* atoms.retry(address, commandId);
  }),
);

export const reconnectAtom = runtime.fn<void>()(
  Effect.fnUntraced(function* () {
    const { atoms } = yield* BoardSession;

    yield* atoms.recover(address);
  }),
);

export interface Peer {
  readonly actorId: string;
  readonly connectionId: string;
  readonly editingCardId: string | null;
  readonly expiresAtMillis: number;
}

// Presence is bounded, expires, and never enters the durable replica or journal.
export const peersAtom = runtime
  .atom(
    Stream.unwrap(Effect.map(BoardSession, ({ lease }) => lease.messages)).pipe(
      Stream.merge(Stream.fromSchedule(Schedule.spaced("5 seconds")).pipe(Stream.map(() => null))),
      Stream.mapEffect((frame) => Effect.map(Clock.currentTimeMillis, (now) => ({ frame, now }))),
      Stream.scan([] as ReadonlyArray<Peer>, (peers, { frame, now }) => {
        const current = peers.filter(
          (peer) => peer.expiresAtMillis > now && peer.connectionId !== frame?.connectionId,
        );

        if (frame === null || frame._tag === "MessageLeave" || frame.actorId === actorId)
          return current;
        if (frame.message.namespace !== "$source") return current;

        return [
          ...current.slice(-31),
          {
            actorId: frame.actorId,
            connectionId: frame.connectionId,
            editingCardId: frame.message.payload.editingCardId,
            expiresAtMillis: now + 25_000,
          },
        ];
      }),
    ),
    { initialValue: [] },
  )
  .pipe(Atom.setIdleTTL(0));

export const announcePresenceAtom = runtime
  .atom((get) => {
    const editingCardId = get(focusedCardAtom);
    const board = get(boardAtom);

    if (board._tag !== "Success" || board.value.connection !== "live") return Effect.void;

    return Effect.gen(function* () {
      const { lease } = yield* BoardSession;

      // Missed ephemeral announcements can be dropped; the next heartbeat replaces them.
      yield* lease
        .publishMessage({ editingCardId })
        .pipe(Effect.ignore, Effect.repeat(Schedule.spaced("8 seconds")));
    });
  })
  .pipe(Atom.setIdleTTL(0));

export type BoardError = ClientError | CardRejected | PersistenceError;

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
