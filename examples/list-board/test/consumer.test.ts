import { ProtocolError } from "@yielded/sync";
import { Client, ClientError } from "@yielded/sync/client";
import { SELF, env, evictDurableObject, reset } from "cloudflare:test";
import { Context, Effect, Fiber, Layer, Schema, Stream } from "effect";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import { Socket } from "effect/unstable/socket";
import { afterEach, expect, it } from "vite-plus/test";

import { BoardClient } from "../src/client.ts";
import { CardRejected, Cards } from "../src/contract.ts";
import type { BoardObject } from "../src/worker.ts";

const bindings = env as { BOARDS: DurableObjectNamespace<BoardObject> };
const address = Cards.address("shared");
const stub = () => bindings.BOARDS.getByName("board:shared");

const unavailable = () =>
  ProtocolError.make({ reason: "Unavailable", message: "Response lost after commit" });

const socketProtocol = (onConnect?: (socket: WebSocket) => void) =>
  RpcClient.layerProtocolSocket({ retryTransientErrors: false }).pipe(
    Layer.provide(
      Layer.effect(
        Socket.Socket,
        Socket.fromWebSocket(
          Effect.acquireRelease(
            Effect.gen(function* () {
              const { actorId } = yield* Client.CurrentActor;

              const response = yield* Effect.promise(() =>
                SELF.fetch("https://example.test/sync/boards/shared", {
                  headers: {
                    upgrade: "websocket",
                    authorization: `Bearer ${actorId}-local`,
                  },
                }),
              );

              const socket = response.webSocket;

              if (socket === null) return yield* Effect.die("Upgrade failed");
              socket.accept();
              onConnect?.(socket);

              return socket;
            }),
            (socket) => Effect.sync(() => socket.close(1000)),
          ),
        ),
      ),
    ),
    Layer.provide(RpcSerialization.layerJson),
  );

afterEach(() => reset());

it("runs two public clients against a durable board with exact retry, rollback, presence, and replay", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const services = yield* Layer.build(
          Client.layerRpcTransport(Cards).pipe(
            Layer.provide(socketProtocol()),
            Layer.provideMerge(Layer.succeed(Client.CurrentActor, { actorId: "alice" })),
          ),
        );

        const aliceTransport = Context.get(services, Client.Transport);

        let loseResponse = true;

        const alice = yield* Client.make(BoardClient, {
          persistence: { mode: "volatile" },
        }).pipe(
          Effect.provideService(Client.Transport, {
            ...aliceTransport,
            execute: (command) =>
              aliceTransport.execute(command).pipe(
                Effect.flatMap((outcome) => {
                  if (!loseResponse) return Effect.succeed(outcome);
                  loseResponse = false;

                  return Effect.fail(unavailable());
                }),
              ),
          }),
          Effect.provideContext(services),
        );

        let bobSocket: WebSocket | undefined;

        const bobServices = yield* Layer.build(
          Client.layerRpcTransport(Cards).pipe(
            Layer.provide(
              socketProtocol((socket) => {
                bobSocket = socket;
              }),
            ),
            Layer.provideMerge(Layer.succeed(Client.CurrentActor, { actorId: "bob" })),
          ),
        );

        const bob = yield* Client.make(BoardClient, {
          persistence: { mode: "volatile" },
        }).pipe(Effect.provideContext(bobServices));

        const aliceLease = yield* alice.open(address);
        const bobLease = yield* bob.open(address);

        yield* aliceLease.ready;
        yield* bobLease.ready;

        const first = yield* Effect.result(
          aliceLease.execute(Cards.actions.add, { id: "one", title: "First" }),
        );

        if (
          first._tag !== "Failure" ||
          !Schema.is(ClientError)(first.failure) ||
          first.failure.commandId === undefined
        ) {
          return yield* Effect.die("Expected a retained command identity");
        }
        yield* bobLease.changes.pipe(
          Stream.filter((state) => state.value?.source.cards.length === 1),
          Stream.take(1),
          Stream.runDrain,
        );
        yield* bobLease.execute(Cards.actions.add, { id: "two", title: "Second" });
        yield* Effect.promise(() => evictDurableObject(stub()));
        if (bobSocket === undefined) return yield* Effect.die("Bob did not connect");
        // Wake the new activation, then let native RPC reset and reconnect its lost streams.
        bobSocket.send(JSON.stringify({ _tag: "Ping" }));
        yield* Effect.forEach(
          [aliceLease, bobLease],
          (lease) =>
            lease.changes.pipe(
              Stream.filter((state) => state.connection === "recovering"),
              Stream.take(1),
              Stream.runDrain,
              Effect.andThen(lease.ready),
            ),
          { concurrency: 2, discard: true },
        );
        expect(yield* aliceLease.retry(first.failure.commandId)).toEqual({ id: "one", count: 1 });
        yield* bobLease.execute(Cards.plugins.board.actions.rename, "Planning");
        yield* aliceLease.changes.pipe(
          Stream.filter((state) => state.value?.plugins.board.title === "Planning"),
          Stream.take(1),
          Stream.runDrain,
        );

        const rejected = yield* Effect.result(
          bobLease.execute(Cards.actions.add, { id: "one", title: "Duplicate" }),
        );

        expect(rejected._tag).toBe("Failure");
        if (rejected._tag === "Failure")
          expect(rejected.failure).toEqual(CardRejected.make({ reason: "Duplicate" }));
        expect((yield* bobLease.read).value?.source.cards.map((card) => card.id)).toEqual([
          "one",
          "two",
        ]);

        const received = yield* bobLease.messages.pipe(
          Stream.filter((frame) => frame._tag === "Message"),
          Stream.take(1),
          Stream.runCollect,
          Effect.forkScoped,
        );

        yield* Effect.yieldNow;
        const beforePresence = (yield* bobLease.read).authoritative?.position.cursor;

        yield* aliceLease.publishMessage({ editingCardId: "one" });
        expect((yield* Fiber.join(received))[0]).toMatchObject({
          actorId: "alice",
          message: { namespace: "$source", payload: { editingCardId: "one" } },
        });
        expect((yield* bobLease.read).authoritative?.position.cursor).toBe(beforePresence);

        if (bobSocket === undefined) return yield* Effect.die("Bob did not connect");
        bobSocket.close(1012);
        yield* bobLease.changes.pipe(
          Stream.filter((state) => state.connection === "recovering"),
          Stream.take(1),
          Stream.runDrain,
        );
        yield* aliceLease.execute(Cards.actions.move, { id: "one", lane: "doing" });
        yield* bobLease.changes.pipe(
          Stream.filter((state) => state.value?.source.cards[0]?.lane === "doing"),
          Stream.take(1),
          Stream.runDrain,
        );
        expect((yield* bobLease.read).value).toEqual((yield* aliceLease.read).value);
      }),
    ),
  );
}, 15_000);
