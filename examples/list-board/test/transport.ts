import { ProtocolError } from "@yielded/sync";
import { Client } from "@yielded/sync/client";
import { SELF } from "cloudflare:test";
import { Deferred, Effect, Layer, Queue, type Schema, Stream } from "effect";
import { RpcClient, RpcSerialization } from "effect/rpc";
import { Socket } from "effect/socket";

export const socketProtocol = (onConnect?: (socket: WebSocket) => void) =>
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

interface Request {
  readonly command: Client.EncodedCommand;
  readonly send: Effect.Effect<Schema.Json, ProtocolError>;
  readonly reply: (value: Schema.Json) => Effect.Effect<boolean>;
  readonly drop: Effect.Effect<boolean>;
}

const disconnected = () =>
  ProtocolError.make({ reason: "Unavailable", message: "Test disconnected delivery" });

/** Gates real RPC traffic; bootstrap frames pass through so each client can become ready. */
export const controlled = Effect.fnUntraced(function* (remote: Client.Transport) {
  const requests = yield* Queue.make<Request>({ capacity: 8 });
  const lookups = yield* Queue.make<Request>({ capacity: 8 });
  const deliveries = yield* Queue.make<Effect.Effect<boolean>>({ capacity: 8 });

  const connections = yield* Queue.make<{ readonly disconnect: Effect.Effect<boolean> }>({
    capacity: 8,
  });

  const commands: Array<Client.EncodedCommand> = [];

  yield* Effect.addFinalizer(() =>
    Effect.all(
      [
        Queue.shutdown(requests),
        Queue.shutdown(lookups),
        Queue.shutdown(deliveries),
        Queue.shutdown(connections),
      ],
      { discard: true },
    ),
  );

  const call = (queue: Queue.Queue<Request>, operation: Client.Transport["execute"]) =>
    Effect.fnUntraced(function* (command: Client.EncodedCommand) {
      const response = yield* Deferred.make<Schema.Json, ProtocolError>();

      yield* Queue.offer(queue, {
        command,
        send: operation(command),
        reply: (value) => Deferred.succeed(response, value),
        drop: Deferred.fail(response, disconnected()),
      });

      return yield* Deferred.await(response);
    });

  const execute = call(requests, remote.execute);

  const transport: Client.Transport = {
    ...remote,
    execute: (command) =>
      Effect.suspend(() => {
        commands.push(command);

        return execute(command);
      }),
    result: call(lookups, remote.result),
    subscribe: (request) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const stopped = yield* Deferred.make<never, ProtocolError>();
          let bootstrap = true;

          yield* Queue.offer(connections, { disconnect: Deferred.fail(stopped, disconnected()) });

          return remote.subscribe(request).pipe(
            Stream.mapEffect(
              Effect.fnUntraced(function* (frame) {
                if (bootstrap) {
                  bootstrap = false;

                  return frame;
                }
                const delivered = yield* Deferred.make<void>();

                yield* Queue.offer(deliveries, Deferred.succeed(delivered, undefined));
                yield* Deferred.await(delivered);

                return frame;
              }),
            ),
            Stream.interruptWhen(Deferred.await(stopped)),
          );
        }),
      ),
  };

  return {
    transport,
    commands,
    request: Queue.take(requests),
    lookup: Queue.take(lookups),
    connection: Queue.take(connections),
    deliver: Queue.take(deliveries).pipe(Effect.flatten),
  };
});
