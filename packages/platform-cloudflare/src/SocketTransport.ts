import type { ProtocolError } from "@yielded/sync";
import { Effect, Queue, type Scope } from "effect";
import { type RpcMessage, RpcSerialization, RpcServer } from "effect/rpc";

import { invalid } from "./SourceHub.ts";

export type Socket = WebSocket;
export type ObjectState = Pick<DurableObjectState, "storage" | "getWebSockets" | "acceptWebSocket">;

/** Host I/O only. RpcServer owns procedure dispatch and the RPC state machine. */
export interface Transport {
  readonly protocol: RpcServer.Protocol["Service"];
  readonly accept: (socket: Socket) => Effect.Effect<void, ProtocolError>;
  readonly message: (
    socket: Socket,
    data: string | ArrayBuffer,
  ) => Effect.Effect<void, ProtocolError>;
  readonly close: (socket: Socket) => Effect.Effect<void>;
}

export type Factory = Effect.Effect<
  Transport,
  never,
  RpcSerialization.RpcSerialization | Scope.Scope
>;

/** Native Cloudflare event bridge for hosts without their own Effect RPC transport. */
export const make = Effect.fn("Cloudflare.SocketTransport.make")(function* (state: ObjectState) {
  const serialization = yield* RpcSerialization.RpcSerialization;
  const disconnects = yield* Queue.make<number>();
  const clients = new Map<Socket, { id: number; parser: RpcSerialization.Parser }>();
  const sockets = new Map<number, Socket>();
  let nextId = 0;

  // RPC fibers are activation-local. The client retains the cursor and exact retry evidence.
  for (const socket of state.getWebSockets()) {
    if (socket.readyState !== WebSocket.OPEN) continue;
    yield* Effect.try({
      try: () => socket.close(1012, "RPC activation reset; reconnect and replay"),
      catch: invalid,
    }).pipe(Effect.ignore);
  }

  const close = (socket: Socket) =>
    Effect.gen(function* () {
      const client = clients.get(socket);

      if (client === undefined) return;
      clients.delete(socket);
      sockets.delete(client.id);
      yield* Queue.offer(disconnects, client.id);
    });

  let writeRequest: (
    clientId: number,
    request: RpcMessage.FromClientEncoded,
  ) => Effect.Effect<void>;

  const protocol = yield* RpcServer.Protocol.make((write) => {
    writeRequest = write;

    return Effect.succeed({
      disconnects,
      send: (clientId: number, response: RpcMessage.FromServerEncoded) =>
        Effect.gen(function* () {
          const socket = sockets.get(clientId);
          const client = socket === undefined ? undefined : clients.get(socket);

          if (socket === undefined || client === undefined) return;
          yield* Effect.try({
            try: () => {
              const encoded = client.parser.encode(response);

              if (encoded !== undefined) socket.send(encoded);
            },
            catch: invalid,
          }).pipe(
            Effect.catch(() =>
              Effect.gen(function* () {
                yield* Effect.try({
                  try: () => socket.close(1013, "RPC delivery failed"),
                  catch: invalid,
                }).pipe(Effect.ignore);
                yield* close(socket);
              }),
            ),
          );
        }),
      end: (clientId: number) =>
        Effect.gen(function* () {
          const socket = sockets.get(clientId);

          if (socket === undefined) return;
          yield* Effect.try({ try: () => socket.close(1000), catch: invalid }).pipe(Effect.ignore);
          yield* close(socket);
        }),
      clientIds: Effect.sync(() => new Set(sockets.keys())),
      initialMessage: Effect.succeedNone,
      supportsAck: true,
      supportsTransferables: false,
      supportsSpanPropagation: true,
      supportsNotifications: true,
      codecFor: serialization.codecFor,
    });
  });

  return {
    protocol,
    accept: (socket) =>
      Effect.try({
        try: () => {
          state.acceptWebSocket(socket);
          const id = nextId++;

          clients.set(socket, { id, parser: serialization.makeUnsafe() });
          sockets.set(id, socket);
        },
        catch: invalid,
      }),
    message: (socket, data) =>
      Effect.gen(function* () {
        const client = clients.get(socket);

        if (client === undefined) return;

        const messages = yield* Effect.try({
          try: () => client.parser.decode(typeof data === "string" ? data : new Uint8Array(data)),
          catch: invalid,
        });

        for (const message of messages) {
          // RpcSerialization supplies envelopes; RpcServer validates their procedure codecs.
          yield* writeRequest(client.id, message as RpcMessage.FromClientEncoded);
        }
      }),
    close,
  } satisfies Transport;
});
