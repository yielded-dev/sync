import type { Action, ProtocolError, Source } from "@yielded/sync";
import { Cloudflare, SqliteStorage } from "@yielded/sync-platform-cloudflare";
import type { Server, SourceStorage, StorageError } from "@yielded/sync/server";
import { Context, Effect, Layer } from "effect";
import {
  DurableObjectRpcWebSocket,
  DurableObjectState,
  DurableObjectWebSocket,
  Worker,
} from "effect-cf";
import { RpcServer } from "effect/rpc";

export type Options<R = never> = Cloudflare.Options<R>;
export type Identity = Cloudflare.Identity;

export type ClientOptions<Kind extends string, E = never, R = never> = Cloudflare.ClientOptions<
  Kind,
  E,
  R
>;

export type Command<A extends Action.Definition> = Cloudflare.Command<A>;
export type RemoteClient<S extends Source.Spec> = Cloudflare.RemoteClient<S>;
export const prepareRequest: typeof Cloudflare.prepareRequest = Cloudflare.prepareRequest;
export const errorResponse = Cloudflare.errorResponse;
export const client: typeof Cloudflare.client = Cloudflare.client;

/** Select native SQLite storage for this object's source. */
export const sqlite = (
  options: SqliteStorage.Options,
): Layer.Layer<SourceStorage, StorageError, DurableObjectState.DurableObjectState> =>
  Layer.unwrap(
    Effect.map(DurableObjectState.DurableObjectState, (state) =>
      SqliteStorage.layer(state.raw.storage, options),
    ),
  );

export interface Host<R> {
  readonly layer: Layer.Layer<
    Cloudflare.Handlers,
    StorageError,
    DurableObjectState.DurableObjectState | SourceStorage | R
  >;
  readonly handlers: {
    readonly fetch: Effect.Effect<
      Response,
      ProtocolError,
      Worker.NativeRequest | Cloudflare.Handlers
    >;
    readonly webSocketMessage: (
      socket: DurableObjectWebSocket.DurableWebSocket,
      message: string | ArrayBuffer,
    ) => Effect.Effect<void, never, Cloudflare.Handlers>;
    readonly webSocketClose: (
      socket: DurableObjectWebSocket.DurableWebSocket,
    ) => Effect.Effect<void, never, Cloudflare.Handlers>;
    readonly webSocketError: (
      socket: DurableObjectWebSocket.DurableWebSocket,
    ) => Effect.Effect<void, never, Cloudflare.Handlers>;
    readonly alarm: () => Effect.Effect<void, ProtocolError | StorageError, Cloudflare.Handlers>;
  };
}

/** Pass the returned Layer and handlers to effect-cf's DurableObject.make. */
export const make = <S extends Source.Spec, R, DeliveryR = never>(
  definition: Server.Definition<S, R>,
  options: Options<DeliveryR>,
): Host<R | DeliveryR> => {
  const runtime = Context.Service<Cloudflare.Handlers>(
    `@yielded/sync-platform-effect-cf/${definition.contract.kind}`,
  );

  const layer = Layer.effect(
    runtime,
    Effect.flatMap(DurableObjectState.DurableObjectState, (state) => {
      const transport: Cloudflare.TransportFactory = Effect.gen(function* () {
        const context = yield* Layer.build(
          DurableObjectRpcWebSocket.layer({ heartbeat: "passthrough" }),
        ).pipe(Effect.provideService(DurableObjectState.DurableObjectState, state));

        const socket = Context.get(context, DurableObjectRpcWebSocket.DurableObjectRpcWebSocket);

        return {
          protocol: Context.get(context, RpcServer.Protocol),
          accept: (raw) => socket.accept(DurableObjectWebSocket.fromWebSocket(raw)),
          message: (raw, message) =>
            socket.message(DurableObjectWebSocket.fromWebSocket(raw), message),
          close: (raw) => socket.close(DurableObjectWebSocket.fromWebSocket(raw)),
        } satisfies Cloudflare.Transport;
      });

      return Cloudflare.make(definition, state.raw, options, transport);
    }),
  );

  const handlers = {
    fetch: Effect.gen(function* () {
      const request = yield* Worker.NativeRequest;

      return yield* (yield* runtime).fetch(request);
    }),
    webSocketMessage: (
      socket: DurableObjectWebSocket.DurableWebSocket,
      message: string | ArrayBuffer,
    ) => Effect.flatMap(runtime, (source) => source.webSocketMessage(socket.raw, message)),
    webSocketClose: (socket: DurableObjectWebSocket.DurableWebSocket) =>
      Effect.flatMap(runtime, (source) => source.webSocketClose(socket.raw)),
    webSocketError: (socket: DurableObjectWebSocket.DurableWebSocket) =>
      Effect.flatMap(runtime, (source) => source.webSocketError(socket.raw)),
    alarm: () => Effect.flatMap(runtime, (source) => source.alarm),
  };

  return { layer, handlers };
};
