import type { Action, Source } from "@yielded/sync";
import { Cloudflare } from "@yielded/sync-platform-cloudflare";
import type { Server, ServerCrypto, SourceStorage, StorageError } from "@yielded/sync/server";
import { Context, Effect, Layer } from "effect";
import {
  DurableObject,
  DurableObjectDefinition,
  DurableObjectRpcWebSocket,
  DurableObjectState,
  DurableObjectWebSocket,
  Worker,
} from "effect-cf";
import { RpcServer } from "effect/rpc";

export type Options<DeliveryR = never, E = never, R = never> = Cloudflare.Options<DeliveryR, E, R>;
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

/** Default platform services for custom composition in an effect-cf object. */
export const layer = <DeliveryR = never, E = never, R = never>(
  options: Options<DeliveryR, E, R> = {},
): Layer.Layer<
  SourceStorage | ServerCrypto,
  StorageError | E,
  DurableObjectState.DurableObjectState | R
> =>
  Layer.unwrap(
    Effect.map(DurableObjectState.DurableObjectState, (state) =>
      Cloudflare.layer(state.raw.storage, options),
    ),
  );

/** Construct a native Durable Object with per-instance platform services. */
export const make = <
  S extends Source.Spec,
  R extends DurableObject.RuntimeContext<SourceStorage | ServerCrypto>,
  DeliveryR extends DurableObject.RuntimeContext<SourceStorage | ServerCrypto> = never,
  E = never,
  PlatformR extends DurableObject.RuntimeContext<never> = never,
>(
  definition: Server.Definition<S, R>,
  options: Options<DeliveryR, E, PlatformR> = {},
) => {
  const runtime = Context.Service<Cloudflare.Handlers>(
    `@yielded/sync-platform-effect-cf/${definition.contract.kind}`,
  );

  const live = Layer.effect(
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

  return Object.assign(DurableObject.make(live, handlers), {
    binding: <const Name extends string>(name: Name) => {
      const namespace = DurableObjectDefinition.make(name, {});

      return {
        layer: namespace.layer({ binding: name }),
        getByName: namespace.getByName,
        fetch: namespace.fetch,
      };
    },
  });
};
