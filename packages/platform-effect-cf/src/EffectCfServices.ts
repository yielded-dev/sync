import { ProtocolError, type Source } from "@yielded/sync";
import {
  Cloudflare,
  type DurableObject as SyncDurableObject,
} from "@yielded/sync-platform-cloudflare";
import type { ServerCrypto, SourceStorage, StorageError } from "@yielded/sync/server";
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

/** Default platform services for custom composition in an effect-cf object. */
export const layer = <DeliveryR = never, E = never, R = never>(
  options: Cloudflare.Options<DeliveryR, E, R> = {},
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

/** Construct an effect-cf object at application composition; acquire services per instance. */
export const EffectCfServices = <
  S extends Source.Spec,
  R extends DurableObject.RuntimeContext<SourceStorage | ServerCrypto>,
  DeliveryR extends DurableObject.RuntimeContext<SourceStorage | ServerCrypto> = never,
  E = never,
  PlatformR extends DurableObject.RuntimeContext<never> = never,
>(
  declaration: SyncDurableObject.Definition<S, R, DeliveryR, E, PlatformR>,
) => {
  const { server: definition, options } = declaration;

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
        handler: <R>(options: Cloudflare.HandlerOptions<S, R>) => {
          const handle = Cloudflare.handler(definition.contract, {
            ...options,
            fetch: (name, request) =>
              Effect.flatMap(namespace.getByName(name), (stub) =>
                namespace.fetch(stub, request),
              ).pipe(
                Effect.mapError(() =>
                  ProtocolError.make({ reason: "Unavailable", message: "Source unavailable" }),
                ),
              ),
          });

          return Effect.flatMap(Worker.NativeRequest, handle);
        },
      };
    },
  });
};
