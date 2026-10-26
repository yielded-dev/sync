import type { Action, ProtocolError, Source } from "@yielded/sync";
import { Cloudflare } from "@yielded/sync-platform-cloudflare";
import type { Server, ServerCrypto, SourceStorage, StorageError } from "@yielded/sync/server";
import type { Container } from "alchemy/Cloudflare";
import {
  DurableObject,
  DurableObjectState,
  type DurableObjectServices,
  type WebSocket,
  type Worker,
} from "alchemy/Cloudflare/Workers";
import { Context, Effect, Layer, Scope } from "effect";
import { type HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";

export type Options<DeliveryR = never, E = never, R = never> = Cloudflare.Options<DeliveryR, E, R>;
export type Identity = Cloudflare.Identity;

export type ClientOptions<Kind extends string, E = never, R = never> = Cloudflare.ClientOptions<
  Kind,
  E,
  R
>;

export type Command<A extends Action.Definition> = Cloudflare.Command<A>;
export type RemoteClient<S extends Source.Spec> = Cloudflare.RemoteClient<S>;
export const client: typeof Cloudflare.client = Cloudflare.client;

/** Build in an object's runtime Effect, after Alchemy has supplied native storage. */
export const layer = <DeliveryR = never, E = never, R = never>(
  options: Options<DeliveryR, E, R> = {},
): Layer.Layer<SourceStorage | ServerCrypto, StorageError | E, DurableObjectState | R> =>
  Layer.unwrap(
    Effect.map(DurableObjectState, (state) => Cloudflare.layer(state.raw.storage, options)),
  );

export type Handlers = {
  readonly fetch: Effect.Effect<
    HttpServerResponse.HttpServerResponse,
    HttpServerError.RequestError,
    HttpServerRequest.HttpServerRequest
  >;
  readonly webSocketMessage: (
    socket: WebSocket,
    message: string | ArrayBuffer,
  ) => Effect.Effect<void>;
  readonly webSocketClose: (socket: WebSocket) => Effect.Effect<void>;
  readonly webSocketError: (socket: WebSocket) => Effect.Effect<void>;
  readonly alarm: () => Effect.Effect<void>;
};

/** Native responses, especially WebSocket upgrades, must pass through intact. */
const response = (value: Response) => HttpServerResponse.raw(value, { status: value.status });

/** Declare an Alchemy Durable Object; acquire platform services only at runtime. */
export const make = <
  S extends Source.Spec,
  R extends DurableObjectServices | SourceStorage | ServerCrypto,
  DeliveryR extends DurableObjectServices | SourceStorage | ServerCrypto = never,
  E = never,
  PlatformR extends DurableObjectServices = never,
>(
  name: string,
  definition: Server.Definition<S, R>,
  options: Options<DeliveryR, E, PlatformR> = {},
): Effect.Effect<
  DurableObject<Handlers>,
  never,
  | Worker
  | Extract<
      Exclude<R | DeliveryR, SourceStorage | ServerCrypto> | PlatformR,
      Container.Application<unknown>
    >
> & { new (_: never): Handlers } =>
  DurableObject<Handlers>()(
    name,
    Effect.map(
      Effect.all([
        DurableObjectState,
        Effect.context<Exclude<R | DeliveryR, SourceStorage | ServerCrypto> | PlatformR>(),
      ]),
      ([state, services]) =>
        Effect.gen(function* (): Effect.fn.Return<Handlers, never, Scope.Scope> {
          const scope = yield* Scope.Scope;

          // Alchemy constructors have no typed failure channel; initialization failure aborts activation.
          const source = yield* Cloudflare.make(definition, state.raw, options).pipe(
            Effect.provideContext(Context.add(services, Scope.Scope, scope)),
            Effect.orDie,
          );

          return {
            fetch: Effect.gen(function* () {
              const request = yield* HttpServerRequest.toWeb(
                yield* HttpServerRequest.HttpServerRequest,
              );

              return response(
                yield* source
                  .fetch(request)
                  .pipe(
                    Effect.catchTag("ProtocolError", (error) =>
                      Effect.succeed(Cloudflare.errorResponse(error)),
                    ),
                  ),
              );
            }),
            webSocketMessage: (socket: WebSocket, message: string | ArrayBuffer) =>
              source.webSocketMessage(socket.ws, message),
            webSocketClose: (socket: WebSocket) => source.webSocketClose(socket.ws),
            webSocketError: (socket: WebSocket) => source.webSocketError(socket.ws),
            alarm: () => source.alarm.pipe(Effect.orDie),
          };
        }),
    ),
  );

/** Prepare the native request for the application's authenticated Alchemy route. */
export interface RequestOptions<Kind extends string> {
  readonly request: HttpServerRequest.HttpServerRequest;
  readonly address: { readonly kind: Kind; readonly id: string };
  readonly identity: Identity;
}

export const prepareRequest: <S extends Source.Spec>(
  contract: Source.Definition<S>,
  input: RequestOptions<S["kind"]>,
) => Effect.Effect<
  HttpServerRequest.HttpServerRequest,
  ProtocolError | HttpServerError.RequestError
> = Effect.fn("AlchemyCf.prepareRequest")(function* <S extends Source.Spec>(
  contract: Source.Definition<S>,
  input: RequestOptions<S["kind"]>,
): Effect.fn.Return<
  HttpServerRequest.HttpServerRequest,
  ProtocolError | HttpServerError.RequestError
> {
  const request = yield* HttpServerRequest.toWeb(input.request);

  return HttpServerRequest.fromWeb(
    yield* Cloudflare.prepareRequest(contract, { ...input, request }),
  );
});

export const errorResponse = (error: Parameters<typeof Cloudflare.errorResponse>[0]) =>
  response(Cloudflare.errorResponse(error));
