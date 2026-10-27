import { ProtocolError, type Source } from "@yielded/sync";
import {
  Cloudflare,
  type DurableObject as SyncDurableObject,
} from "@yielded/sync-platform-cloudflare";
import type { ServerCrypto, SourceStorage, StorageError } from "@yielded/sync/server";
import type { Container } from "alchemy/Cloudflare";
import {
  DurableObject,
  DurableObjectState,
  type DurableObjectServices,
  Request as NativeRequest,
  type WebSocket,
  type Worker,
} from "alchemy/Cloudflare/Workers";
import { Context, Effect, Layer, Scope } from "effect";
import { type HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";

/** Build in an object's runtime Effect, after Alchemy has supplied native storage. */
export const layer = <DeliveryR = never, E = never, R = never>(
  options: Cloudflare.Options<DeliveryR, E, R> = {},
): Layer.Layer<SourceStorage | ServerCrypto, StorageError | E, DurableObjectState | R> =>
  Layer.unwrap(
    Effect.map(DurableObjectState, (state) => Cloudflare.layer(state.raw.storage, options)),
  );

type Handlers = {
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

/** Select Alchemy's logical object name without opening storage during planning. */
export const AlchemyServices =
  (name: string) =>
  <
    S extends Source.Spec,
    R extends DurableObjectServices | SourceStorage | ServerCrypto,
    DeliveryR extends DurableObjectServices | SourceStorage | ServerCrypto = never,
    E = never,
    PlatformR extends DurableObjectServices = never,
  >(
    declaration: SyncDurableObject.Definition<S, R, DeliveryR, E, PlatformR>,
  ): Effect.Effect<
    DurableObject<Handlers>,
    never,
    | Worker
    | Extract<
        Exclude<R | DeliveryR, SourceStorage | ServerCrypto> | PlatformR,
        Container.Application<unknown>
      >
  > & {
    new (_: never): Handlers;
    handler<AuthR>(
      options: Cloudflare.HandlerOptions<S, AuthR>,
    ): Effect.Effect<
      Effect.Effect<HttpServerResponse.HttpServerResponse, never, NativeRequest | AuthR>,
      never,
      | Worker
      | Extract<
          Exclude<R | DeliveryR, SourceStorage | ServerCrypto> | PlatformR,
          Container.Application<unknown>
        >
    >;
  } => {
    const { server: definition, options } = declaration;

    const object = DurableObject<Handlers>()(
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

    return Object.assign(object, {
      handler: <AuthR>(options: Cloudflare.HandlerOptions<S, AuthR>) =>
        Effect.map(object, (namespace) => {
          const handle = Cloudflare.handler(definition.contract, {
            ...options,
            fetch: (name, request) =>
              namespace
                .getByName(name)
                .fetch(HttpServerRequest.fromWeb(request))
                .pipe(
                  Effect.map(HttpServerResponse.toWeb),
                  Effect.mapError(() =>
                    ProtocolError.make({ reason: "Unavailable", message: "Source unavailable" }),
                  ),
                ),
          });

          return Effect.flatMap(NativeRequest, handle).pipe(Effect.map(response));
        }),
    });
  };

/** Prepare the native request for the application's authenticated Alchemy route. */
interface RequestOptions<Kind extends string> {
  readonly request: HttpServerRequest.HttpServerRequest;
  readonly address: { readonly kind: Kind; readonly id: string };
  readonly identity: Cloudflare.Identity;
}

export const prepareRequest: <S extends Source.Spec>(
  contract: Source.Definition<S>,
  input: RequestOptions<S["kind"]>,
) => Effect.Effect<
  HttpServerRequest.HttpServerRequest,
  ProtocolError | HttpServerError.RequestError
> = Effect.fn("AlchemyServices.prepareRequest")(function* <S extends Source.Spec>(
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
