import type { ProtocolError, Source } from "@yielded/sync";
import { Cloudflare } from "@yielded/sync-platform-cloudflare";
import type { Server } from "@yielded/sync/server";
import { DurableObjectState, type WebSocket } from "alchemy/Cloudflare/Workers";
import { Context, Effect, Scope } from "effect";
import { type HttpServerError, HttpServerRequest, HttpServerResponse } from "effect/http";

export type Options<R = never> = Cloudflare.Options<R>;
export type Identity = Cloudflare.Identity;

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

/** Supplies Alchemy's construction/runtime Effects to its native DurableObject API. */
export const make: <S extends Source.Spec, R, DeliveryR = never>(
  definition: Server.Definition<S, R>,
  options: Options<DeliveryR>,
) => Effect.Effect<
  Effect.Effect<Handlers, never, Scope.Scope>,
  never,
  DurableObjectState | R | DeliveryR
> = Effect.fn("AlchemyCf.make")(function* <S extends Source.Spec, R, DeliveryR = never>(
  definition: Server.Definition<S, R>,
  options: Options<DeliveryR>,
): Effect.fn.Return<
  Effect.Effect<Handlers, never, Scope.Scope>,
  never,
  DurableObjectState | R | DeliveryR
> {
  const state = yield* DurableObjectState;
  const services = yield* Effect.context<R | DeliveryR>();

  return Effect.gen(function* () {
    const scope = yield* Scope.Scope;

    // Alchemy constructors have no typed failure channel; initialization failure aborts activation.
    const source = yield* Cloudflare.make(definition, state.raw, options).pipe(
      Effect.provideContext(Context.add(services, Scope.Scope, scope)),
      Effect.orDie,
    );

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.toWeb(yield* HttpServerRequest.HttpServerRequest);

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
      alarm: () => source.alarm().pipe(Effect.orDie),
    };
  });
});

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
