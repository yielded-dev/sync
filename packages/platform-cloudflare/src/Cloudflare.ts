import { type ProtocolError, SourceAddress, type Source } from "@yielded/sync";
import { Server, ServerCrypto, StorageError } from "@yielded/sync/server";
import { DateTime, Effect, Layer, Schema, type Scope } from "effect";
import { Hex } from "effect/encoding";
import { HttpServerRequest, HttpServerResponse } from "effect/http";
import { RpcSerialization, RpcServer } from "effect/rpc";

import * as RpcHost from "./RpcHost.ts";
import * as SocketTransport from "./SocketTransport.ts";
import * as SourceHub from "./SourceHub.ts";
import { Attachment, invalid, unavailable } from "./SourceHub.ts";
import * as SqliteStorage from "./SqliteStorage.ts";

export type {
  ObjectState,
  Socket,
  Transport,
  Factory as TransportFactory,
} from "./SocketTransport.ts";

export type { Options } from "./SourceHub.ts";

const CryptoLive = Layer.succeed(ServerCrypto, {
  generation: Effect.sync(() => crypto.randomUUID()),
  sha256: (text) =>
    Effect.tryPromise({
      try: async () =>
        Hex.encode(
          new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text))),
        ),
      catch: (cause) => StorageError.make({ message: "SHA-256 failed", cause }),
    }),
});

const decode = <S extends Schema.Codec<unknown, unknown>>(schema: S, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(invalid));

export interface Handlers {
  readonly fetch: (request: Request) => Effect.Effect<Response, ProtocolError>;
  readonly webSocketMessage: (
    socket: SocketTransport.Socket,
    message: string | ArrayBuffer,
  ) => Effect.Effect<void>;
  readonly webSocketClose: (socket: SocketTransport.Socket) => Effect.Effect<void>;
  readonly webSocketError: (socket: SocketTransport.Socket) => Effect.Effect<void>;
  readonly alarm: () => Effect.Effect<void, ProtocolError | StorageError>;
}

/** Acquire once per native object. RPC fibers live in the instance scope; source operations are scoped separately. */
export const make = Effect.fn("Cloudflare.make")(function* <
  S extends Source.Spec,
  R,
  DeliveryR = never,
>(
  definition: Server.Definition<S, R>,
  state: SocketTransport.ObjectState,
  options: SourceHub.Options<DeliveryR>,
  transportFactory?: SocketTransport.Factory,
): Effect.fn.Return<Handlers, StorageError, Scope.Scope | R | DeliveryR> {
  for (const limit of [
    options.replay.maxEvents,
    options.replay.maxBatchBytes,
    options.sockets.maxConnections,
    options.sockets.bufferSize,
    options.maxQueuedTurns ?? 128,
    options.turnTimeoutMillis ?? 5_000,
    ...(options.outbox === undefined
      ? []
      : [options.outbox.maxPending, options.outbox.batchSize, options.outbox.retryMillis]),
  ]) {
    if (!Number.isSafeInteger(limit) || limit <= 0)
      throw new RangeError("Cloudflare source limits must be positive safe integers");
  }

  const context = yield* Layer.build(
    Layer.mergeAll(
      CryptoLive,
      SqliteStorage.layer(state.storage, {
        namespace: options.storageNamespace,
        replayWindow: options.replay.maxEvents,
        maxPendingOutbox: options.outbox?.maxPending ?? 0,
      }),
    ),
  );

  const closeNative = (socket: SocketTransport.Socket, code: number, reason: string) =>
    Effect.try({ try: () => socket.close(code, reason), catch: unavailable }).pipe(Effect.ignore);

  const readAttachment = (socket: SocketTransport.Socket) =>
    Effect.try({ try: () => socket.deserializeAttachment(), catch: invalid }).pipe(
      Effect.flatMap((value) => decode(Attachment, value)),
      Effect.map((attachment): SourceHub.Connection => ({ ...attachment, socket })),
    );

  // Fence expired identities before a native transport restores or resets its RPC sockets.
  const now = DateTime.toEpochMillis(yield* DateTime.now);

  for (const socket of state.getWebSockets()) {
    yield* readAttachment(socket).pipe(
      Effect.flatMap((attachment) =>
        attachment.session.expiresAtMillis <= now
          ? closeNative(socket, 4403, "Session expired")
          : Effect.void,
      ),
      Effect.catch(() => closeNative(socket, 4403, "Invalid source session")),
    );
  }

  const serialization = RpcHost.serialization(options.replay.maxBatchBytes);

  const transport = yield* (transportFactory ?? SocketTransport.make(state)).pipe(
    Effect.provideService(RpcSerialization.RpcSerialization, serialization),
  );

  const close = (socket: SocketTransport.Socket, code: number, reason: string) =>
    closeNative(socket, code, reason).pipe(Effect.andThen(transport.close(socket)));

  const hub = yield* SourceHub.make(definition, state, options, close).pipe(
    Effect.provideContext(context),
  );

  const rpc = RpcHost.handlers(hub);
  const handlers = yield* rpc.services;

  yield* RpcServer.make(rpc.group).pipe(
    Effect.provideContext(handlers),
    Effect.provideService(RpcServer.Protocol, RpcHost.withEventLifetime(transport.protocol)),
    Effect.forkScoped,
  );

  const http = yield* RpcServer.toHttpEffect(rpc.group).pipe(
    Effect.provideContext(handlers),
    Effect.provideService(RpcSerialization.RpcSerialization, serialization),
  );

  const timed = <A, E, Requirements>(effect: Effect.Effect<A, E, Requirements>) =>
    effect.pipe(
      Effect.timeoutOrElse({ duration: options.turnTimeoutMillis ?? 5_000, orElse: unavailable }),
    );

  const fetch = Effect.fn("Cloudflare.fetch")(function* (request: Request) {
    const address = yield* decode(
      Schema.fromJsonString(SourceAddress),
      request.headers.get("x-yielded-address"),
    );

    const session = yield* decode(
      Schema.fromJsonString(Server.Session),
      request.headers.get("x-yielded-session"),
    );

    const connection: SourceHub.Connection = { address, session };

    if (request.headers.get("upgrade")?.toLowerCase() === "websocket") {
      return yield* hub.run(connection, address, (runtime) =>
        Effect.gen(function* () {
          yield* runtime.checkSubscription(session);
          if (
            state.getWebSockets().filter((socket) => socket.readyState === WebSocket.OPEN).length >=
            options.sockets.maxConnections
          )
            return new Response("Source connection capacity reached", { status: 503 });

          const pair = yield* Effect.try({
            try: () => {
              const pair = new WebSocketPair();

              pair[1].serializeAttachment({ session, address } satisfies typeof Attachment.Type);

              return pair;
            },
            catch: unavailable,
          });

          yield* transport.accept(pair[1]);

          return new Response(null, { status: 101, webSocket: pair[0] });
        }),
      );
    }
    if (request.method !== "POST")
      return new Response("Expected POST or WebSocket", { status: 405 });
    const body = yield* readBody(request, options.replay.maxBatchBytes);

    const boundedRequest = new Request(request.url, {
      method: "POST",
      headers: request.headers,
      body,
    });

    const result = yield* http.pipe(
      Effect.provideService(
        HttpServerRequest.HttpServerRequest,
        HttpServerRequest.fromWeb(boundedRequest),
      ),
      Effect.provideService(RpcHost.CurrentConnection, connection),
      Effect.scoped,
    );

    return HttpServerResponse.toWeb(result);
  });

  const webSocketMessage = (socket: SocketTransport.Socket, message: string | ArrayBuffer) =>
    timed(
      Effect.gen(function* () {
        const connection = yield* readAttachment(socket);

        yield* hub.run(connection, connection.address, (runtime) =>
          runtime.checkSubscription(connection.session),
        );
        yield* transport
          .message(socket, message)
          .pipe(Effect.provideService(RpcHost.CurrentConnection, connection));
      }),
    ).pipe(
      Effect.catch((error) =>
        error.reason === "Unavailable"
          ? close(socket, 1013, "Source unavailable; reconnect and replay")
          : close(socket, 4403, "Invalid request, expired session or revoked access"),
      ),
    );

  const departure = (socket: SocketTransport.Socket) =>
    Effect.gen(function* () {
      const connection = yield* readAttachment(socket);

      yield* close(socket, 1000, "Connection closed");
      yield* hub.departure(connection);
    }).pipe(Effect.catch(() => close(socket, 1011, "Connection closed")));

  return {
    fetch: (request) => timed(fetch(request)),
    webSocketMessage,
    webSocketClose: departure,
    webSocketError: departure,
    alarm: hub.alarm,
  };
});

const readBody = Effect.fn("Cloudflare.readBody")(function* (request: Request, maxBytes: number) {
  if (request.body === null) return yield* invalid();
  const reader = request.body.getReader();

  const result = yield* Effect.tryPromise({
    try: async (signal) => {
      const chunks: Array<Uint8Array> = [];
      let length = 0;
      const cancel = () => reader.cancel().catch(() => undefined);

      signal.addEventListener("abort", cancel, { once: true });

      try {
        while (true) {
          const item = await reader.read();

          if (item.done) break;
          length += item.value.byteLength;
          if (length > maxBytes) throw invalid();
          chunks.push(item.value);
        }
        const bytes = new Uint8Array(length);
        let offset = 0;

        for (const chunk of chunks) {
          bytes.set(chunk, offset);
          offset += chunk.byteLength;
        }

        return new TextDecoder().decode(bytes);
      } finally {
        signal.removeEventListener("abort", cancel);
        await reader.cancel();
        reader.releaseLock();
      }
    },
    catch: invalid,
  });

  return result;
});

export type Identity = Omit<Server.Session, "connectionId">;

/** The caller owns routing, authentication and its typed object binding. */
export interface RequestOptions<Kind extends string> {
  readonly request: Request;
  readonly address: { readonly kind: Kind; readonly id: string };
  readonly identity: Identity;
}

/** Attach trusted Sync metadata before the application calls its native object stub. */
export const prepareRequest: <S extends Source.Spec>(
  contract: Source.Definition<S>,
  input: RequestOptions<S["kind"]>,
) => Effect.Effect<Request, ProtocolError> = Effect.fn("Cloudflare.prepareRequest")(function* <
  S extends Source.Spec,
>(
  contract: Source.Definition<S>,
  input: RequestOptions<S["kind"]>,
): Effect.fn.Return<Request, ProtocolError> {
  const address = yield* decode(contract.addressSchema, input.address);

  const session = yield* decode(Server.Session, {
    ...input.identity,
    connectionId: crypto.randomUUID(),
  });

  const request = new Request(input.request);

  request.headers.set("x-yielded-address", JSON.stringify(address));
  request.headers.set("x-yielded-session", JSON.stringify(session));

  return request;
});

/** Protocol failure response for hosts that expose Sync directly over HTTP. */
export const errorResponse = (error: ProtocolError): Response =>
  Response.json(error, {
    status:
      error.reason === "Unauthenticated"
        ? 401
        : error.reason === "Forbidden"
          ? 403
          : error.reason === "Unavailable"
            ? 503
            : 400,
  });
