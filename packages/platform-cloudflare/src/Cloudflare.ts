import { ProtocolError, SourceAddress, type Source } from "@yielded/sync";
import {
  Server,
  ServerCrypto,
  ServerRpc,
  StorageError,
  type SourceStorage,
} from "@yielded/sync/server";
import { Crypto, DateTime, Effect, Exit, Layer, PlatformError, Schema, Scope } from "effect";
import { Hex } from "effect/encoding";
import { HttpServerRequest, HttpServerResponse } from "effect/http";
import { RpcSerialization, RpcServer } from "effect/rpc";

import { makeClient, type ClientConstructor } from "./RemoteClient.ts";
import * as RpcHost from "./RpcHost.ts";
import * as SocketTransport from "./SocketTransport.ts";
import * as SqliteStorage from "./SqliteStorage.ts";

const invalid = () =>
  ProtocolError.make({ reason: "UnsupportedVersion", message: "Invalid source request" });

const unavailable = () =>
  ProtocolError.make({ reason: "Unavailable", message: "Source temporarily unavailable" });

const Attachment = Schema.Struct({ session: Server.Session, address: SourceAddress });

export type {
  ObjectState,
  Socket,
  Transport,
  Factory as TransportFactory,
} from "./SocketTransport.ts";

export interface Options<DeliveryR = never, E = never, R = never> {
  readonly replay?: Server.HostOptions["replay"];
  readonly sockets?: Server.HostOptions["connections"];
  readonly maxQueuedTurns?: number;
  readonly turnTimeoutMillis?: number;
  readonly outbox?: Omit<NonNullable<Server.HostOptions<DeliveryR>["outbox"]>, "schedule">;
  /** SQLite options, or a replacement that prevents SQLite from being opened. */
  readonly storage?: Partial<SqliteStorage.Options> | Layer.Layer<SourceStorage, E, R>;
  readonly crypto?: Layer.Layer<ServerCrypto, E, R>;
}

export type { ClientOptions, Command, RemoteClient } from "./RemoteClient.ts";

const webCrypto = Crypto.make({
  randomBytes: (size) => crypto.getRandomValues(new Uint8Array(size)),
  digest: (algorithm, data) =>
    Effect.tryPromise({
      try: async () => new Uint8Array(await crypto.subtle.digest(algorithm, new Uint8Array(data))),
      catch: (cause) =>
        PlatformError.systemError({
          _tag: "Unknown",
          module: "Crypto",
          method: "digest",
          cause,
        }),
    }),
});

const CryptoLive = Layer.succeed(ServerCrypto, {
  generation: webCrypto.randomUUIDv4().pipe(Effect.orDie),
  sha256: (text) =>
    Effect.tryPromise({
      try: async () =>
        Hex.encode(
          new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text))),
        ),
      catch: (cause) => StorageError.make({ message: "SHA-256 failed", cause }),
    }),
});

/** Build in the object's scope. The same defaults serve both framework integrations. */
export const layer = <DeliveryR = never, E = never, R = never>(
  native: DurableObjectStorage,
  options: Options<DeliveryR, E, R> = {},
): Layer.Layer<SourceStorage | ServerCrypto, StorageError | E, R> => {
  const storage: Layer.Layer<SourceStorage, StorageError | E, R> =
    options.storage !== undefined && "pipe" in options.storage
      ? options.storage
      : SqliteStorage.layer(native, {
          namespace: options.storage?.namespace ?? "sync",
          replayWindow: options.storage?.replayWindow ?? options.replay?.maxEvents ?? 512,
          maxPendingOutbox: options.storage?.maxPendingOutbox ?? (options.outbox ? 1_000 : 0),
        });

  return Layer.merge(options.crypto ?? CryptoLive, storage);
};

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
  readonly alarm: Effect.Effect<void, ProtocolError | StorageError>;
}

/** Acquire once per native object. RPC fibers live in the instance scope; source operations are scoped separately. */
export const make = Effect.fn("Cloudflare.make")(function* <
  S extends Source.Spec,
  R,
  DeliveryR = never,
  E = never,
  PlatformR = never,
>(
  definition: Server.Definition<S, R>,
  state: SocketTransport.ObjectState,
  options: Options<DeliveryR, E, PlatformR> = {},
  transportFactory?: SocketTransport.Factory,
): Effect.fn.Return<
  Handlers,
  StorageError | E,
  Scope.Scope | PlatformR | Exclude<R | DeliveryR, SourceStorage | ServerCrypto>
> {
  const context = yield* Layer.build(layer(state.storage, options));
  const scope = yield* Scope.Scope;

  const connections = new Map<
    SocketTransport.Socket,
    { connection: Server.LiveConnection; scope: Scope.Closeable }
  >();

  const closeNative = (socket: SocketTransport.Socket, code: number, reason: string) =>
    Effect.try({ try: () => socket.close(code, reason), catch: unavailable }).pipe(Effect.ignore);

  const readAttachment = (socket: SocketTransport.Socket) =>
    Effect.try({ try: () => socket.deserializeAttachment(), catch: invalid }).pipe(
      Effect.flatMap((value) => decode(Attachment, value)),
    );

  // Fence restored identities before a native transport resets its RPC sockets.
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

  const maxBatchBytes = options.replay?.maxBatchBytes ?? 600_000;
  const serialization = ServerRpc.serialization(maxBatchBytes);

  const transport = yield* (transportFactory ?? SocketTransport.make(state)).pipe(
    Effect.provideService(RpcSerialization.RpcSerialization, serialization),
  );

  const close = (socket: SocketTransport.Socket, code: number, reason: string) =>
    closeNative(socket, code, reason).pipe(Effect.andThen(transport.close(socket)));

  const { outbox } = options;
  // Native handlers dispatch dynamically; the source's codecs retain their concrete schemas.
  const source = definition as unknown as Server.Definition<Source.Spec, R>;

  const hub = yield* Server.host(source, {
    replay: options.replay,
    connections: options.sockets,
    maxQueuedTurns: options.maxQueuedTurns,
    turnTimeoutMillis: options.turnTimeoutMillis,
    ...(outbox === undefined
      ? {}
      : {
          outbox: {
            ...outbox,
            schedule: (atMillis: number | undefined) =>
              Effect.tryPromise({
                try: () =>
                  atMillis === undefined
                    ? state.storage.deleteAlarm()
                    : state.storage.setAlarm(atMillis),
                catch: (cause) =>
                  StorageError.make({ message: "Could not schedule outbox delivery", cause }),
              }),
          },
        }),
  }).pipe(Effect.provideContext(context));

  const rpc = ServerRpc.handlers(hub);
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

    const connection: Server.Connection = { address, session };

    if (request.headers.get("upgrade")?.toLowerCase() === "websocket") {
      const child = yield* Scope.fork(scope, "sequential");

      return yield* Effect.gen(function* () {
        const live = yield* hub.connect(connection).pipe(Effect.provideService(Scope.Scope, child));

        const pair = yield* Effect.try({
          try: () => {
            const pair = new WebSocketPair();

            pair[1].serializeAttachment(connection satisfies typeof Attachment.Type);

            return pair;
          },
          catch: unavailable,
        });

        const socket = pair[1];

        connections.set(socket, { connection: live, scope: child });
        yield* Scope.addFinalizer(
          child,
          Effect.gen(function* () {
            connections.delete(socket);
            yield* close(socket, 1000, "Connection closed");
          }),
        );
        yield* transport.accept(socket);
        yield* live.closed.pipe(
          Effect.catch((error) =>
            close(socket, error.reason === "Unavailable" ? 1013 : 4403, error.message),
          ),
          Effect.andThen(Scope.close(child, Exit.void)),
          Effect.forkIn(scope),
        );

        return new Response(null, { status: 101, webSocket: pair[0] });
      }).pipe(Effect.onError(() => Scope.close(child, Exit.void)));
    }
    if (request.method !== "POST")
      return new Response("Expected POST or WebSocket", { status: 405 });
    const body = yield* readBody(request, maxBatchBytes);

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
      Effect.provideService(ServerRpc.CurrentConnection, connection),
      Effect.scoped,
    );

    return HttpServerResponse.toWeb(result);
  });

  const webSocketMessage = (socket: SocketTransport.Socket, message: string | ArrayBuffer) =>
    timed(
      Effect.gen(function* () {
        const attachment = yield* readAttachment(socket);

        yield* hub.checkConnection(attachment);
        const live = connections.get(socket);

        if (live === undefined) return yield* unavailable();
        yield* transport
          .message(socket, message)
          .pipe(Effect.provideService(ServerRpc.CurrentConnection, live.connection));
      }),
    ).pipe(
      Effect.catch((error) =>
        close(
          socket,
          error.reason === "Unavailable" ? 1013 : 4403,
          error.reason === "Unavailable"
            ? "Source unavailable; reconnect and replay"
            : "Invalid request, expired session or revoked access",
        ),
      ),
    );

  const departure = (socket: SocketTransport.Socket) =>
    Effect.gen(function* () {
      const live = connections.get(socket);

      if (live === undefined) return yield* close(socket, 1000, "Connection closed");
      yield* Scope.close(live.scope, Exit.void);
    });

  return {
    fetch: (request) => timed(fetch(request)),
    webSocketMessage,
    webSocketClose: departure,
    webSocketError: departure,
    alarm: hub.drainOutbox,
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
    connectionId: yield* webCrypto.randomUUIDv4().pipe(Effect.orDie),
  });

  const request = new Request(input.request);

  request.headers.set("x-yielded-address", JSON.stringify(address));
  request.headers.set("x-yielded-session", JSON.stringify(session));

  return request;
});

/** A scoped, request-only client. The application retains exact command identities. */
export const client: ClientConstructor = makeClient(prepareRequest);

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
