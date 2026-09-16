import { ProtocolError, SourceAddress, SourcePosition, type Source } from "@yielded/sync";
import {
  Server,
  ServerCrypto,
  SourceStorage,
  StorageError,
  type DeliveryDisposition,
  type OutboxRecord,
} from "@yielded/sync/server";
import { Context, DateTime, Effect, Encoding, Layer, Schema, Semaphore } from "effect";
import {
  DurableObject,
  DurableObjectState,
  DurableObjectWebSocket,
  Worker,
  WorkerEnvironment,
} from "effect-cf";
import type { RpcMessage } from "effect/unstable/rpc";

import * as SqliteStorage from "./SqliteStorage.ts";

const unavailable = () =>
  ProtocolError.make({ reason: "Unavailable", message: "Source temporarily unavailable" });

const invalid = () =>
  ProtocolError.make({ reason: "UnsupportedVersion", message: "Invalid source request" });

const CryptoLive = Layer.succeed(ServerCrypto, {
  generation: Effect.sync(() => crypto.randomUUID()),
  sha256: (text) =>
    Effect.tryPromise({
      try: async () =>
        Encoding.encodeHex(
          new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text))),
        ),
      catch: (cause) => StorageError.make({ message: "SHA-256 failed", cause }),
    }),
});

const RequestId = Schema.Union([Schema.String, Schema.Int]);

const RpcRequest = Schema.TaggedStruct("Request", {
  id: RequestId,
  tag: Schema.String,
  payload: Schema.Json,
});

const Inbound = Schema.Union([
  RpcRequest,
  Schema.TaggedStruct("Ping", {}),
  Schema.TaggedStruct("Eof", {}),
  Schema.TaggedStruct("Ack", { requestId: RequestId }),
  Schema.TaggedStruct("Interrupt", { requestId: RequestId }),
]);

const Attachment = Schema.Struct({
  session: Server.Session,
  address: SourceAddress,
  requestId: Schema.NullOr(RequestId),
  pending: Schema.Int,
});

type Attachment = typeof Attachment.Type;
type Socket = DurableObjectWebSocket.DurableWebSocket;
class Gate extends Context.Service<
  Gate,
  { readonly semaphore: Semaphore.Semaphore; waiting: number }
>()("@yielded/sync-cloudflare/Gate") {}

const GateLive = Layer.effect(
  Gate,
  Effect.map(Semaphore.make(1), (semaphore) => ({ semaphore, waiting: 0 })),
);

const decode = <S extends Schema.Codec<unknown, unknown>>(schema: S, value: unknown) =>
  Schema.decodeUnknownEffect(schema)(value).pipe(Effect.mapError(invalid));

const encode = <S extends Schema.Codec<unknown, unknown>>(schema: S, value: S["Type"]) =>
  Schema.encodeEffect(Schema.toCodecJson(schema))(value).pipe(Effect.mapError(unavailable));

export interface Options<R> {
  readonly services: Layer.Layer<
    R,
    ProtocolError,
    DurableObjectState.DurableObjectState | WorkerEnvironment
  >;
  readonly storageNamespace: string;
  readonly replay: { readonly maxEvents: number; readonly maxBatchBytes: number };
  readonly sockets: { readonly maxConnections: number; readonly bufferSize: number };
  readonly maxQueuedTurns?: number;
  readonly turnTimeoutMillis?: number;
  readonly outbox?: {
    readonly maxPending: number;
    readonly batchSize: number;
    readonly retryMillis: number;
    readonly deliver: (
      record: OutboxRecord,
    ) => Effect.Effect<DeliveryDisposition, ProtocolError, R>;
  };
}

export type SourceObject = DurableObject.DurableObjectClass<Record<never, never>, never>;
export type SourceWorker = Worker.FetchHandler;

/** A source runtime and application services are scoped to each platform event. */
export const durableObject = <S extends Source.Spec, R>(
  definition: Server.Definition<S, R>,
  options: Options<R>,
): SourceObject => {
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
  const contract = definition.contract;
  const eventDefinition = Server.provide(definition, options.services);

  const limits = {
    maxEvents: options.replay.maxEvents,
    maxBytes: options.replay.maxBatchBytes,
    maxOutbox: options.outbox?.batchSize ?? 100,
    turnTimeoutMillis: options.turnTimeoutMillis ?? 5_000,
  };

  const open = (address: SourceAddress) => Server.open(eventDefinition, address, limits);

  const layer = Layer.mergeAll(
    CryptoLive,
    GateLive,
    SqliteStorage.layer({
      namespace: options.storageNamespace,
      replayWindow: options.replay.maxEvents,
      maxPendingOutbox: options.outbox?.maxPending ?? 0,
    }),
  );

  const locked = <A, E, Requirements>(effect: Effect.Effect<A, E, Requirements>) =>
    Effect.flatMap(Gate, (gate) =>
      Effect.suspend(() => {
        if (gate.waiting >= (options.maxQueuedTurns ?? 128)) return Effect.fail(unavailable());
        gate.waiting++;

        return gate.semaphore
          .withPermits(1)(effect)
          .pipe(
            Effect.timeoutOrElse({
              duration: limits.turnTimeoutMillis,
              orElse: () => Effect.fail(unavailable()),
            }),
            Effect.ensuring(
              Effect.sync(() => {
                gate.waiting--;
              }),
            ),
          );
      }),
    );

  const close = (socket: Socket, code: number, message: string) =>
    socket.close(code, message).pipe(Effect.ignore);

  const readAttachment = (socket: Socket) =>
    socket.deserializeAttachment.pipe(
      Effect.mapError(invalid),
      Effect.flatMap((value) => decode(Attachment, value)),
    );

  const send = (socket: Socket, message: RpcMessage.FromServerEncoded) =>
    socket.send(JSON.stringify(message)).pipe(Effect.mapError(unavailable));

  const chunk = Effect.fn("Cloudflare.chunk")(function* (
    socket: Socket,
    attachment: Attachment,
    frame: unknown,
  ) {
    if (attachment.requestId === null) return;
    if (attachment.pending >= options.sockets.bufferSize) {
      yield* close(socket, 1013, "Subscription overflow; recover from durable position");

      return;
    }
    yield* socket
      .serializeAttachment({ ...attachment, pending: attachment.pending + 1 })
      .pipe(Effect.mapError(unavailable));
    yield* send(socket, { _tag: "Chunk", requestId: attachment.requestId, values: [frame] });
  });

  const broadcast = Effect.fn("Cloudflare.broadcast")(function* (
    runtime: Server.Runtime<S>,
    frame: Schema.Json,
  ) {
    const host = yield* DurableObjectState.DurableObjectState;

    for (const socket of yield* host.getWebSockets()) {
      yield* Effect.gen(function* () {
        const attachment = yield* readAttachment(socket);

        if (attachment.requestId === null) return;
        yield* runtime.checkSubscription(attachment.session);
        yield* chunk(socket, attachment, frame);
      }).pipe(Effect.catch(() => close(socket, 4403, "Session expired or source access revoked")));
    }
  });

  const dispatch = Effect.fn("Cloudflare.dispatch")(function* (
    runtime: Server.Runtime<S>,
    session: Server.Session,
    request: typeof RpcRequest.Type,
    connected: boolean,
  ) {
    const rpc = contract.rpc.requests.get(request.tag);

    if (rpc === undefined) return yield* invalid();
    const payload = yield* decode(rpc.payloadSchema, request.payload);
    // The request schema owns version validation; the bound object owns the address.
    const requested = yield* decode(Schema.Struct({ address: SourceAddress }), request.payload);

    if (
      requested.address.kind !== runtime.address.kind ||
      requested.address.id !== runtime.address.id
    )
      return yield* invalid();
    if (request.tag === "snapshot") {
      const schema = contract.envelope.snapshotFrame as Schema.Codec<unknown, unknown>;

      return yield* encode(schema, yield* runtime.snapshot(session));
    }
    if (request.tag.startsWith("execute/")) {
      const executed = yield* runtime.dispatch(session, request.payload);

      for (const event of executed.events)
        yield* broadcast(runtime, {
          _tag: "Event",
          protocolVersion: 1,
          address: runtime.address,
          schemaVersion: contract.schemaVersion,
          event,
        });

      return executed.outcome;
    }
    if (request.tag.startsWith("result/")) return yield* runtime.lookup(session, request.payload);
    if (request.tag === "publishMessage") {
      if (!connected)
        return yield* ProtocolError.make({
          reason: "Forbidden",
          message: "Ephemeral messages require a WebSocket connection",
        });
      const input = yield* decode(Schema.Struct({ message: Schema.Json }), request.payload);
      const frame = yield* runtime.publishMessage(session, input.message);

      yield* broadcast(runtime, yield* encode(contract.envelope.messageFrame, frame));

      return yield* encode(Schema.Void, undefined);
    }
    // Subscriptions require a hibernatable connection; no event scope leaks into a Response body.
    void payload;

    return yield* ProtocolError.make({
      reason: "UnsupportedVersion",
      message: "Use WebSocket RPC for subscribe",
    });
  });

  const response = Effect.fn("Cloudflare.response")(function* (
    runtime: Server.Runtime<S>,
    session: Server.Session,
    request: typeof RpcRequest.Type,
    connected = false,
  ): Effect.fn.Return<
    RpcMessage.ResponseExitEncoded,
    never,
    DurableObjectState.DurableObjectState
  > {
    const result = yield* Effect.result(dispatch(runtime, session, request, connected));

    return {
      _tag: "Exit",
      requestId: request.id,
      exit:
        result._tag === "Success"
          ? { _tag: "Success", value: result.success }
          : { _tag: "Failure", cause: [{ _tag: "Fail", error: result.failure }] },
    };
  });

  const mount = Effect.fn("Cloudflare.mount")(function* (
    request: Request,
    rawAddress: unknown,
    rawSession: unknown,
  ) {
    const address = yield* decode(SourceAddress, rawAddress);
    const session = yield* decode(Server.Session, rawSession);
    const runtime = yield* open(address);

    if (Worker.isWebSocketUpgrade(request)) {
      yield* runtime.checkSubscription(session);
      const host = yield* DurableObjectState.DurableObjectState;

      if ((yield* host.getWebSockets()).length >= options.sockets.maxConnections)
        return new Response("Source connection capacity reached", { status: 503 });

      const accepted = yield* DurableObjectWebSocket.acceptUpgrade({
        attachment: { session, address, requestId: null, pending: 0 } satisfies Attachment,
      });

      return accepted.response;
    }
    if (request.method !== "POST")
      return new Response("Expected POST or WebSocket", { status: 405 });
    const text = yield* readBody(request, options.replay.maxBatchBytes);

    const requests = yield* decode(
      Schema.fromJsonString(
        Schema.Union([RpcRequest, Schema.Array(RpcRequest).check(Schema.isMaxLength(32))]),
      ),
      text,
    );

    const replies = yield* Effect.forEach(
      Array.isArray(requests) ? requests : [requests],
      (input) => response(runtime, session, input),
    );

    return Response.json(replies);
  });

  const webSocketMessage = (socket: Socket, raw: string | ArrayBuffer) =>
    locked(
      Effect.gen(function* () {
        const attachment = yield* readAttachment(socket);
        const runtime = yield* open(attachment.address);

        yield* runtime.checkSubscription(attachment.session);
        if (
          typeof raw !== "string" ||
          new TextEncoder().encode(raw).byteLength > options.replay.maxBatchBytes
        )
          return yield* invalid();

        const messages = yield* decode(
          Schema.fromJsonString(
            Schema.Union([Inbound, Schema.Array(Inbound).check(Schema.isMaxLength(32))]),
          ),
          raw,
        );

        for (const message of Array.isArray(messages) ? messages : [messages]) {
          const current = yield* readAttachment(socket);

          if (message._tag === "Ping") {
            yield* send(socket, { _tag: "Pong" });
          } else if (message._tag === "Ack") {
            if (message.requestId === current.requestId)
              yield* socket.serializeAttachment({
                ...current,
                pending: Math.max(0, current.pending - 1),
              });
          } else if (message._tag === "Interrupt") {
            if (message.requestId === current.requestId) {
              yield* socket.serializeAttachment({ ...current, requestId: null, pending: 0 });
              yield* send(socket, {
                _tag: "Exit",
                requestId: message.requestId,
                exit: { _tag: "Failure", cause: [{ _tag: "Interrupt", fiberId: undefined }] },
              });
            }
          } else if (message._tag === "Eof") {
            yield* close(socket, 1000, "Client finished");
          } else if (message.tag === "subscribe") {
            if (current.requestId !== null) return yield* invalid();
            const rpc = contract.rpc.requests.get("subscribe");

            if (rpc === undefined) return yield* invalid();
            yield* decode(rpc.payloadSchema, message.payload);

            const input = yield* decode(
              Schema.Struct({ address: SourceAddress, after: Schema.optionalKey(SourcePosition) }),
              message.payload,
            );

            if (
              input.address.kind !== runtime.address.kind ||
              input.address.id !== runtime.address.id
            )
              return yield* invalid();
            const subscribed = { ...current, requestId: message.id };

            // The source event gate covers registration, bootstrap, commit and publication.
            yield* socket.serializeAttachment(subscribed);
            const frame = yield* runtime.bootstrap(current.session, input.after);
            const schema = contract.envelope.outbound as Schema.Codec<unknown, unknown>;

            yield* chunk(socket, subscribed, yield* encode(schema, frame));
          } else {
            yield* send(socket, yield* response(runtime, current.session, message, true));
          }
        }
      }),
    ).pipe(
      Effect.catch(() => close(socket, 4403, "Invalid request, expired session or revoked access")),
    );

  const departure = (socket: Socket) =>
    locked(
      Effect.gen(function* () {
        const attachment = yield* readAttachment(socket);

        yield* socket.serializeAttachment({ ...attachment, requestId: null });
        yield* close(socket, 1000, "Connection closed");
        const runtime = yield* open(attachment.address);

        yield* broadcast(runtime, {
          _tag: "MessageLeave",
          protocolVersion: 1,
          address: attachment.address,
          schemaVersion: contract.schemaVersion,
          actorId: attachment.session.actorId,
          connectionId: attachment.session.connectionId,
        });
      }),
    ).pipe(Effect.catch(() => close(socket, 1011, "Connection closed")));

  const alarm = Effect.fn("Cloudflare.alarm")(function* () {
    const delivery = options.outbox;

    if (delivery === undefined) return;
    const storage = yield* SourceStorage;
    const host = yield* DurableObjectState.DurableObjectState;
    const now = DateTime.toEpochMillis(yield* DateTime.now);

    // Rearm before any external effect; an ambiguous delivery retains its stable id.
    yield* host.storage.setAlarm(now + delivery.retryMillis);
    const records = yield* storage.claimOutbox(now, delivery.retryMillis, delivery.batchSize);
    const services = yield* Layer.build(options.services);

    const retry = (_tag: "Retry" | "Indeterminate") =>
      Effect.map(DateTime.now, (time) => ({
        _tag,
        atMillis: DateTime.toEpochMillis(time) + delivery.retryMillis,
      }));

    for (const record of records) {
      const disposition = yield* delivery.deliver(record).pipe(
        Effect.provideContext(services),
        Effect.timeoutOrElse({
          duration: delivery.retryMillis / 2,
          orElse: () => retry("Indeterminate"),
        }),
        Effect.catch(() => retry("Retry")),
      );

      yield* storage.settleOutbox(record, disposition);
    }
    yield* locked(
      Effect.gen(function* () {
        const next = yield* storage.nextOutboxTime;
        const finishedAt = DateTime.toEpochMillis(yield* DateTime.now);

        if (next !== undefined) yield* host.storage.setAlarm(Math.max(finishedAt + 1, next));
        else yield* host.storage.deleteAlarm();
      }),
    );
  });

  const fetch = Effect.gen(function* () {
    const request = yield* Worker.NativeRequest;

    const address = yield* decode(
      Schema.fromJsonString(SourceAddress),
      request.headers.get("x-yielded-address"),
    );

    const session = yield* decode(
      Schema.fromJsonString(Server.Session),
      request.headers.get("x-yielded-session"),
    );

    return yield* locked(mount(request, address, session));
  });

  return DurableObject.make(layer, {
    fetch,
    webSocketMessage,
    webSocketClose: departure,
    webSocketError: departure,
    alarm,
  });
};

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

interface MountStub {
  fetch(request: Request): Promise<Response>;
}
interface MountNamespace {
  getByName(name: string): MountStub;
}

export const worker = <S extends Source.Spec>(
  contract: Source.Definition<S>,
  options: {
    readonly binding: string;
    readonly path: string;
    readonly objectName: (id: string) => string;
    readonly authenticate: (request: Request) => Effect.Effect<
      {
        readonly principal: Schema.Json;
        readonly actorId: string;
        readonly expiresAtMillis: number;
      },
      ProtocolError,
      WorkerEnvironment
    >;
  },
): SourceWorker =>
  Worker.makeFetchHandler(Layer.empty, {
    fetch: Effect.gen(function* () {
      const request = yield* Worker.NativeRequest;
      const env = yield* WorkerEnvironment;
      const prefix = options.path.endsWith("/:id") ? options.path.slice(0, -3) : undefined;

      if (prefix === undefined) return yield* Effect.die("Source path must end in /:id");
      const pathname = new URL(request.url).pathname;
      const path = pathname.endsWith("/") ? pathname.slice(0, -1) : pathname;

      if (!path.startsWith(prefix) || path.slice(prefix.length).includes("/"))
        return new Response("Not found", { status: 404 });

      const id = yield* Effect.try({
        try: () => decodeURIComponent(path.slice(prefix.length)),
        catch: invalid,
      });

      const address = yield* decode(contract.addressSchema, { kind: contract.kind, id });
      const identity = yield* options.authenticate(request);

      const session = yield* decode(Server.Session, {
        ...identity,
        connectionId: crypto.randomUUID(),
      });

      // The application chooses the binding name; the namespace is a trusted platform capability.
      const binding = (env as unknown as Record<string, MountNamespace>)[options.binding];

      if (binding === undefined) return yield* unavailable();

      // Only this authenticated gateway exposes the binding. Always replace client-supplied identity headers.
      const forwarded = new Request(request);

      forwarded.headers.set("x-yielded-address", JSON.stringify(address));
      forwarded.headers.set("x-yielded-session", JSON.stringify(session));

      return yield* Effect.tryPromise({
        try: () => binding.getByName(options.objectName(id)).fetch(forwarded),
        catch: unavailable,
      });
    }).pipe(
      Effect.catchTag("ProtocolError", (error) =>
        Effect.succeed(
          Response.json(error, {
            status:
              error.reason === "Unauthenticated"
                ? 401
                : error.reason === "Forbidden"
                  ? 403
                  : error.reason === "Unavailable"
                    ? 503
                    : 400,
          }),
        ),
      ),
    ),
  });
