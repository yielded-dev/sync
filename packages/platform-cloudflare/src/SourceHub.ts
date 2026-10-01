import { ProtocolError, SourceAddress, type Source, type SourcePosition } from "@yielded/sync";
import {
  Server,
  type ServerCrypto,
  SourceStorage,
  type StorageError,
  type DeliveryDisposition,
  type OutboxRecord,
} from "@yielded/sync/server";
import { DateTime, Effect, Queue, Schema, Semaphore, Stream, type Scope } from "effect";

import type { ObjectState, Socket } from "./SocketTransport.ts";

export const unavailable = () =>
  ProtocolError.make({ reason: "Unavailable", message: "Source temporarily unavailable" });

export const invalid = () =>
  ProtocolError.make({ reason: "UnsupportedVersion", message: "Invalid source request" });

export const Attachment = Schema.Struct({ session: Server.Session, address: SourceAddress });
export type Connection = typeof Attachment.Type & { readonly socket?: Socket };

export interface Options<R = never> {
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

type Frame = Source.Definition["envelope"]["outbound"]["Type"];

export interface Hub {
  readonly contract: Source.Definition;
  readonly run: <A>(
    connection: Connection,
    address: SourceAddress,
    body: (runtime: Server.Runtime<Source.Spec>) => Effect.Effect<A, ProtocolError>,
  ) => Effect.Effect<A, ProtocolError>;
  readonly subscribe: (
    connection: Connection,
    address: SourceAddress,
    after?: SourcePosition,
  ) => Stream.Stream<Frame, ProtocolError>;
  readonly dispatch: (
    connection: Connection,
    address: SourceAddress,
    command: Schema.Json,
  ) => Effect.Effect<Schema.Json, ProtocolError>;
  readonly publishMessage: (
    connection: Connection,
    address: SourceAddress,
    message: Schema.Json,
  ) => Effect.Effect<void, ProtocolError>;
  readonly departure: (connection: Connection) => Effect.Effect<void, ProtocolError>;
  readonly alarm: () => Effect.Effect<void, ProtocolError | StorageError>;
}

/** Source ordering and recovery policy, independent of RPC framing and acknowledgement. */
export const make = Effect.fn("Cloudflare.SourceHub.make")(function* <
  S extends Source.Spec,
  R,
  DeliveryR,
>(
  definition: Server.Definition<S, R>,
  state: ObjectState,
  options: Options<DeliveryR>,
  close: (socket: Socket, code: number, reason: string) => Effect.Effect<void>,
): Effect.fn.Return<Hub, never, R | DeliveryR | SourceStorage | ServerCrypto> {
  const services = yield* Effect.context<R | DeliveryR | SourceStorage | ServerCrypto>();
  const storage = yield* SourceStorage;
  // The host runs a dynamic source registry. Keep its actual schemas and application requirements.
  const source = definition as unknown as Server.Definition<Source.Spec, R>;
  const contract = source.contract;
  const semaphore = yield* Semaphore.make(1);
  const subscriptions = new Map<Socket, { connection: Connection; queue: Queue.Queue<Frame> }>();
  let waiting = 0;

  const limits = {
    maxEvents: options.replay.maxEvents,
    maxBytes: options.replay.maxBatchBytes,
    maxOutbox: options.outbox?.batchSize ?? 100,
    turnTimeoutMillis: options.turnTimeoutMillis ?? 5_000,
  };

  const finish = <A, E>(
    effect: Effect.Effect<A, E, R | DeliveryR | SourceStorage | ServerCrypto | Scope.Scope>,
  ) => effect.pipe(Effect.scoped, Effect.provideContext(services));

  const locked = <A, E, Requirements>(effect: Effect.Effect<A, E, Requirements>) =>
    Effect.suspend(() => {
      if (waiting >= (options.maxQueuedTurns ?? 128)) return Effect.fail(unavailable());
      waiting++;

      return semaphore
        .withPermits(1)(effect)
        .pipe(
          Effect.timeoutOrElse({ duration: limits.turnTimeoutMillis, orElse: unavailable }),
          Effect.ensuring(
            Effect.sync(() => {
              waiting--;
            }),
          ),
        );
    });

  const run: Hub["run"] = (connection, address, body) =>
    finish(
      locked(
        Effect.gen(function* () {
          if (address.kind !== connection.address.kind || address.id !== connection.address.id)
            return yield* invalid();
          const runtime = yield* Server.open(source, connection.address, limits);

          return yield* body(runtime);
        }),
      ),
    );

  const broadcast = Effect.fn("Cloudflare.SourceHub.broadcast")(function* (
    runtime: Server.Runtime<Source.Spec>,
    frame: Frame,
  ) {
    for (const [socket, subscription] of subscriptions) {
      yield* runtime.checkSubscription(subscription.connection.session).pipe(
        Effect.flatMap(() => Queue.offer(subscription.queue, frame)),
        Effect.flatMap((accepted) =>
          accepted
            ? Effect.void
            : close(socket, 1013, "Subscription overflow; recover from durable position"),
        ),
        Effect.catch(() => close(socket, 4403, "Session expired or source access revoked")),
      );
    }
  });

  const subscribe: Hub["subscribe"] = (connection, address, after) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const socket = connection.socket;

        if (socket === undefined)
          return yield* ProtocolError.make({
            reason: "UnsupportedVersion",
            message: "Use WebSocket RPC for subscribe",
          });

        // Reserve one frame for the batch awaiting Effect RPC's acknowledgement.
        const queue = yield* Queue.make<Frame>({
          capacity: options.sockets.bufferSize - 1,
          strategy: "dropping",
        });

        const subscription = { connection, queue };

        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            if (subscriptions.get(socket) === subscription) subscriptions.delete(socket);
            yield* Queue.shutdown(queue);
          }),
        );

        const initial = yield* run(connection, address, (runtime) =>
          Effect.gen(function* () {
            if (subscriptions.has(socket)) return yield* invalid();
            // Registration and bootstrap share the commit/publication gate.
            subscriptions.set(socket, subscription);

            return yield* runtime.bootstrap(connection.session, after);
          }),
        );

        return Stream.make(initial).pipe(Stream.concat(Stream.fromEffectRepeat(Queue.take(queue))));
      }),
    );

  const dispatch: Hub["dispatch"] = (connection, address, command) =>
    run(connection, address, (runtime) => {
      const sockets = state.getWebSockets();
      let committed = false;

      return Effect.gen(function* () {
        const executed = yield* runtime.dispatch(connection.session, command);

        committed = true;
        for (const event of executed.events) {
          const frame = yield* Schema.decodeEffect(
            Schema.toCodecJson(contract.envelope.eventFrame),
          )({
            _tag: "Event",
            protocolVersion: 1,
            address: runtime.address,
            schemaVersion: contract.schemaVersion,
            event,
          }).pipe(Effect.mapError(unavailable));

          yield* broadcast(runtime, frame);
        }

        return executed.outcome;
      }).pipe(
        Effect.onError((cause) => {
          const needsRecovery =
            committed ||
            cause.reasons.some(
              (reason) => reason._tag !== "Fail" || reason.error.reason === "Unavailable",
            );

          return needsRecovery
            ? Effect.forEach(
                sockets,
                (socket) =>
                  close(socket, 1013, "Publication incomplete; recover from durable position"),
                { discard: true },
              )
            : Effect.void;
        }),
      );
    });

  const publishMessage: Hub["publishMessage"] = (connection, address, message) =>
    run(connection, address, (runtime) =>
      Effect.gen(function* () {
        if (connection.socket === undefined)
          return yield* ProtocolError.make({
            reason: "Forbidden",
            message: "Ephemeral messages require a WebSocket connection",
          });
        yield* broadcast(runtime, yield* runtime.publishMessage(connection.session, message));
      }),
    );

  const departure: Hub["departure"] = (connection) =>
    run(connection, connection.address, (runtime) =>
      Effect.gen(function* () {
        if (connection.socket !== undefined) {
          const subscription = subscriptions.get(connection.socket);

          subscriptions.delete(connection.socket);
          if (subscription !== undefined) yield* Queue.shutdown(subscription.queue);
        }
        yield* broadcast(runtime, {
          _tag: "MessageLeave",
          protocolVersion: 1,
          address: connection.address,
          schemaVersion: contract.schemaVersion,
          actorId: connection.session.actorId,
          connectionId: connection.session.connectionId,
        });
      }),
    );

  const alarm = () =>
    finish(
      Effect.gen(function* () {
        const delivery = options.outbox;

        if (delivery === undefined) return;
        const now = DateTime.toEpochMillis(yield* DateTime.now);

        yield* Effect.tryPromise({
          try: () => state.storage.setAlarm(now + delivery.retryMillis),
          catch: unavailable,
        });
        const records = yield* storage.claimOutbox(now, delivery.retryMillis, delivery.batchSize);

        const retry = (_tag: "Retry" | "Indeterminate") =>
          Effect.map(DateTime.now, (time) => ({
            _tag,
            atMillis: DateTime.toEpochMillis(time) + delivery.retryMillis,
          }));

        for (const record of records) {
          const disposition = yield* delivery.deliver(record).pipe(
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

            yield* Effect.tryPromise({
              try: () =>
                next === undefined
                  ? state.storage.deleteAlarm()
                  : state.storage.setAlarm(Math.max(finishedAt + 1, next)),
              catch: unavailable,
            });
          }),
        );
      }),
    );

  return { contract, run, subscribe, dispatch, publishMessage, departure, alarm };
});
