import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";

import { ProtocolError, type SourceAddress, type SourcePosition } from "../Model.ts";
import type * as Source from "../Source.ts";
import { invalid, unavailable } from "./serverCodec.ts";
import { open, type Runtime, type Session } from "../server/Server.ts";
import type { Definition } from "../server/Server.ts";
import {
  SourceStorage,
  StorageError,
  type DeliveryDisposition,
  type OutboxRecord,
  type ServerCrypto,
} from "../server/SourceStorage.ts";

import type { Connection, LiveConnection, Host, HostOptions } from "../server/Host.ts";

type Frame<S extends Source.Spec> = Source.Definition<S>["envelope"]["outbound"]["Type"];

type Turn<S extends Source.Spec> = <A>(
  connection: Connection,
  address: SourceAddress,
  body: (runtime: Runtime<S>) => Effect.Effect<A, ProtocolError>,
) => Effect.Effect<A, ProtocolError>;

/** One coordinator per storage authority. The host scope owns all live connections. */
export const host = Effect.fn("Server.host")(function* <
  S extends Source.Spec,
  R,
  DeliveryR = never,
>(
  definition: Definition<S, R>,
  options: HostOptions<DeliveryR> = {},
): Effect.fn.Return<
  Host<S>,
  StorageError,
  R | DeliveryR | SourceStorage | ServerCrypto | Scope.Scope
> {
  const services = yield* Effect.context<R | DeliveryR | SourceStorage | ServerCrypto>();
  const storage = yield* SourceStorage;
  const outbox = storage.outbox;

  const delivery =
    options.outbox === undefined
      ? undefined
      : {
          ...options.outbox,
          batchSize: options.outbox.batchSize ?? 10,
          retryMillis: options.outbox.retryMillis ?? 60_000,
        };

  const replay = options.replay ?? { maxEvents: 512, maxBatchBytes: 600_000 };
  const connections = options.connections ?? { maxConnections: 100, bufferSize: 256 };
  const maxQueuedTurns = options.maxQueuedTurns ?? 128;

  const limits = {
    maxEvents: replay.maxEvents,
    maxBytes: replay.maxBatchBytes,
    maxOutbox: delivery?.batchSize ?? 100,
    turnTimeoutMillis: options.turnTimeoutMillis ?? 5_000,
  };

  for (const limit of [
    ...Object.values(replay),
    ...Object.values(connections),
    maxQueuedTurns,
    limits.turnTimeoutMillis,
    ...(delivery === undefined ? [] : [delivery.batchSize, delivery.retryMillis]),
  ]) {
    if (!Number.isSafeInteger(limit) || limit <= 0)
      return yield* Effect.die("Source limits must be positive safe integers");
  }
  if ((delivery === undefined) !== (outbox === undefined))
    return yield* StorageError.make({
      message: "Outbox delivery and storage capability must be configured together",
    });

  const contract = definition.contract;
  const semaphore = yield* Semaphore.make(1);

  type Entry = {
    readonly connection: LiveConnection;
    readonly signal: Deferred.Deferred<void, ProtocolError>;
    active: boolean;
  };
  const live = new Set<Entry>();
  // Weak ownership retains fencing without retaining past connections.
  const entries = new WeakMap<Connection, Entry>();
  const subscriptions = new Map<Connection, { queue: Queue.Queue<Frame<S>> }>();
  let waiting = 0;
  let active = true;

  const finish = <A, E>(
    effect: Effect.Effect<A, E, R | DeliveryR | SourceStorage | ServerCrypto | Scope.Scope>,
  ) => effect.pipe(Effect.scoped, Effect.provideContext(services));

  const locked = <A, E, Requirements>(effect: Effect.Effect<A, E, Requirements>) =>
    Effect.suspend(() => {
      if (!active || waiting >= maxQueuedTurns)
        return Effect.fail(unavailable("Source temporarily unavailable"));
      waiting++;

      return semaphore
        .withPermits(1)(
          Effect.suspend<A, E | ProtocolError, Requirements>(() =>
            active ? effect : Effect.fail(unavailable("Source host has closed")),
          ),
        )
        .pipe(
          Effect.timeoutOrElse({
            duration: limits.turnTimeoutMillis,
            orElse: () => unavailable("Source turn timed out"),
          }),
          Effect.ensuring(
            Effect.sync(() => {
              waiting--;
            }),
          ),
        );
    });

  const turn: Turn<S> = (connection, address, body) =>
    finish(
      locked(
        Effect.gen(function* () {
          if (address.kind !== connection.address.kind || address.id !== connection.address.id)
            return yield* invalid("Source address differs from the authenticated connection");
          const runtime = yield* open(definition, connection.address, limits);

          return yield* body(runtime);
        }),
      ),
    );

  const run: Turn<S> = (connection, address, body) =>
    turn(connection, address, (runtime) =>
      Effect.gen(function* () {
        if (entries.get(connection)?.active === false)
          return yield* unavailable("Connection has closed");

        return yield* body(runtime);
      }),
    );

  const close = (entry: Entry, error: ProtocolError) =>
    Effect.gen(function* () {
      if (!entry.active) return;
      entry.active = false;
      live.delete(entry);
      const subscription = subscriptions.get(entry.connection);

      subscriptions.delete(entry.connection);
      if (subscription !== undefined) yield* Queue.shutdown(subscription.queue);
      yield* Deferred.fail(entry.signal, error);
    }).pipe(Effect.uninterruptible);

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      active = false;
      yield* Effect.forEach(
        [...live],
        (entry) => close(entry, unavailable("Source host has closed")),
        { discard: true },
      );
    }),
  );

  const broadcast = Effect.fn("Server.host.broadcast")(function* (
    runtime: Runtime<S>,
    frame: Frame<S>,
  ) {
    for (const [connection, subscription] of subscriptions) {
      const entry = entries.get(connection);

      if (entry === undefined || !entry.active) continue;
      yield* runtime.checkSubscription(connection.session).pipe(
        Effect.flatMap(() => Queue.offer(subscription.queue, frame)),
        Effect.flatMap((accepted) =>
          accepted
            ? Effect.void
            : close(entry, unavailable("Subscription overflow; recover from durable position")),
        ),
        Effect.catch(() =>
          close(
            entry,
            ProtocolError.make({
              reason: "Forbidden",
              message: "Session expired or source access revoked",
            }),
          ),
        ),
      );
    }
  });

  const connect: Host<S>["connect"] = (input) =>
    Effect.acquireRelease(
      Effect.gen(function* () {
        const signal = yield* Deferred.make<void, ProtocolError>();
        const connection: LiveConnection = { ...input, closed: Deferred.await(signal) };
        const entry: Entry = { connection, signal, active: true };

        yield* turn(connection, connection.address, (runtime) =>
          Effect.gen(function* () {
            yield* runtime.checkSubscription(connection.session);
            if (!active || live.size >= connections.maxConnections)
              return yield* unavailable("Source connection capacity reached");
            entries.set(connection, entry);
            live.add(entry);
          }),
        ).pipe(Effect.onError(() => close(entry, unavailable("Connection acquisition failed"))));

        return entry;
      }),
      (entry) => {
        const cleanup = Effect.gen(function* () {
          entry.active = false;
          live.delete(entry);
          const subscription = subscriptions.get(entry.connection);

          subscriptions.delete(entry.connection);
          if (subscription !== undefined) yield* Queue.shutdown(subscription.queue);
          yield* Deferred.succeed(entry.signal, undefined);
        });

        return turn(entry.connection, entry.connection.address, (runtime) =>
          Effect.gen(function* () {
            yield* cleanup;

            const frame = yield* Schema.decodeEffect(
              Schema.toCodecJson(contract.envelope.leaveFrame),
            )({
              _tag: "MessageLeave",
              protocolVersion: 1,
              address: entry.connection.address,
              schemaVersion: contract.schemaVersion,
              actorId: entry.connection.session.actorId,
              connectionId: entry.connection.session.connectionId,
            }).pipe(Effect.mapError(() => unavailable("Invalid departure frame")));

            yield* broadcast(runtime, frame);
          }),
        ).pipe(Effect.ensuring(cleanup), Effect.ignore);
      },
    ).pipe(Effect.map((entry) => entry.connection));

  const subscribe: Host<S>["subscribe"] = (connection, address, after) =>
    Stream.unwrap(
      Effect.gen(function* () {
        // Reserve one frame for the batch awaiting the transport's acknowledgement.
        const queue = yield* Queue.make<Frame<S>>({
          capacity: connections.bufferSize - 1,
          strategy: "dropping",
        });

        const subscription = { queue };

        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            if (subscriptions.get(connection) === subscription) subscriptions.delete(connection);
            yield* Queue.shutdown(queue);
          }),
        );

        const initial = yield* run(connection, address, (runtime) =>
          Effect.gen(function* () {
            if (!entries.get(connection)?.active)
              return yield* ProtocolError.make({
                reason: "Forbidden",
                message: "Subscription requires a live connection",
              });
            if (subscriptions.has(connection))
              return yield* invalid("Connection is already subscribed");
            subscriptions.set(connection, subscription);

            return yield* runtime.bootstrap(connection.session, after);
          }),
        );

        return Stream.make(initial).pipe(Stream.concat(Stream.fromEffectRepeat(Queue.take(queue))));
      }),
    );

  const dispatch: Host<S>["dispatch"] = (connection, address, command) =>
    run(connection, address, (runtime) => {
      const connected = [...live];
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
          }).pipe(Effect.mapError(() => unavailable("Invalid event frame")));

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
                connected,
                (entry) =>
                  close(
                    entry,
                    unavailable("Publication incomplete; recover from durable position"),
                  ),
                { discard: true },
              )
            : Effect.void;
        }),
      );
    });

  const publishMessage: Host<S>["publishMessage"] = (connection, address, message) =>
    run(connection, address, (runtime) =>
      Effect.gen(function* () {
        if (!entries.get(connection)?.active)
          return yield* ProtocolError.make({
            reason: "Forbidden",
            message: "Ephemeral messages require a live connection",
          });
        yield* broadcast(runtime, yield* runtime.publishMessage(connection.session, message));
      }),
    );

  const drainOutbox = finish(
    Effect.gen(function* () {
      if (!active) return yield* unavailable("Source host has closed");
      if (delivery === undefined || outbox === undefined) return;
      const now = DateTime.toEpochMillis(yield* DateTime.now);

      yield* delivery.schedule(now + delivery.retryMillis);
      const records = yield* outbox.claim(now, delivery.retryMillis, delivery.batchSize);

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

        yield* outbox.settle(record, disposition);
      }
      yield* locked(
        Effect.gen(function* () {
          const next = yield* outbox.nextTime;
          const finishedAt = DateTime.toEpochMillis(yield* DateTime.now);

          yield* delivery.schedule(next === undefined ? undefined : Math.max(finishedAt + 1, next));
        }),
      );
    }),
  );

  return {
    contract,
    connect,
    checkConnection: (connection) =>
      run(connection, connection.address, (runtime) =>
        runtime.checkSubscription(connection.session),
      ),
    snapshot: (connection, address) =>
      run(connection, address, (runtime) => runtime.snapshot(connection.session)),
    lookup: (connection, address, command) =>
      run(connection, address, (runtime) => runtime.lookup(connection.session, command)),
    subscribe,
    dispatch,
    publishMessage,
    drainOutbox,
  };
});
