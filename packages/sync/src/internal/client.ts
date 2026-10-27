import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Random from "effect/Random";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import type * as Action from "../Action.ts";
import type {
  Definition,
  Actions,
  Snapshot,
  State,
  Limits,
  Retry,
  Options,
  Runtime,
  Lease,
  Command,
  CommandStatus,
  Transition,
} from "../client/Client.ts";
import { CurrentActor } from "../client/CurrentActor.ts";
import { ClientError, type CommandRecovery, type EncodedCommand, Intent } from "../client/Model.ts";
import {
  ReplicaPersistence,
  type JournalRow,
  type PersistenceError,
} from "../client/ReplicaPersistence.ts";
import { Transport, type Request } from "../client/Transport.ts";
import {
  ActorId,
  comparePositions,
  type ActionOutcome,
  type ProtocolError,
  type ResultLookup,
  type SourceAddress,
  type WireSchema,
} from "../Model.ts";
import type * as Source from "../Source.ts";
import { copyJson, decode, encode, error, initial } from "./clientModel.ts";
import * as ReplicaState from "./replica.ts";

/** @internal The registration boundary retains the schema for every erased slot. */
export interface Slot {
  readonly id: string;
  readonly applyEvent: (snapshot: unknown, event: unknown) => unknown;
  readonly optimistic: Readonly<
    Partial<Record<string, (snapshot: unknown, payload: unknown) => unknown>>
  >;
}

type BoundAction<S extends Source.Spec> = Omit<
  Action.Bound<string, number, string, Action.Definition>,
  "execute" | "result" | "outcome" | "lookup"
> & {
  readonly outcome: WireSchema & Schema.Codec<Outcome<S>, unknown>;
  readonly lookup: WireSchema &
    Schema.Codec<ResultLookup<Action.Success<Actions<S>>, Action.Error<Actions<S>>>, unknown>;
};
type Outcome<S extends Source.Spec> = ActionOutcome<
  Action.Success<Actions<S>>,
  Action.Error<Actions<S>>
>;
type Change<T> = T extends unknown ? Omit<T, "sequence" | "replica"> : never;

interface Entry<S extends Source.Spec> {
  readonly address: SourceAddress;
  state: ReplicaState.State;
  slots: ReadonlyArray<Slot>;
  readonly lock: Semaphore.Semaphore;
  readonly commands: Semaphore.Semaphore;
  scope: Scope.Closeable | undefined;
  run: Fiber.Fiber<void> | undefined;
  readonly restart: Semaphore.Semaphore;
  leases: number;
  order: number;
  recoveryGeneration: string | undefined;
  dirty: boolean;
  readonly settled: Map<string, { readonly command: EncodedCommand; readonly outcome: Outcome<S> }>;
  readonly recoveryAttempts: Map<string, CommandRecovery>;
  messages: PubSub.PubSub<ReplicaState.Frame> | undefined;
  sequence: number;
  readonly observers: Set<Queue.Queue<Transition<S>, ClientError>>;
}

const defaults: Limits = { maxSources: 32, maxPendingPerSource: 100, frameBuffer: 256 };
const retryDefaults: Retry = { maxAttempts: 8, initialDelay: "250 millis", maxDelay: "30 seconds" };
const keyOf = (address: SourceAddress) => JSON.stringify([address.kind, address.id]);

const operationError = (failure: ProtocolError, commandId?: string) =>
  error(
    failure.reason === "Unauthenticated" || failure.reason === "Forbidden"
      ? "Unauthorized"
      : failure.reason === "AuthorityMismatch"
        ? "AuthorityChanged"
        : commandId === undefined
          ? "Disconnected"
          : "OutcomeUnknown",
    failure.message,
    commandId,
  );

export const make = Effect.fn("Client.make")(function* <S extends Source.Spec, R>(
  definition: Definition<S, R>,
  options: Options,
): Effect.fn.Return<
  Runtime<S>,
  ClientError,
  R | CurrentActor | Transport | ReplicaPersistence | Scope.Scope
> {
  const contract = definition.contract;
  const wire = contract as unknown as Source.Definition;

  const actor = yield* CurrentActor;

  const actorId = yield* Schema.decodeEffect(ActorId)(actor.actorId).pipe(
    Effect.mapError(() => error("InvalidValue", "Invalid actor id")),
  );

  const limits = options.limits ?? defaults;
  const retry = options.retry ?? retryDefaults;

  for (const bound of [
    limits.maxSources,
    limits.maxPendingPerSource,
    limits.frameBuffer,
    retry.maxAttempts,
  ]) {
    if (!Number.isSafeInteger(bound) || bound <= 0)
      return yield* error("InvalidValue", "Client bounds must be positive safe integers");
  }
  const timeout = limits.operationTimeout ?? "10 seconds";
  const heartbeat = limits.heartbeatInterval ?? "30 seconds";
  const initialDelay = Duration.toMillis(Duration.fromInputUnsafe(retry.initialDelay));
  const maxDelay = Duration.toMillis(Duration.fromInputUnsafe(retry.maxDelay));

  if (
    ![
      initialDelay,
      maxDelay,
      Duration.toMillis(Duration.fromInputUnsafe(timeout)),
      Duration.toMillis(Duration.fromInputUnsafe(heartbeat)),
    ].every((n) => Number.isFinite(n) && n > 0)
  ) {
    return yield* error("InvalidValue", "Client durations must be finite and positive");
  }

  const remote = yield* Transport;
  const storage = options.persistence.mode === "persistent" ? yield* ReplicaPersistence : undefined;
  const cache = storage?.snapshotCache;

  if (storage !== undefined && storage.actorId !== actorId)
    return yield* error("InvalidValue", "Persistence belongs to a different actor");
  const context = yield* Effect.context<R>();
  const sessionScope = yield* Effect.scope;
  const entries = new Map<string, Entry<S>>();
  const leases = Semaphore.makeUnsafe(1);
  const changed = yield* PubSub.sliding<void>({ capacity: 1, replay: 1 });

  const saves =
    cache === undefined
      ? undefined
      : yield* Queue.make<void>({ capacity: 1, strategy: "dropping" });

  yield* PubSub.publish(changed, undefined);

  const actions: ReadonlyArray<BoundAction<S>> = [
    ...Object.values(wire.actions),
    ...Object.values(wire.plugins).flatMap((p) => Object.values(p.actions)),
  ] as ReadonlyArray<BoundAction<S>>;

  const findAction = (command: EncodedCommand) =>
    actions.find(
      (action) => action.namespace === command.namespace && action.name === command.action,
    );

  const request = (address: SourceAddress): Request => ({
    protocolVersion: 1,
    address,
    schemaVersion: wire.schemaVersion,
  });

  const bounded = <A, E, Env>(effect: Effect.Effect<A, E, Env>, commandId?: string) =>
    effect.pipe(
      Effect.timeoutOrElse({
        duration: timeout,
        orElse: () =>
          Effect.fail(
            error(
              commandId === undefined ? "Timeout" : "OutcomeUnknown",
              "Client operation timed out",
              commandId,
            ),
          ),
      }),
    );

  const persistence = <A>(effect: Effect.Effect<A, PersistenceError>, commandId?: string) =>
    bounded(effect, commandId).pipe(
      Effect.mapError((failure) => error("Storage", failure.message, commandId)),
    );

  const assertOpen = (entry?: Entry<S>, sourceScope?: Scope.Closeable, leaseScope?: Scope.Scope) =>
    Effect.suspend(() =>
      sessionScope.state._tag === "Closed" ||
      leaseScope?.state._tag === "Closed" ||
      (entry !== undefined &&
        (sourceScope === undefined ||
          entry.scope !== sourceScope ||
          sourceScope.state._tag === "Closed"))
        ? Effect.fail(error("Closed", "Source or actor scope is closed"))
        : Effect.void,
    );

  const transport = <A>(effect: Effect.Effect<A, ProtocolError>, id?: string) =>
    Effect.flatMap(Effect.scope, (scope) =>
      bounded(
        effect.pipe(
          Effect.provideContext(Context.add(context, Scope.Scope, scope)),
          Effect.mapError((failure) => operationError(failure, id)),
        ),
        id,
      ),
    );

  const signal = Effect.asVoid(PubSub.publish(changed, undefined));

  const present = (entry: Entry<S>): State<S> =>
    ReplicaState.view(
      entry.state,
      (snapshot, intent) => {
        const action = findAction(intent.command);

        if (action === undefined) throw error("InvalidValue", "Missing command action");
        const command = Schema.decodeSync(Schema.toCodecJson(action.command))(intent.command);
        const slot = entry.slots.find((candidate) => candidate.id === command.namespace);

        if (slot === undefined) throw error("InvalidValue", "Missing optimistic reducer");

        const reduce = Object.hasOwn(slot.optimistic, command.action)
          ? slot.optimistic[command.action]
          : undefined;

        return reduce === undefined
          ? snapshot
          : ReplicaState.replaceSlot(
              snapshot,
              slot.id,
              reduce(ReplicaState.slotValue(snapshot, slot.id), command.payload),
            );
      },
      (id) =>
        entry.recoveryAttempts.get(id) ?? {
          attempts: 0,
          lastError: undefined,
          automaticRetryExhausted: false,
        },
    ) as State<S>;

  // Publish synchronously with the installed state. No consumer work or queue
  // backpressure may run inside a replica transaction.
  const observe = (entry: Entry<S>, change: Change<Transition<S>>) => {
    entry.sequence += 1;
    if (entry.observers.size === 0) return;
    let replica: State<S>;

    try {
      replica = present(entry);
    } catch {
      const cause = Cause.fail(error("InvalidValue", "Optimistic reducer failed"));

      for (const queue of entry.observers) {
        Queue.failCauseUnsafe(queue, cause);
        Queue.shutdownUnsafe(queue);
      }
      entry.observers.clear();

      return;
    }
    const value: Transition<S> = { ...change, sequence: entry.sequence, replica };

    for (const queue of entry.observers) {
      if (Queue.offerUnsafe(queue, value)) continue;
      // Discard the entire incomplete history, retaining a baseline at this
      // exact sequence. A later overflow can replace this baseline again.
      while (Queue.takeUnsafe(queue)?._tag === "Success") {}
      Queue.offerUnsafe(queue, {
        _tag: "Reset",
        reason: "Overflow",
        sequence: entry.sequence,
        replica,
      });
    }
  };

  const commit = Effect.fn("Client.commit")(
    function* (
      entry: Entry<S>,
      sourceScope: Scope.Closeable,
      change: (state: ReplicaState.State) => ReplicaState.State,
      leaseScope?: Scope.Scope,
      onCommit?: () => void,
      transition?: (
        previous: ReplicaState.State,
        next: ReplicaState.State,
      ) => Change<Transition<S>> | undefined,
    ) {
      yield* assertOpen(entry, sourceScope, leaseScope);
      const previous = entry.state;

      const next = yield* Effect.try({
        try: () => change(previous),
        catch: (cause) =>
          Schema.is(ClientError)(cause)
            ? cause
            : error("InvalidValue", "Client reducer or payload codec failed"),
      });

      if (next.authoritative !== undefined && next.authoritative !== previous.authoritative)
        yield* encode(wire.snapshot, next.authoritative.snapshot);
      if (storage !== undefined) {
        const updates = Array.from(next.intents).filter(
          ([id, intent]) => previous.intents.get(id) !== intent,
        );

        const removals = Array.from(previous.intents.keys()).filter((id) => !next.intents.has(id));

        if (updates.length > 0 || removals.length > 0) {
          const insert: Array<JournalRow> = [];
          const replace: Array<JournalRow> = [];

          for (const [id, intent] of updates) {
            const row = {
              address: entry.address,
              commandId: id,
              value: yield* Schema.encodeEffect(Intent)(intent).pipe(Effect.orDie),
            };

            (previous.intents.has(id) ? replace : insert).push(row);
          }
          yield* persistence(
            storage.intentJournal.commit(entry.address, { insert, replace, remove: removals }),
          );
        }
      }
      // Finish installing an admitted transaction for the surviving coordinator,
      // even when its originating lease closes while storage is committing.
      yield* assertOpen(entry, sourceScope);
      entry.state = next;
      onCommit?.();
      if (entry.observers.size === 0) {
        entry.sequence += 1;
      } else {
        const observation =
          transition !== undefined
            ? transition(previous, next)
            : next === previous
              ? undefined
              : next.authoritative !== previous.authoritative ||
                  next.provisional !== previous.provisional
                ? { _tag: "Reset" as const, reason: "Resync" as const }
                : { _tag: "StatusChanged" as const };

        if (observation !== undefined) observe(entry, observation);
      }
      if (saves !== undefined && next.authoritative !== previous.authoritative) {
        entry.dirty = true;
        yield* Queue.offer(saves, undefined);
      }
      yield* signal;
    },
    (effect, entry) => effect.pipe(Effect.uninterruptible, Semaphore.withPermit(entry.lock)),
  );

  const acceptFrame = Effect.fn("Client.frame")(function* (
    entry: Entry<S>,
    sourceScope: Scope.Closeable,
    encoded: Schema.Json,
    leaseScope?: Scope.Scope,
    replay = false,
  ) {
    const frame = yield* decode(wire.envelope.outbound, encoded);

    if (keyOf(frame.address) !== keyOf(entry.address))
      return yield* error("InvalidValue", "Frame belongs to another source");
    if (frame._tag === "Message" || frame._tag === "MessageLeave") {
      yield* assertOpen(entry, sourceScope, leaseScope);
      if (entry.messages !== undefined) yield* PubSub.publish(entry.messages, frame);

      return false;
    }
    if (frame._tag === "ResyncRequired")
      entry.recoveryGeneration = frame.position.sourceAuthorityGeneration;
    yield* commit(
      entry,
      sourceScope,
      (state) => ReplicaState.frame(state, entry.slots, actorId, frame),
      leaseScope,
      undefined,
      (previous, next) => {
        const before = previous.authoritative?.position;
        const after = next.authoritative?.position;

        const advanced =
          after !== undefined && (before === undefined || comparePositions(after, before) !== 0);

        if (advanced) {
          if (!replay && frame._tag === "Events" && before !== undefined) {
            const first = frame.events[0];
            const command = first?.command;

            if (
              command !== undefined &&
              frame.events.every(
                (event, index) =>
                  event.position.cursor === before.cursor + index + 1 &&
                  event.command?.actorId === command.actorId &&
                  event.command.commandId === command.commandId,
              )
            ) {
              return {
                _tag: "Committed",
                // The source envelope decoded the slot-specific event types.
                events: frame.events as ReadonlyArray<
                  Source.Definition<S>["envelope"]["sequenced"]["Type"]
                >,
              };
            }
          }

          return { _tag: "Reset", reason: "Resync" };
        }

        return next === previous ? undefined : { _tag: "StatusChanged" };
      },
    );
    if (entry.state.connection === "recovering")
      return yield* entry.state.error ?? error("Gap", "Source needs recovery");

    return entry.state.connection === "live";
  });

  const flushEntry = Effect.fn("Client.flushSource")(
    function* (entry: Entry<S>) {
      if (!entry.dirty || cache === undefined || entry.state.authoritative === undefined) return;
      const authority = entry.state.authoritative;

      const saved = yield* Effect.result(
        encode(wire.envelope.snapshotFrame, {
          ...request(entry.address),
          _tag: "Snapshot",
          ...authority,
        } as typeof wire.envelope.snapshotFrame.Type).pipe(
          Effect.flatMap((frame) => persistence(cache.put(entry.address, frame))),
        ),
      );

      const cacheError = saved._tag === "Failure" ? saved.failure : undefined;

      if (entry.state.cacheError !== cacheError) {
        entry.state = { ...entry.state, cacheError };
        observe(entry, { _tag: "StatusChanged" });
        yield* signal;
      }
      if (saved._tag === "Failure") return yield* saved.failure;
      if (entry.state.authoritative === authority) entry.dirty = false;
    },
    (effect, entry) => effect.pipe(Semaphore.withPermit(entry.lock)),
  );

  const flush = Effect.gen(function* () {
    yield* assertOpen();
    for (const entry of entries.values()) yield* flushEntry(entry);
  });

  const remember = (entry: Entry<S>, command: EncodedCommand, outcome: Outcome<S>) => {
    entry.settled.set(command.commandId, { command, outcome });
    while (entry.settled.size > limits.maxPendingPerSource) {
      const oldest = entry.settled.keys().next();

      if (oldest.done) break;
      entry.settled.delete(oldest.value);
    }
  };

  const restore = Effect.fn("Client.restore")(function* (
    entry: Entry<S>,
    sourceScope: Scope.Closeable,
  ) {
    if (storage === undefined) return;

    const rows = yield* persistence(
      storage.intentJournal.load(entry.address, limits.maxPendingPerSource + 1),
    );

    if (rows.length > limits.maxPendingPerSource)
      return yield* error(
        "Capacity",
        "Retained journal exceeds the pending bound; increase the configured limit",
      );
    const intents = new Map<string, Intent>();
    const quarantined = new Set<string>();

    for (const row of rows) {
      const parsed = yield* Effect.result(decode(Intent, row.value));

      if (parsed._tag === "Failure") {
        quarantined.add(row.commandId);
        continue;
      }
      const intent = parsed.success;
      const action = findAction(intent.command);
      let accepted: Outcome<S> | undefined;

      let valid =
        action === undefined
          ? false
          : (yield* Effect.result(decode(action.command, intent.command)))._tag === "Success";

      if (
        intent.command.admittedGeneration !== null &&
        intent.command.admittedGeneration !== intent.boundGeneration
      )
        valid = false;
      if (intent.phase === "Pending" && (intent.confirmedAt !== null || intent.outcome !== null))
        valid = false;
      if (
        intent.phase !== "Pending" &&
        (intent.confirmedAt === null ||
          intent.confirmedAt.sourceAuthorityGeneration !== intent.boundGeneration)
      )
        valid = false;
      if (intent.phase === "ConfirmedAwaitingResult" && intent.outcome !== null) valid = false;
      if (intent.phase === "Accepted") {
        const outcome =
          action === undefined
            ? undefined
            : yield* Effect.result(decode(action.outcome, intent.outcome));

        if (
          outcome?._tag !== "Success" ||
          outcome.success._tag !== "Succeeded" ||
          outcome.success.position.sourceAuthorityGeneration !== intent.boundGeneration ||
          outcome.success.position.cursor !== intent.confirmedAt?.cursor
        )
          valid = false;
        else accepted = outcome.success;
      }

      if (
        !valid ||
        intent.command.commandId !== row.commandId ||
        keyOf(intent.command.address) !== keyOf(entry.address)
      ) {
        quarantined.add(row.commandId);
        continue;
      }
      intents.set(row.commandId, intent);
      // Hydration may retire this journal row before a restored handle reads it.
      if (accepted !== undefined) remember(entry, intent.command, accepted);
      entry.order = Math.max(entry.order, intent.order + 1);
    }
    yield* assertOpen(entry, sourceScope);
    entry.state = { ...entry.state, intents, quarantined };
    observe(entry, { _tag: "Reset", reason: "Resync" });
    yield* signal;
  });

  const restoreCache = Effect.fn("Client.restoreCache")(function* (
    entry: Entry<S>,
    sourceScope: Scope.Closeable,
  ) {
    if (cache === undefined) return;
    const cached = yield* persistence(cache.get(entry.address));

    if (cached === undefined) {
      if (entry.state.cacheError !== undefined)
        yield* commit(entry, sourceScope, (state) => ({ ...state, cacheError: undefined }));

      return;
    }
    const result = yield* Effect.result(decode(wire.envelope.snapshotFrame, cached));

    if (result._tag === "Failure" || keyOf(result.success.address) !== keyOf(entry.address)) {
      yield* persistence(cache.remove(entry.address));
      if (entry.state.cacheError !== undefined)
        yield* commit(entry, sourceScope, (state) => ({ ...state, cacheError: undefined }));

      return;
    }
    yield* commit(entry, sourceScope, (state) =>
      state.authoritative === undefined
        ? {
            ...state,
            provisional: result.success.snapshot,
            provisionalPosition: result.success.position,
            cacheError: undefined,
          }
        : { ...state, cacheError: undefined },
    );
  });

  const recordFailure = Effect.fn("Client.failure")(function* (
    entry: Entry<S>,
    sourceScope: Scope.Closeable,
    failure: ClientError,
  ) {
    yield* commit(entry, sourceScope, (state) => ({
      ...state,
      error: failure,
      connection:
        failure.reason === "Unauthorized" ||
        failure.reason === "AuthorityChanged" ||
        failure.reason === "InvalidValue" ||
        failure.reason === "Storage"
          ? "parked"
          : "recovering",
    }));
  });

  const settle = Effect.fn("Client.settle")(function* (
    entry: Entry<S>,
    sourceScope: Scope.Closeable,
    action: BoundAction<S>,
    intent: Intent,
    outcome: Outcome<S>,
    leaseScope?: Scope.Scope,
  ) {
    const id = intent.command.commandId;

    if (outcome._tag === "Succeeded") {
      const generation = entry.state.authoritative?.position.sourceAuthorityGeneration;

      if (
        (intent.boundGeneration !== null &&
          outcome.position.sourceAuthorityGeneration !== intent.boundGeneration) ||
        (generation !== undefined && generation !== outcome.position.sourceAuthorityGeneration)
      ) {
        return yield* error(
          "AuthorityChanged",
          "The result belongs to a different source authority",
          id,
        );
      }
    }
    const encoded = outcome._tag === "Succeeded" ? yield* encode(action.outcome, outcome) : null;

    yield* commit(
      entry,
      sourceScope,
      (state) => {
        const intents = new Map(state.intents);

        if (outcome._tag === "Rejected") {
          intents.delete(id);

          return {
            ...state,
            intents,
            failures: [
              ...state.failures.filter((f) => f.commandId !== id),
              { commandId: id, error: outcome.error },
            ].slice(-limits.maxPendingPerSource),
          };
        }
        intents.set(id, {
          ...intent,
          phase: "Accepted",
          boundGeneration: outcome.position.sourceAuthorityGeneration,
          confirmedAt: outcome.position,
          outcome: copyJson(encoded),
        });

        return ReplicaState.reflected({ ...state, intents });
      },
      leaseScope,
      () => remember(entry, intent.command, outcome),
      () => ({ _tag: "Settled", command: copyJson(intent.command), outcome }),
    );

    return outcome;
  });

  const send = Effect.fn("Client.send")(
    function* (
      entry: Entry<S>,
      sourceScope: Scope.Closeable,
      id: string,
      lookup: boolean,
      leaseScope?: Scope.Scope,
    ) {
      yield* assertOpen(entry, sourceScope, leaseScope);
      const remembered = entry.settled.get(id);

      if (remembered !== undefined) return remembered.outcome;
      if (entry.state.quarantined.has(id))
        return yield* error(
          "Quarantined",
          "The retained command cannot be decoded; its identity remains reserved",
          id,
        );
      const intent = entry.state.intents.get(id);

      if (intent === undefined)
        return yield* error("NotPending", "No retained command has this id", id);
      const generation = entry.state.authoritative?.position.sourceAuthorityGeneration;

      if (lookup && intent.boundGeneration !== null && generation === undefined)
        return yield* error("AuthorityUnknown", "Recover authority before retrying", id);
      if (
        intent.boundGeneration !== null &&
        generation !== undefined &&
        generation !== intent.boundGeneration
      )
        return yield* error(
          "AuthorityChanged",
          "Old commands require reconciliation after authority replacement",
          id,
        );
      const action = findAction(intent.command);

      if (action === undefined) throw error("InvalidValue", "Missing command action");

      if (intent.outcome !== null) return yield* decode(action.outcome, intent.outcome);
      if (lookup) {
        const result = yield* transport(remote.result(copyJson(intent.command)), id).pipe(
          Effect.flatMap((value) => decode(action.lookup, value)),
        );

        if (result._tag === "Found")
          return yield* settle(entry, sourceScope, action, intent, result.outcome, leaseScope);
        if (
          result._tag === "Expired" ||
          intent.boundGeneration === null ||
          intent.phase === "ConfirmedAwaitingResult"
        ) {
          return yield* error(
            "OutcomeUnknown",
            "The exact result remains unresolved; the original identity is retained",
            id,
          );
        }
      }
      yield* assertOpen(entry, sourceScope, leaseScope);

      const outcome = yield* transport(remote.execute(copyJson(intent.command)), id).pipe(
        Effect.flatMap((value) => decode(action.outcome, value)),
      );

      return yield* settle(entry, sourceScope, action, intent, outcome, leaseScope);
    },
    (effect, entry) => effect.pipe(Semaphore.withPermit(entry.commands)),
  );

  const unwrap = (
    outcome: Outcome<S>,
  ): Effect.Effect<Action.Success<Actions<S>>, Action.Error<Actions<S>>> =>
    outcome._tag === "Succeeded" ? Effect.succeed(outcome.result) : Effect.fail(outcome.error);

  const hydrate = Effect.fn("Client.hydrate")(function* (
    entry: Entry<S>,
    sourceScope: Scope.Closeable,
  ) {
    const value = yield* transport(remote.snapshot(request(entry.address)));

    const frame = yield* decode(wire.envelope.snapshotFrame, value);

    if (entry.recoveryGeneration === frame.position.sourceAuthorityGeneration) {
      yield* acceptFrame(entry, sourceScope, {
        ...(value as Record<string, Schema.Json>),
        _tag: "Reset",
      });
      entry.recoveryGeneration = undefined;
    } else {
      yield* acceptFrame(entry, sourceScope, value);
    }
  });

  const connection = Effect.fn("Client.connection")(function* (
    entry: Entry<S>,
    sourceScope: Scope.Closeable,
    recovered: () => void,
  ) {
    const connectionScope = yield* Effect.scope;

    if (entry.state.connection === "recovering" || entry.state.connection === "parked")
      yield* hydrate(entry, sourceScope);
    yield* commit(entry, sourceScope, (state) => ({
      ...state,
      connection: state.authoritative === undefined ? "connecting" : "recovering",
      error: undefined,
    }));
    const after = entry.state.authoritative?.position;

    const frames = yield* Queue.make<Schema.Json, ClientError>({
      capacity: limits.frameBuffer,
      strategy: "dropping",
    });

    const producer = remote
      .subscribe({ ...request(entry.address), ...(after === undefined ? {} : { after }) })
      .pipe(
        Stream.provideContext(Context.add(context, Scope.Scope, connectionScope)),
        Stream.mapError((failure) => operationError(failure)),
        Stream.runForEach((frame) =>
          Queue.offer(frames, frame).pipe(
            Effect.flatMap((accepted) =>
              accepted
                ? Effect.void
                : Effect.fail(error("Overflow", "Source frame buffer overflowed")),
            ),
          ),
        ),
        Effect.andThen(Effect.fail(error("Disconnected", "Subscription ended"))),
        Effect.catch((failure) => Queue.fail(frames, failure)),
      );

    yield* Effect.forkScoped(producer, { startImmediately: true });

    let replay = true;

    const consume = Stream.fromQueue(frames).pipe(
      Stream.runForEach((frame) =>
        acceptFrame(entry, sourceScope, frame, undefined, replay).pipe(
          Effect.tap((healthy) =>
            Effect.sync(() => {
              if (healthy) {
                replay = false;
                recovered();
              }
            }),
          ),
        ),
      ),
    );

    const heartbeatLoop = Effect.gen(function* () {
      yield* Effect.sleep(heartbeat);
      yield* hydrate(entry, sourceScope);
    }).pipe(Effect.forever);

    const reconcileLoop = Effect.gen(function* () {
      // Wait for authority, then bound automatic recovery independently of heartbeat probes.
      if (entry.state.authoritative === undefined) {
        yield* Stream.fromPubSub(changed).pipe(
          Stream.filter(() => entry.state.authoritative !== undefined),
          Stream.take(1),
          Stream.runDrain,
        );
      }
      for (const [id, intent] of entry.state.intents) {
        if (intent.outcome !== null) continue;
        const attempts = entry.recoveryAttempts.get(id)?.attempts ?? 0;

        if (attempts >= retry.maxAttempts) continue;

        const recovery: CommandRecovery = {
          attempts: attempts + 1,
          lastError: entry.recoveryAttempts.get(id)?.lastError,
          automaticRetryExhausted: false,
        };

        entry.recoveryAttempts.set(id, recovery);
        observe(entry, { _tag: "StatusChanged" });
        yield* signal;
        yield* send(entry, sourceScope, id, true).pipe(
          Effect.onExit((exit) =>
            Effect.gen(function* () {
              // Explicit recovery can clear diagnostics while this attempt is interrupted.
              if (!entry.state.intents.has(id) || entry.recoveryAttempts.get(id) !== recovery)
                return;
              entry.recoveryAttempts.set(id, {
                ...recovery,
                lastError: Exit.isFailure(exit)
                  ? exit.cause.reasons.find((reason) => reason._tag === "Fail")?.error
                  : undefined,
                automaticRetryExhausted:
                  Exit.isFailure(exit) && recovery.attempts >= retry.maxAttempts,
              });
              observe(entry, { _tag: "StatusChanged" });
              yield* signal;
            }),
          ),
          Effect.catchIf(
            (failure) => failure.reason !== "Unauthorized",
            () => Effect.void,
          ),
        );
      }
      for (const id of entry.recoveryAttempts.keys()) {
        if (!entry.state.intents.has(id)) entry.recoveryAttempts.delete(id);
      }
      yield* Effect.sleep(heartbeat);
    }).pipe(Effect.forever);

    yield* Effect.all([consume, heartbeatLoop, reconcileLoop], { concurrency: 3 });
  }, Effect.scoped);

  const run = Effect.fn("Client.coordinate")(function* (
    entry: Entry<S>,
    sourceScope: Scope.Closeable,
  ) {
    let failures = 0;

    while (failures < retry.maxAttempts) {
      const result = yield* Effect.result(
        connection(entry, sourceScope, () => {
          failures = 0;
        }),
      );

      if (result._tag === "Success") return;
      yield* recordFailure(entry, sourceScope, result.failure);
      if (entry.state.connection === "parked") return;
      failures += 1;
      if (failures < retry.maxAttempts)
        yield* Effect.sleep(Math.min(maxDelay, initialDelay * 2 ** (failures - 1)));
    }
    yield* commit(entry, sourceScope, (state) => ({ ...state, connection: "parked" }));
  });

  const start = Effect.fn("Client.start")(
    function* (entry: Entry<S>, sourceScope: Scope.Closeable, leaseScope?: Scope.Scope) {
      yield* assertOpen(entry, sourceScope, leaseScope);
      if (entry.run !== undefined) yield* Fiber.interrupt(entry.run);
      yield* commit(
        entry,
        sourceScope,
        (state) => ({
          ...state,
          connection: state.authoritative === undefined ? "connecting" : "recovering",
          error: undefined,
        }),
        leaseScope,
      );
      entry.run = yield* run(entry, sourceScope).pipe(
        Effect.catch((failure) =>
          failure.reason === "Closed"
            ? Effect.void
            : recordFailure(entry, sourceScope, failure).pipe(Effect.ignore),
        ),
        Effect.provideContext(Context.add(context, Scope.Scope, sourceScope)),
        Effect.forkIn(sourceScope),
      );
    },
    (effect, entry) => effect.pipe(Effect.uninterruptible, Semaphore.withPermit(entry.restart)),
  );

  const read = Effect.fn("Client.read")(function* (
    address: SourceAddress,
  ): Effect.fn.Return<State<S>, ClientError> {
    yield* decode(wire.addressSchema, address);
    if (sessionScope.state._tag === "Closed")
      return initial<Snapshot<S>, Action.Error<Actions<S>>>("closed");
    const entry = entries.get(keyOf(address));

    return entry === undefined
      ? initial<Snapshot<S>, Action.Error<Actions<S>>>()
      : yield* Effect.try({
          try: () => present(entry),
          catch: () => error("InvalidValue", "Optimistic reducer failed"),
        });
  });

  const changes = (address: SourceAddress) =>
    Stream.fromPubSub(changed).pipe(Stream.mapEffect(() => read(address)));

  const open = Effect.fn("Client.open")(function* (
    input: Source.Address<S>,
  ): Effect.fn.Return<Lease<S>, ClientError, Scope.Scope> {
    const address = yield* decode(wire.addressSchema, input);
    const leaseScope = yield* Effect.scope;

    const entry = yield* Effect.acquireRelease(
      Effect.gen(function* () {
        yield* assertOpen();
        const key = keyOf(address);
        let entry = entries.get(key);

        if (entry === undefined) {
          if (entries.size >= limits.maxSources) {
            const disposable = Array.from(entries).find(
              ([, candidate]) =>
                candidate.leases === 0 &&
                candidate.state.intents.size === 0 &&
                candidate.state.quarantined.size === 0,
            );

            if (disposable === undefined)
              return yield* error(
                "Capacity",
                "Source capacity exhausted; unresolved work is never evicted",
              );
            entries.delete(disposable[0]);
          }
          entry = {
            address,
            state: ReplicaState.empty(),
            slots: [],
            lock: Semaphore.makeUnsafe(1),
            commands: Semaphore.makeUnsafe(1),
            restart: Semaphore.makeUnsafe(1),
            scope: undefined,
            run: undefined,
            leases: 0,
            order: 0,
            recoveryGeneration: undefined,
            dirty: false,
            settled: new Map(),
            recoveryAttempts: new Map(),
            messages: undefined,
            sequence: 0,
            observers: new Set(),
          };
          entries.set(key, entry);
        }
        if (entry.leases === 0) {
          const sourceScope = yield* Scope.fork(sessionScope);

          entry.scope = sourceScope;
          const active = entry;

          const acquired = yield* Effect.result(
            Effect.gen(function* () {
              if (storage?.intentJournal.acquire !== undefined)
                yield* persistence(
                  storage.intentJournal.acquire(address).pipe(Scope.provide(sourceScope)),
                );
              active.slots = yield* definition.acquire.pipe(
                Scope.provide(sourceScope),
                Effect.provideContext(context),
              );
              active.messages = yield* PubSub.sliding<ReplicaState.Frame>({
                capacity: limits.frameBuffer,
              });
              yield* Scope.addFinalizer(sourceScope, PubSub.shutdown(active.messages));
              // Reopening reloads commits that finished journaling after the old scope was fenced.
              yield* restore(active, sourceScope);
              yield* restoreCache(active, sourceScope).pipe(
                Effect.catch((failure) =>
                  commit(active, sourceScope, (state) => ({ ...state, cacheError: failure })).pipe(
                    Effect.ignore,
                  ),
                ),
                Effect.forkIn(sourceScope),
              );
              yield* start(active, sourceScope);
            }),
          );

          if (acquired._tag === "Failure") {
            yield* Scope.close(entry.scope, Exit.void);
            entry.scope = undefined;

            return yield* acquired.failure;
          }
        }
        entry.leases += 1;

        return entry;
      }).pipe(Semaphore.withPermit(leases)),
      (entry) =>
        Effect.gen(function* () {
          entry.leases -= 1;
          if (entry.leases !== 0) return;
          const scope = entry.scope;

          entry.scope = undefined;
          if (scope !== undefined) yield* Scope.close(scope, Exit.void);
          entry.run = undefined;
          entry.messages = undefined;
          entry.state = {
            ...entry.state,
            connection: sessionScope.state._tag === "Closed" ? "closed" : "idle",
          };
          yield* signal;
        }).pipe(Semaphore.withPermit(leases)),
    );

    const sourceScope = entry.scope;
    const messages = entry.messages;

    if (sourceScope === undefined || messages === undefined)
      return yield* error("Closed", "Source is closed");

    const operationsScope = yield* Scope.fork(sourceScope);

    yield* Scope.addFinalizerExit(leaseScope, (exit) =>
      Scope.close(operationsScope, exit).pipe(Effect.ensuring(signal)),
    );

    const active = Effect.suspend(() =>
      leaseScope.state._tag === "Closed"
        ? Effect.fail(error("Closed", "Source lease is closed"))
        : assertOpen(entry, sourceScope),
    );

    const owned = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>) =>
      Effect.gen(function* () {
        yield* active;

        const fiber = yield* effect.pipe(
          Effect.provideContext(Context.add(context, Scope.Scope, operationsScope)),
          Effect.forkIn(operationsScope),
        );

        return yield* Fiber.join(fiber).pipe(Effect.onInterrupt(() => Fiber.interrupt(fiber)));
      });

    const admit = (action: BoundAction<S>, payload: unknown) =>
      owned(
        Effect.gen(function* () {
          if (!actions.includes(action))
            return yield* error("InvalidValue", "Action belongs to another source contract");

          const id = yield* Effect.forEach([0, 1, 2, 3], () =>
            Random.nextInt.pipe(Effect.map((n) => (n >>> 0).toString(16).padStart(8, "0"))),
          ).pipe(Effect.map((parts) => parts.join("")));

          yield* commit(
            entry,
            sourceScope,
            (state) => {
              if (
                state.connection === "parked" &&
                state.error !== undefined &&
                !["Disconnected", "Timeout", "OutcomeUnknown"].includes(state.error.reason)
              )
                throw (
                  state.error ??
                  error("Disconnected", "Recover the parked source before dispatching")
                );
              if (state.intents.size + state.quarantined.size >= limits.maxPendingPerSource)
                throw error("Capacity", "Pending command capacity exhausted");

              const generation =
                (state.authoritative?.position ?? state.provisionalPosition)
                  ?.sourceAuthorityGeneration ?? null;

              if (generation === null && !definition.allowUnboundCommands)
                throw error("AuthorityUnknown", "Wait for authority before executing commands");

              const command: EncodedCommand = {
                ...request(address),
                commandId: id,
                admittedGeneration: generation,
                namespace: action.namespace,
                action: action.name,
                payload: Schema.encodeSync(Schema.toCodecJson(action.payload))(payload),
              };

              const intents = new Map(state.intents);

              intents.set(id, {
                formatVersion: 1,
                command: copyJson(command),
                order: entry.order++,
                phase: "Pending",
                boundGeneration: generation,
                confirmedAt: null,
                outcome: null,
              });

              return { ...state, intents };
            },
            leaseScope,
            undefined,
            (_previous, next) => {
              const admitted = next.intents.get(id);

              return admitted === undefined
                ? undefined
                : { _tag: "Admitted", command: copyJson(admitted.command) };
            },
          );

          return id;
        }),
      );

    const execute = (action: BoundAction<S>, payload: unknown) =>
      owned(
        Effect.gen(function* () {
          const id = yield* admit(action, payload);

          return yield* send(entry, sourceScope, id, false, leaseScope).pipe(
            Effect.mapError((failure) => error(failure.reason, failure.message, id)),
            Effect.flatMap(unwrap),
          );
        }),
      );

    const enqueue = (action: BoundAction<S>, payload: unknown) =>
      owned(
        Effect.gen(function* () {
          const id = yield* admit(action, payload);

          yield* send(entry, sourceScope, id, false).pipe(
            Effect.catch((failure) =>
              failure.reason === "Closed"
                ? Effect.void
                : recordFailure(entry, sourceScope, failure),
            ),
            Effect.forkIn(sourceScope),
          );

          return id;
        }),
      );

    const retryCommand = (id: string) =>
      owned(
        send(entry, sourceScope, id, true, leaseScope).pipe(
          Effect.mapError((failure) => error(failure.reason, failure.message, id)),
          Effect.flatMap(unwrap),
        ),
      );

    const retained = Effect.fnUntraced(function* (id: string) {
      yield* active;
      if (entry.state.quarantined.has(id))
        return yield* error("Quarantined", "The retained command cannot be decoded", id);
      const evidence = entry.settled.get(id) ?? entry.state.intents.get(id);

      if (evidence === undefined)
        return yield* error("NotPending", "Command evidence is missing or no longer retained", id);

      return evidence;
    });

    const commandStatus = Effect.fnUntraced(function* (
      id: string,
    ): Effect.fn.Return<CommandStatus<Actions<S>>, ClientError> {
      const evidence = yield* retained(id);

      if (!("phase" in evidence)) return evidence.outcome;
      const generation = entry.state.authoritative?.position.sourceAuthorityGeneration;

      if (
        evidence.boundGeneration !== null &&
        generation !== undefined &&
        generation !== evidence.boundGeneration
      )
        return yield* error("AuthorityChanged", "The command belongs to a replaced authority", id);
      if (evidence.phase === "Accepted") {
        const action = findAction(evidence.command);

        if (action === undefined) return yield* error("InvalidValue", "Missing command action", id);

        return yield* decode(action.outcome, evidence.outcome);
      }

      return { _tag: evidence.phase };
    });

    const command = (id: string): Command<Actions<S>> => {
      const observations = Stream.unwrap(active.pipe(Effect.as(Stream.fromPubSub(changed)))).pipe(
        Stream.mapEffect(() => commandStatus(id)),
        Stream.changesWith((previous, next) => previous._tag === next._tag),
        Stream.takeUntil((status) => status._tag === "Succeeded" || status._tag === "Rejected"),
      );

      return {
        id,
        changes: observations,
        awaitResult: owned(
          observations.pipe(
            Stream.filter(
              (status): status is Outcome<S> =>
                status._tag === "Succeeded" || status._tag === "Rejected",
            ),
            Stream.runHead,
            Effect.flatMap((value) =>
              value._tag === "Some"
                ? Effect.succeed(value.value)
                : Effect.fail(error("Closed", "Source is closed", id)),
            ),
            Effect.flatMap(unwrap),
          ),
        ),
        retry: retryCommand(id),
      };
    };

    const submit = (action: BoundAction<S>, payload: unknown) =>
      enqueue(action, payload).pipe(Effect.map(command));

    const restoreCommand = (action: BoundAction<S>, id: string) =>
      owned(
        Effect.gen(function* () {
          if (!actions.includes(action))
            return yield* error("InvalidValue", "Action belongs to another source contract", id);
          const evidence = yield* retained(id);

          if (findAction(evidence.command) !== action)
            return yield* error("InvalidValue", "Retained command belongs to another action", id);
          yield* decode(action.command, evidence.command);

          return command(id);
        }),
      );

    const publish = (namespace: string, message: unknown) =>
      owned(
        Effect.gen(function* () {
          const encoded = yield* encode(wire.message, {
            namespace,
            payload: message,
          } as typeof wire.message.Type);

          yield* active;
          yield* transport(remote.publishMessage({ ...request(address), message: encoded }));
        }),
      );

    const transitions: Lease<S>["transitions"] = (options) =>
      Stream.unwrap(
        Effect.gen(function* () {
          yield* active;
          const capacity = options?.capacity ?? 256;

          if (!Number.isSafeInteger(capacity) || capacity <= 0)
            return yield* error(
              "InvalidValue",
              "Transition capacity must be a positive safe integer",
            );

          const queue = yield* Queue.make<Transition<S>, ClientError>({
            capacity,
            strategy: "dropping",
          });

          const subscriptionScope = yield* Scope.fork(operationsScope);

          yield* Effect.addFinalizer((exit) => Scope.close(subscriptionScope, exit));
          yield* Scope.addFinalizer(
            subscriptionScope,
            Effect.sync(() => {
              entry.observers.delete(queue);
              Queue.failCauseUnsafe(queue, Cause.fail(error("Closed", "Source lease is closed")));
              Queue.shutdownUnsafe(queue);
            }),
          );

          const baseline = yield* Effect.gen(function* () {
            yield* active;

            return yield* Effect.try({
              try: (): Transition<S> => {
                const replica = present(entry);

                entry.observers.add(queue);

                return { _tag: "Reset", reason: "Initial", sequence: entry.sequence, replica };
              },
              catch: () => error("InvalidValue", "Optimistic reducer failed"),
            });
          }).pipe(Semaphore.withPermit(entry.lock));

          // Keep the initial baseline ahead of the bounded queue and pull one item
          // at a time, so stream batching cannot retain a second hidden history.
          return Stream.make(baseline).pipe(
            Stream.concat(Stream.fromEffectRepeat(Queue.take(queue))),
            Stream.mapEffect((value) => active.pipe(Effect.as(value))),
          );
        }),
      );

    return {
      read: active.pipe(Effect.andThen(read(address))),
      changes: changes(address).pipe(Stream.mapEffect((state) => active.pipe(Effect.as(state)))),
      transitions,
      ready: changes(address).pipe(
        Stream.mapEffect((state) =>
          state.connection === "parked"
            ? Effect.fail(state.error ?? error("Disconnected", "Source is parked"))
            : active.pipe(Effect.as(state)),
        ),
        Stream.filter((state) => state.authoritative !== undefined && state.connection === "live"),
        Stream.take(1),
        Stream.runHead,
        Effect.flatMap((value) =>
          value._tag === "Some" && value.value.authoritative !== undefined
            ? Effect.succeed(value.value.authoritative.snapshot)
            : Effect.fail(error("Closed", "Source is closed")),
        ),
      ),
      execute: execute as Lease<S>["execute"],
      enqueue: enqueue as Lease<S>["enqueue"],
      submit: submit as Lease<S>["submit"],
      restore: restoreCommand as Lease<S>["restore"],
      retry: retryCommand,
      recover: owned(
        Effect.sync(() => entry.recoveryAttempts.clear()).pipe(
          Effect.andThen(start(entry, sourceScope, leaseScope)),
        ),
      ),
      publishMessage: (message) => publish("$source", message),
      publishPluginMessage: publish,
      messages: Stream.fromPubSub(messages).pipe(
        Stream.mapEffect((frame) => active.pipe(Effect.as(frame))),
      ) as Lease<S>["messages"],
      dismissFailure: (id) =>
        owned(
          commit(
            entry,
            sourceScope,
            (state) => ({
              ...state,
              failures: state.failures.filter((f) => f.commandId !== id),
            }),
            leaseScope,
          ),
        ),
    };
  });

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      entries.clear();
      yield* signal;
      yield* PubSub.shutdown(changed);
      if (saves !== undefined) yield* Queue.shutdown(saves);
    }),
  );
  if (saves !== undefined)
    yield* Effect.gen(function* () {
      while (true) {
        yield* Queue.take(saves);
        yield* Effect.sleep("100 millis");
        yield* flush.pipe(Effect.ignore);
      }
    }).pipe(Effect.forkScoped);

  return { actorId, contract, open, read, changes, flush };
});
