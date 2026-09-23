import {
  Context,
  Duration,
  Effect,
  Exit,
  Fiber,
  PubSub,
  Queue,
  Random,
  Schema,
  Scope,
  Semaphore,
  Stream,
} from "effect";

import type * as Action from "../Action.ts";
import { ActorId, type ActionOutcome, type ProtocolError, type SourceAddress } from "../Model.ts";
import type * as Source from "../Source.ts";
import type { Definition, Slot } from "./Definition.ts";
import {
  ClientError,
  type EncodedCommand,
  Intent,
  copyJson,
  decode,
  encode,
  error,
  initial,
  type Replica,
} from "./Model.ts";
import * as ReplicaState from "./replica.ts";
import { type Handle, type PersistenceError } from "./ReplicaPersistence.ts";
import type { Request, Transport } from "./Transport.ts";

type SourceActions<S extends Source.Spec> =
  | Action.Bound<S["kind"], S["schemaVersion"], "$source", S["actions"][number]>
  | {
      [P in S["plugins"][number] as P["id"]]: Action.Bound<
        S["kind"],
        S["schemaVersion"],
        P["id"],
        P["spec"]["actions"][number]
      >;
    }[S["plugins"][number]["id"]];
export type Actions<S extends Source.Spec> = Extract<SourceActions<S>, BoundAction>;
export type Snapshot<S extends Source.Spec> = Source.Definition<S>["snapshot"]["Type"];
export type State<S extends Source.Spec> = Replica<Snapshot<S>, Action.Error<Actions<S>>>;
type BoundAction = Omit<
  Action.Bound<string, number, string, Action.Definition>,
  "execute" | "result"
>;
type Outcome = ActionOutcome<unknown, unknown>;

export interface Limits {
  readonly maxSources: number;
  readonly maxPendingPerSource: number;
  readonly frameBuffer: number;
  readonly operationTimeout?: Duration.Input;
  readonly heartbeatInterval?: Duration.Input;
}

export interface Retry {
  readonly maxAttempts: number;
  readonly initialDelay: Duration.Input;
  readonly maxDelay: Duration.Input;
}

export interface Options<R = never> {
  readonly actorId: string;
  readonly transport: Transport<R>;
  readonly persistence:
    | { readonly mode: "volatile" }
    | { readonly mode: "persistent"; readonly storage: Handle };
  readonly limits?: Limits;
  readonly retry?: Retry;
}

export interface Lease<S extends Source.Spec> {
  readonly read: Effect.Effect<State<S>, ClientError>;
  readonly changes: Stream.Stream<State<S>, ClientError>;
  readonly ready: Effect.Effect<Snapshot<S>, ClientError>;
  readonly execute: <A extends Actions<S>>(
    action: A,
    payload: Action.Payload<NoInfer<A>>,
  ) => Effect.Effect<Action.Success<A>, Action.Error<A> | ClientError>;
  /** A bare id can belong to any action in this source; results/errors retain that union. */
  readonly retry: (
    commandId: string,
  ) => Effect.Effect<Action.Success<Actions<S>>, Action.Error<Actions<S>> | ClientError>;
  readonly recover: Effect.Effect<void, ClientError>;
  readonly publishMessage: (message: S["message"]["Type"]) => Effect.Effect<void, ClientError>;
  readonly publishPluginMessage: <Id extends S["plugins"][number]["id"]>(
    id: Id,
    message: Extract<S["plugins"][number], { readonly id: Id }>["message"]["Type"],
  ) => Effect.Effect<void, ClientError>;
  readonly messages: Stream.Stream<
    Source.Definition<S>["envelope"]["messageFrame" | "leaveFrame"]["Type"],
    ClientError
  >;
  readonly hydrate: (
    snapshot: Source.Definition<S>["envelope"]["snapshotFrame"]["Type"],
  ) => Effect.Effect<void, ClientError>;
  readonly mergeObservation: (
    merge: (current: Snapshot<S> | undefined) => Snapshot<S>,
  ) => Effect.Effect<void, ClientError>;
  readonly applyHistory: (
    generation: string,
    snapshot: Snapshot<S>,
  ) => Effect.Effect<void, ClientError>;
  readonly dismissFailure: (commandId: string) => Effect.Effect<void, ClientError>;
}

export interface Runtime<S extends Source.Spec> {
  readonly actorId: string;
  readonly contract: Source.Definition<S>;
  readonly open: (address: {
    readonly kind: S["kind"];
    readonly id: string;
  }) => Effect.Effect<Lease<S>, ClientError, Scope.Scope>;
  readonly read: (address: SourceAddress) => Effect.Effect<State<S>, ClientError>;
  /** Passive observations never acquire a source lease. Slow observers coalesce views. */
  readonly changes: (address: SourceAddress) => Stream.Stream<State<S>, ClientError>;
  readonly flush: Effect.Effect<void, ClientError>;
}

interface Entry {
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
  readonly settled: Map<string, Outcome>;
  readonly recoveryAttempts: Map<string, number>;
  messages: PubSub.PubSub<ReplicaState.Frame> | undefined;
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

export const make = Effect.fn("Client.make")(function* <S extends Source.Spec, R, T>(
  definition: Definition<S, R>,
  options: Options<T>,
): Effect.fn.Return<Runtime<S>, ClientError, R | T | Scope.Scope> {
  const contract = definition.contract;
  const wire = contract as unknown as Source.Definition;

  const actorId = yield* Schema.decodeEffect(ActorId)(options.actorId).pipe(
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

  const storage =
    options.persistence.mode === "persistent" ? options.persistence.storage : undefined;

  if (storage !== undefined && storage.actorId !== actorId)
    return yield* error("InvalidValue", "Persistence belongs to a different actor");
  const context = yield* Effect.context<R | T>();
  const sessionScope = yield* Effect.scope;
  const entries = new Map<string, Entry>();
  const leases = Semaphore.makeUnsafe(1);
  const changed = yield* PubSub.sliding<void>({ capacity: 1, replay: 1 });
  const saves = yield* Queue.make<void>({ capacity: 1, strategy: "dropping" });

  yield* PubSub.publish(changed, undefined);

  const actions: ReadonlyArray<BoundAction> = [
    ...Object.values(wire.actions),
    ...Object.values(wire.plugins).flatMap((p) => Object.values(p.actions)),
  ];

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

  const assertOpen = (entry?: Entry, sourceScope?: Scope.Closeable, leaseScope?: Scope.Scope) =>
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

  const transport = <A>(effect: Effect.Effect<A, ProtocolError, T>, id?: string) =>
    Effect.flatMap(Effect.scope, (scope) =>
      bounded(
        effect.pipe(
          Effect.provideContext(Context.add(context, Scope.Scope, scope)),
          Effect.mapError((failure) => operationError(failure, id)),
        ),
        id,
      ),
    );

  const signal = Effect.gen(function* () {
    yield* PubSub.publish(changed, undefined);
  });

  const present = (entry: Entry): State<S> =>
    ReplicaState.view(entry.state, (snapshot, intent) => {
      const action = findAction(intent.command);

      if (action === undefined) throw error("InvalidValue", "Missing command action");
      const command = Schema.decodeSync(Schema.toCodecJson(action.command))(intent.command);
      const slot = entry.slots.find((candidate) => candidate.id === command.namespace);

      if (slot === undefined) throw error("InvalidValue", "Missing optimistic reducer");

      return ReplicaState.replaceSlot(
        snapshot,
        slot.id,
        slot.optimistic[command.action](ReplicaState.slotValue(snapshot, slot.id), command.payload),
      );
    }) as State<S>;

  const commit = Effect.fn("Client.commit")(
    function* (
      entry: Entry,
      sourceScope: Scope.Closeable,
      change: (state: ReplicaState.State) => ReplicaState.State,
      leaseScope?: Scope.Scope,
      onCommit?: () => void,
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

        if (updates.length > 0 || removals.length > 0)
          yield* persistence(
            storage.intentJournal.transaction((tx) =>
              Effect.gen(function* () {
                for (const [id, intent] of updates) {
                  const row = {
                    address: entry.address,
                    commandId: id,
                    value: Schema.encodeSync(Intent)(intent),
                  };

                  yield* previous.intents.has(id) ? tx.replace(row) : tx.put(row);
                }
                for (const id of removals) yield* tx.remove(entry.address, id);
              }),
            ),
          );
      }
      // Finish installing an admitted transaction for the surviving coordinator,
      // even when its originating lease closes while storage is committing.
      yield* assertOpen(entry, sourceScope);
      entry.state = next;
      onCommit?.();
      if (next.authoritative !== previous.authoritative) {
        entry.dirty = true;
        yield* Queue.offer(saves, undefined);
      }
      yield* signal;
    },
    (effect, entry) => effect.pipe(Effect.uninterruptible, Semaphore.withPermit(entry.lock)),
  );

  const acceptFrame = Effect.fn("Client.frame")(function* (
    entry: Entry,
    sourceScope: Scope.Closeable,
    encoded: Schema.Json,
    leaseScope?: Scope.Scope,
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
    );
    if (entry.state.connection === "recovering")
      return yield* entry.state.error ?? error("Gap", "Source needs recovery");

    return entry.state.connection === "live";
  });

  const flushEntry = Effect.fn("Client.flushSource")(
    function* (entry: Entry) {
      if (!entry.dirty || storage === undefined || entry.state.authoritative === undefined) return;
      const authority = entry.state.authoritative;

      const frame = yield* encode(wire.envelope.snapshotFrame, {
        ...request(entry.address),
        _tag: "Snapshot",
        ...authority,
      } as typeof wire.envelope.snapshotFrame.Type);

      yield* persistence(storage.snapshotCache.put(entry.address, frame));
      if (entry.state.authoritative === authority) entry.dirty = false;
    },
    (effect, entry) => effect.pipe(Semaphore.withPermit(entry.lock)),
  );

  const flush = Effect.gen(function* () {
    yield* assertOpen();
    for (const entry of entries.values()) yield* flushEntry(entry);
  });

  const restore = Effect.fn("Client.restore")(function* (
    entry: Entry,
    sourceScope: Scope.Closeable,
  ) {
    if (storage === undefined) return;

    const rows = yield* persistence(
      storage.intentJournal.transaction((tx) =>
        tx.scan(entry.address, limits.maxPendingPerSource + 1),
      ),
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
      entry.order = Math.max(entry.order, intent.order + 1);
    }
    yield* assertOpen(entry, sourceScope);
    entry.state = { ...entry.state, intents, quarantined };
    yield* signal;
  });

  const restoreCache = Effect.fn("Client.restoreCache")(function* (
    entry: Entry,
    sourceScope: Scope.Closeable,
  ) {
    if (storage === undefined) return;
    const cached = yield* persistence(storage.snapshotCache.get(entry.address));

    if (cached === undefined) return;
    const result = yield* Effect.result(decode(wire.envelope.snapshotFrame, cached));

    if (result._tag === "Failure" || keyOf(result.success.address) !== keyOf(entry.address)) {
      yield* persistence(storage.snapshotCache.remove(entry.address));

      return;
    }
    yield* commit(entry, sourceScope, (state) =>
      state.authoritative === undefined
        ? { ...state, provisional: result.success.snapshot }
        : state,
    );
  });

  const recordFailure = Effect.fn("Client.failure")(function* (
    entry: Entry,
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
    entry: Entry,
    sourceScope: Scope.Closeable,
    action: BoundAction,
    intent: Intent,
    outcome: Outcome,
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
      () => {
        entry.settled.set(id, outcome);
        while (entry.settled.size > limits.maxPendingPerSource) {
          const oldest = entry.settled.keys().next();

          if (oldest.done) break;
          entry.settled.delete(oldest.value);
        }
      },
    );

    return outcome;
  });

  const send = Effect.fn("Client.send")(
    function* (
      entry: Entry,
      sourceScope: Scope.Closeable,
      id: string,
      lookup: boolean,
      leaseScope?: Scope.Scope,
    ) {
      yield* assertOpen(entry, sourceScope, leaseScope);
      const remembered = entry.settled.get(id);

      if (remembered !== undefined) return remembered;
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
        const result = yield* transport(
          options.transport.result(copyJson(intent.command)),
          id,
        ).pipe(Effect.flatMap((value) => decode(action.lookup, value)));

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

      const outcome = yield* transport(
        options.transport.execute(copyJson(intent.command)),
        id,
      ).pipe(Effect.flatMap((value) => decode(action.outcome, value)));

      return yield* settle(entry, sourceScope, action, intent, outcome, leaseScope);
    },
    (effect, entry) => effect.pipe(Semaphore.withPermit(entry.commands)),
  );

  const unwrap = (outcome: Outcome): Effect.Effect<unknown, unknown> =>
    outcome._tag === "Succeeded" ? Effect.succeed(outcome.result) : Effect.fail(outcome.error);

  const hydrate = Effect.fn("Client.hydrate")(function* (
    entry: Entry,
    sourceScope: Scope.Closeable,
  ) {
    const value = yield* transport(options.transport.snapshot(request(entry.address)));

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
    entry: Entry,
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

    const producer = options.transport
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

    const consume = Stream.fromQueue(frames).pipe(
      Stream.runForEach((frame) =>
        acceptFrame(entry, sourceScope, frame).pipe(
          Effect.tap((healthy) =>
            Effect.sync(() => {
              if (healthy) recovered();
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
      for (const id of entry.state.intents.keys()) {
        const attempts = entry.recoveryAttempts.get(id) ?? 0;

        if (attempts >= retry.maxAttempts) continue;
        entry.recoveryAttempts.set(id, attempts + 1);
        yield* send(entry, sourceScope, id, true).pipe(
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
    entry: Entry,
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
    function* (entry: Entry, sourceScope: Scope.Closeable, leaseScope?: Scope.Scope) {
      yield* assertOpen(entry, sourceScope, leaseScope);
      if (entry.run !== undefined) yield* Fiber.interrupt(entry.run);
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

  const open = Effect.fn("Client.open")(function* (input: {
    readonly kind: S["kind"];
    readonly id: string;
  }): Effect.fn.Return<Lease<S>, ClientError, Scope.Scope> {
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
          };
          entries.set(key, entry);
        }
        if (entry.leases === 0) {
          const sourceScope = yield* Scope.fork(sessionScope);

          entry.scope = sourceScope;
          const active = entry;

          const acquired = yield* Effect.result(
            Effect.gen(function* () {
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
                  recordFailure(active, sourceScope, failure).pipe(Effect.ignore),
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

    yield* Scope.addFinalizerExit(leaseScope, (exit) => Scope.close(operationsScope, exit));

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

    const execute = (action: BoundAction, payload: unknown) =>
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
              if (state.connection === "parked")
                throw (
                  state.error ??
                  error("Disconnected", "Recover the parked source before dispatching")
                );
              if (state.intents.size + state.quarantined.size >= limits.maxPendingPerSource)
                throw error("Capacity", "Pending command capacity exhausted");
              if (state.authoritative === undefined && !definition.allowUnboundCommands)
                throw error("AuthorityUnknown", "Wait for authority before executing commands");
              const generation = state.authoritative?.position.sourceAuthorityGeneration ?? null;

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
          );

          return yield* send(entry, sourceScope, id, false, leaseScope).pipe(
            Effect.mapError((failure) => error(failure.reason, failure.message, id)),
            Effect.flatMap(unwrap),
          );
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
          yield* transport(
            options.transport.publishMessage({ ...request(address), message: encoded }),
          );
        }),
      );

    return {
      read: active.pipe(Effect.andThen(read(address))),
      changes: changes(address).pipe(Stream.mapEffect((state) => active.pipe(Effect.as(state)))),
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
      retry: (id) =>
        owned(
          send(entry, sourceScope, id, true, leaseScope).pipe(
            Effect.mapError((failure) => error(failure.reason, failure.message, id)),
            Effect.flatMap(unwrap),
          ),
        ) as ReturnType<Lease<S>["retry"]>,
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
      hydrate: (frame) =>
        owned(
          encode(
            wire.envelope.snapshotFrame,
            frame as typeof wire.envelope.snapshotFrame.Type,
          ).pipe(
            Effect.flatMap((value) => acceptFrame(entry, sourceScope, value, leaseScope)),
            Effect.asVoid,
          ),
        ),
      mergeObservation: (merge) =>
        owned(
          commit(
            entry,
            sourceScope,
            (state) => {
              const value = merge(
                (state.authoritative?.snapshot ?? state.provisional) as Snapshot<S> | undefined,
              );

              return state.authoritative === undefined
                ? { ...state, provisional: value as ReplicaState.Snapshot }
                : {
                    ...state,
                    authoritative: {
                      ...state.authoritative,
                      snapshot: value as ReplicaState.Snapshot,
                    },
                  };
            },
            leaseScope,
          ),
        ),
      applyHistory: (generation, value) =>
        owned(
          commit(
            entry,
            sourceScope,
            (state) =>
              ReplicaState.history(state, entry.slots, generation, value as ReplicaState.Snapshot),
            leaseScope,
          ),
        ),
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
      yield* Queue.shutdown(saves);
    }),
  );
  yield* Effect.gen(function* () {
    while (true) {
      yield* Queue.take(saves);
      yield* Effect.sleep("100 millis");
      yield* flush.pipe(
        Effect.catch((failure) =>
          Effect.gen(function* () {
            for (const entry of entries.values()) {
              if (entry.dirty) entry.state = { ...entry.state, error: failure };
            }
            yield* signal;
          }),
        ),
      );
    }
  }).pipe(Effect.forkScoped);

  return { actorId, contract, open, read, changes, flush };
});
