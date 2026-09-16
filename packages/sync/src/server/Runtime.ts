import { DateTime, Effect, Schema, type Scope } from "effect";

import type * as Action from "../Action.ts";
import { ProtocolError, type SourceAddress, SourcePosition, type WireSchema } from "../Model.ts";
import type * as Source from "../Source.ts";
import { decode, encode, invalid, unavailable } from "./codec.ts";
import type { AuthorizationOperation, Definition } from "./Server.ts";
import {
  ServerCrypto,
  SourceStorage,
  type SourceTransaction,
  type StoredEvent,
  type StoredHead,
  type StoredReceipt,
  type OutboxRecord,
} from "./SourceStorage.ts";

export const Session = Schema.Struct({
  principal: Schema.Json,
  actorId: Schema.NonEmptyString,
  connectionId: Schema.NonEmptyString,
  expiresAtMillis: Schema.Finite,
});

export type Session = typeof Session.Type;

export interface Limits {
  readonly maxEvents: number;
  readonly maxOutbox: number;
  readonly maxBytes: number;
  readonly turnTimeoutMillis: number;
}

export interface Runtime<S extends Source.Spec> {
  readonly contract: Source.Definition<S>;
  readonly address: SourceAddress;
  readonly snapshot: (
    session: Session,
  ) => Effect.Effect<Source.Definition<S>["envelope"]["snapshotFrame"]["Type"], ProtocolError>;
  readonly bootstrap: (
    session: Session,
    after?: SourcePosition,
  ) => Effect.Effect<
    | Source.Definition<S>["envelope"]["snapshotFrame"]["Type"]
    | Source.Definition<S>["envelope"]["eventsFrame"]["Type"]
    | Source.Definition<S>["envelope"]["gapFrame"]["Type"],
    ProtocolError
  >;
  readonly execute: <A extends { readonly command: WireSchema; readonly outcome: WireSchema }>(
    session: Session,
    action: A,
    input: A["command"]["Type"],
  ) => Effect.Effect<A["outcome"]["Type"], ProtocolError>;
  readonly result: <A extends { readonly command: WireSchema; readonly lookup: WireSchema }>(
    session: Session,
    action: A,
    input: A["command"]["Type"],
  ) => Effect.Effect<A["lookup"]["Type"], ProtocolError>;
  readonly dispatch: (
    session: Session,
    input: unknown,
  ) => Effect.Effect<
    { readonly outcome: Schema.Json; readonly events: ReadonlyArray<StoredEvent> },
    ProtocolError
  >;
  readonly lookup: (
    session: Session,
    input: unknown,
  ) => Effect.Effect<
    { readonly _tag: "Unknown" } | { readonly _tag: "Found"; readonly outcome: Schema.Json },
    ProtocolError
  >;
  readonly publishMessage: (
    session: Session,
    message: unknown,
  ) => Effect.Effect<Source.Definition<S>["envelope"]["messageFrame"]["Type"], ProtocolError>;
  readonly checkSubscription: (session: Session) => Effect.Effect<void, ProtocolError>;
}

const StateSlots = Schema.Record(Schema.String, Schema.Json);

/** Deterministic JSON ordering, after the owning Schema's JSON encoding. */
export const canonicalJson = (value: Schema.Json): string => {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const record = value as Readonly<Record<string, Schema.Json>>;

    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
      .join(",")}}`;
  }

  return JSON.stringify(value);
};

const CommandHeader = Schema.Struct({
  commandId: Schema.NonEmptyString,
  namespace: Schema.String,
  action: Schema.String,
  admittedGeneration: Schema.NullOr(SourcePosition.fields.sourceAuthorityGeneration),
});

export const open = Effect.fn("Server.open")(function* <S extends Source.Spec, R>(
  definition: Definition<S, R>,
  address: SourceAddress,
  limits: Limits = { maxEvents: 512, maxOutbox: 100, maxBytes: 600_000, turnTimeoutMillis: 5_000 },
): Effect.fn.Return<Runtime<S>, ProtocolError, R | Scope.Scope | SourceStorage | ServerCrypto> {
  for (const limit of Object.values(limits)) {
    if (!Number.isSafeInteger(limit) || limit <= 0)
      return yield* Effect.die("Server limits must be positive safe integers");
  }
  yield* decode(definition.contract.addressSchema, address);
  const implementation = yield* definition.acquire;
  const storage = yield* SourceStorage;
  const crypto = yield* ServerCrypto;
  const contract = definition.contract;
  const header = { protocolVersion: 1 as const, address, schemaVersion: contract.schemaVersion };
  const slots = new Map(implementation.slots.map((slot) => [slot.id, slot]));

  const actions: Array<Action.Bound<string, number, string, Action.Definition>> = Object.values(
    contract.actions,
  );

  for (const plugin of Object.values(contract.plugins as Source.Definition["plugins"]))
    actions.push(...Object.values(plugin.actions));

  const turn = <A, E>(body: (tx: SourceTransaction) => Effect.Effect<A, E>) =>
    storage.transaction(body).pipe(
      Effect.timeoutOrElse({
        duration: limits.turnTimeoutMillis,
        orElse: () => Effect.fail(unavailable("Source turn timed out")),
      }),
      Effect.catchTag("StorageError", () => Effect.fail(unavailable("Source storage unavailable"))),
    );

  const head = Effect.fn("Server.head")(function* (tx: SourceTransaction) {
    const current = yield* tx.readHead;

    if (current !== undefined) {
      if (
        current.address.kind !== address.kind ||
        current.address.id !== address.id ||
        current.schemaVersion !== contract.schemaVersion
      )
        return yield* invalid(
          "Stored source identity or version differs; an explicit migration is required",
        );

      return current;
    }

    const state = Object.fromEntries(
      yield* Effect.forEach(
        implementation.slots,
        Effect.fn(function* (slot) {
          return [slot.id, yield* slot.initialize] as const;
        }),
      ),
    );

    const initial = {
      ...header,
      position: { sourceAuthorityGeneration: yield* crypto.generation, cursor: 0 },
      state,
    };

    if (canonicalJson(initial).length * 3 > limits.maxBytes)
      return yield* unavailable("Initial state exceeds encoded size limit");
    yield* tx.initialize(initial);

    return initial;
  });

  const authorize = Effect.fn("Server.checkAccess")(function* (
    session: Session,
    current: StoredHead,
    operation: AuthorizationOperation,
  ) {
    yield* decode(Session, session);
    const now = DateTime.toEpochMillis(yield* DateTime.now);

    if (session.expiresAtMillis <= now)
      return yield* ProtocolError.make({ reason: "Unauthenticated", message: "Session expired" });
    const state = yield* decode(StateSlots, current.state);

    yield* implementation.authorize({
      principal: session.principal,
      address,
      state: state.$source,
      operation,
    });
  });

  const project = Effect.fn("Server.snapshotFrame")(function* (current: StoredHead) {
    const state = yield* decode(StateSlots, current.state);

    const values = Object.fromEntries(
      yield* Effect.forEach(
        implementation.slots,
        Effect.fn(function* (slot) {
          return [slot.id, yield* slot.snapshot(state[slot.id])] as const;
        }),
      ),
    );

    // Source.make permits only service-free schemas, including every mapped plugin field.
    const schema = contract.envelope.snapshotFrame as Schema.Codec<
      Source.Definition<S>["envelope"]["snapshotFrame"]["Type"],
      Schema.Json
    >;

    const frame = {
      _tag: "Snapshot",
      ...header,
      position: current.position,
      snapshot: {
        source: values.$source,
        plugins: Object.fromEntries(Object.entries(values).filter(([key]) => key !== "$source")),
      },
    };

    if (canonicalJson(frame).length * 3 > limits.maxBytes)
      return yield* unavailable("Snapshot exceeds encoded size limit");

    return yield* decode(schema, frame);
  });

  const snapshot = (session: Session) =>
    turn(
      Effect.fn(function* (tx) {
        const current = yield* head(tx);

        yield* authorize(session, current, { _tag: "snapshot" });

        return yield* project(current);
      }),
    );

  const bootstrap = (session: Session, after?: SourcePosition) =>
    turn(
      Effect.fn(function* (tx) {
        const current = yield* head(tx);

        yield* authorize(session, current, { _tag: "subscribe" });
        if (after === undefined) return yield* project(current);
        yield* decode(SourcePosition, after);

        const gap = () =>
          decode(contract.envelope.gapFrame, {
            _tag: "ResyncRequired",
            ...header,
            position: current.position,
            reason: "Position is outside retained replay",
          });

        if (
          after.sourceAuthorityGeneration !== current.position.sourceAuthorityGeneration ||
          after.cursor > current.position.cursor ||
          current.position.cursor - after.cursor > limits.maxEvents
        )
          return yield* gap();
        const events = yield* tx.readEvents(after, limits.maxEvents);

        if (
          events.length !== current.position.cursor - after.cursor ||
          events.some((event, index) => event.position.cursor !== after.cursor + index + 1)
        )
          return yield* gap();
        const frame = { _tag: "Events", ...header, position: current.position, events };

        if (canonicalJson(frame).length * 3 > limits.maxBytes) return yield* gap();

        return yield* decode(contract.envelope.eventsFrame, frame);
      }),
    );

  const command = Effect.fn("Server.command")(function* (
    session: Session,
    input: unknown,
    lookup: boolean,
  ) {
    const commandHeader = yield* decode(CommandHeader, input);

    const action = actions.find(
      (candidate) =>
        candidate.namespace === commandHeader.namespace && candidate.name === commandHeader.action,
    );

    if (action === undefined) return yield* invalid("Unregistered action");
    const decoded = yield* decode(action.command, input);

    if (decoded.address.kind !== address.kind || decoded.address.id !== address.id)
      return yield* invalid("Command addresses another source");
    const encoded = yield* encode(action.command, decoded);
    const canonical = canonicalJson(encoded);

    if (canonical.length * 3 > limits.maxBytes)
      return yield* unavailable("Command exceeds encoded size limit");

    const fingerprint = yield* crypto
      .sha256(canonical)
      .pipe(Effect.mapError(() => unavailable("Fingerprint failed")));

    return yield* turn(
      Effect.fn(function* (tx) {
        const current = yield* head(tx);

        yield* authorize(session, current, {
          _tag: lookup ? "result" : "action",
          namespace: decoded.namespace,
          action: decoded.action,
          commandId: decoded.commandId,
        });
        const generation = decoded.admittedGeneration ?? current.position.sourceAuthorityGeneration;

        if (decoded.admittedGeneration === null && !definition.allowUnboundCommands) {
          return yield* ProtocolError.make({
            reason: "AuthorityMismatch",
            message: "This source requires admission to a known authority",
          });
        }

        const identity = {
          sourceAuthorityGeneration: generation,
          actorId: session.actorId,
          commandId: decoded.commandId,
        };

        const receipt = yield* tx.readReceipt({
          ...identity,
          sourceAuthorityGeneration: decoded.admittedGeneration,
        });

        if (receipt !== undefined) {
          if (receipt.fingerprint !== fingerprint)
            return yield* ProtocolError.make({
              reason: "CommandIdConflict",
              message: "Command id already belongs to a different request",
            });

          return { outcome: receipt.outcome, events: [] as ReadonlyArray<StoredEvent> };
        }
        if (generation !== current.position.sourceAuthorityGeneration)
          return yield* ProtocolError.make({
            reason: "AuthorityMismatch",
            message: "Command belongs to another authority",
          });
        if (lookup) return { outcome: undefined, events: [] as ReadonlyArray<StoredEvent> };
        const handler = slots.get(decoded.namespace)?.actions.get(decoded.action);

        if (handler === undefined) return yield* invalid("Unregistered server action");
        const states = yield* decode(StateSlots, current.state);

        const evaluated = yield* handler(
          states[decoded.namespace],
          yield* encode(action.payload, decoded.payload),
          session.principal,
          address,
        );

        const events: Array<StoredEvent> = [];
        let next = current;
        let outcome: Schema.Json;
        let outbox: Array<OutboxRecord> = [];

        if (evaluated._tag === "Rejected") {
          outcome = { _tag: "Rejected", error: evaluated.error };
        } else {
          const plan = evaluated.plan;

          if (plan.events.length > limits.maxEvents || plan.outbox.length > limits.maxOutbox)
            return yield* unavailable("Commit exceeds source limits");
          if (
            plan.events.length === 0 &&
            canonicalJson(plan.state) !== canonicalJson(states[decoded.namespace])
          )
            return yield* unavailable("A state change must emit a durable event");

          const position = yield* decode(SourcePosition, {
            ...current.position,
            cursor: current.position.cursor + plan.events.length,
          });

          next = { ...current, position, state: { ...states, [decoded.namespace]: plan.state } };
          plan.events.forEach((event, index) =>
            events.push({
              position: { ...position, cursor: current.position.cursor + index + 1 },
              command: { actorId: session.actorId, commandId: decoded.commandId },
              event: { namespace: decoded.namespace, payload: event },
            }),
          );
          outcome = { _tag: "Succeeded", position, result: plan.result };
          const now = DateTime.toEpochMillis(yield* DateTime.now);

          outbox = plan.outbox.map((payload, index) => ({
            id: canonicalJson([generation, session.actorId, decoded.commandId, index]),
            position,
            payload,
            attempts: 0,
            availableAtMillis: now,
          }));
        }
        const saved: StoredReceipt = { ...identity, fingerprint, command: encoded, outcome };

        if (
          canonicalJson({ head: next, events, receipt: saved, outbox }).length * 3 >
          limits.maxBytes
        )
          return yield* unavailable("Commit exceeds encoded size limit");
        yield* tx.commit({
          expected: current.position,
          head: next,
          events,
          receipt: saved,
          outbox,
        });

        return { outcome, events };
      }),
    );
  });

  const dispatch = Effect.fn("Server.dispatch")(function* (session: Session, input: unknown) {
    const executed = yield* command(session, input, false);

    if (executed.outcome === undefined) return yield* unavailable("Command outcome missing");

    return { outcome: executed.outcome, events: executed.events };
  });

  const lookup = Effect.fn("Server.lookup")(function* (session: Session, input: unknown) {
    const found = yield* command(session, input, true);

    return found.outcome === undefined
      ? { _tag: "Unknown" as const }
      : { _tag: "Found" as const, outcome: found.outcome };
  });

  const execute = Effect.fn("Server.execute")(function* <
    A extends { readonly command: WireSchema; readonly outcome: WireSchema },
  >(
    session: Session,
    action: A,
    input: A["command"]["Type"],
  ): Effect.fn.Return<A["outcome"]["Type"], ProtocolError> {
    const executed = yield* dispatch(session, yield* encode(action.command, input));

    return yield* decode(action.outcome, executed.outcome);
  });

  const result = Effect.fn("Server.result")(function* <
    A extends { readonly command: WireSchema; readonly lookup: WireSchema },
  >(
    session: Session,
    action: A,
    input: A["command"]["Type"],
  ): Effect.fn.Return<A["lookup"]["Type"], ProtocolError> {
    return yield* decode(
      action.lookup,
      yield* lookup(session, yield* encode(action.command, input)),
    );
  });

  const publishMessage = (session: Session, message: unknown) =>
    turn(
      Effect.fn(function* (tx) {
        const current = yield* head(tx);

        yield* authorize(session, current, { _tag: "message" });

        return yield* decode(contract.envelope.messageFrame, {
          _tag: "Message",
          ...header,
          actorId: session.actorId,
          connectionId: session.connectionId,
          message,
        });
      }),
    );

  return {
    contract,
    address,
    snapshot,
    bootstrap,
    execute,
    result,
    dispatch,
    lookup,
    publishMessage,
    checkSubscription: (session: Session) =>
      turn(
        Effect.fn(function* (tx) {
          yield* authorize(session, yield* head(tx), { _tag: "subscribe" });
        }),
      ),
  };
});
