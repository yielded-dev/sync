/**
 * Durable client record policy over scoped atomic storage drivers.
 * Journals retain unresolved retry evidence; snapshot caches are disposable.
 *
 * @since 0.1.0-beta.0
 */

import { ActorId, CommandId, SourceAddress, type SourceAddress as Address } from "@yielded/sync";
import {
  Client,
  JournalRow,
  PersistenceError,
  type JournalChanges,
  type PersistenceHandle,
} from "@yielded/sync/client";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

/**
 * A scoped driver. modify must commit the callback's result atomically or fail.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface AtomicStore {
  readonly read: (key: string) => Effect.Effect<string | undefined, PersistenceError>;
  readonly modify: <A>(
    key: string,
    f: (value: string | undefined) => readonly [A, string | undefined],
  ) => Effect.Effect<A, PersistenceError>;
}

/**
 * Storage namespace and capacity bounds shared by journal and cache drivers.
 * Capacity failures retain existing unresolved commands.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Options {
  /** Stable application identity. Changing it selects different storage. */
  readonly namespace: string;
  /** Maximum unresolved commands per actor. Defaults to 4096. */
  readonly maxJournalRows?: number;
  /** Maximum cached source snapshots per actor. Defaults to 128. */
  readonly maxSnapshots?: number;
  /** Maximum encoded bytes in each store. Defaults to 8 MiB. */
  readonly maxBytes?: number;
}

/**
 * An actor-bound intent journal and disposable snapshot cache backed by durable drivers.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface DurableHandle extends PersistenceHandle {
  readonly snapshotCache: NonNullable<PersistenceHandle["snapshotCache"]>;
}

const Natural = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const Positive = Schema.Int.check(Schema.isGreaterThan(0));

const Configuration = Schema.Struct({
  namespace: Schema.NonEmptyString,
  actorId: ActorId,
  maxJournalRows: Positive,
  maxSnapshots: Positive,
  maxBytes: Positive,
});

const configuration = (options: Options) =>
  Client.CurrentActor.pipe(
    Effect.flatMap(({ actorId }) =>
      Schema.decodeEffect(Configuration)({
        maxJournalRows: 4096,
        maxSnapshots: 128,
        maxBytes: 8 * 1024 * 1024,
        ...options,
        actorId,
      }),
    ),
    Effect.mapError(() => failure("Unavailable", "Invalid persistence configuration")),
  );

const Changes = Schema.Struct({
  insert: Schema.Array(JournalRow),
  replace: Schema.Array(JournalRow),
  remove: Schema.Array(CommandId),
});

const Journal = Schema.Struct({
  format: Schema.Literal(1),
  generation: Natural,
  // Writers committing against a captured revision rely on this counter.
  revision: Natural,
  rows: Schema.Array(JournalRow),
});

const Snapshot = Schema.Struct({
  address: SourceAddress,
  value: Schema.Json,
});

const Cache = Schema.Struct({
  format: Schema.Literal(2),
  generation: Natural,
  rows: Schema.Array(Snapshot),
});

const journalCodec = Schema.fromJsonString(Journal);
const cacheCodec = Schema.fromJsonString(Cache);
const decodeJournal = Schema.decodeSync(journalCodec);
const encodeJournal = Schema.encodeSync(journalCodec);
const decodeCache = Schema.decodeUnknownOption(cacheCodec);
const encodeCache = Schema.encodeSync(cacheCodec);
const decodeChanges = Schema.decodeSync(Changes);
const decodeAddress = Schema.decodeSync(SourceAddress);
const decodeLimit = Schema.decodeSync(Natural);

/**
 * Constructs a typed persistence failure from its reason and message.
 *
 * @category utilities
 * @since 0.1.0-beta.0
 */
export const failure = (reason: PersistenceError["reason"], message: string) =>
  PersistenceError.make({ reason, message });

/**
 * Preserves typed persistence failures and wraps other storage errors as unavailable.
 *
 * @category utilities
 * @since 0.1.0-beta.0
 */
export const storageError = (cause: unknown): PersistenceError =>
  Schema.is(PersistenceError)(cause)
    ? cause
    : failure("Unavailable", `Persistence operation failed: ${String(cause)}`);

const addressKey = (address: Address) => JSON.stringify([address.kind, address.id]);

const rowKey = (address: Address, commandId: string) =>
  JSON.stringify([address.kind, address.id, commandId]);

const loadRows = (rows: ReadonlyArray<JournalRow>, address: Address, limit: number) => {
  const key = addressKey(decodeAddress(address));

  decodeLimit(limit);

  return rows.filter((row) => addressKey(row.address) === key).slice(0, limit);
};

/** Validate and stage a whole batch before its owning store publishes any rows. */
const changeJournal = (
  initial: ReadonlyArray<JournalRow>,
  address: Address,
  changes: JournalChanges,
  maxRows: number,
) => {
  const source = addressKey(decodeAddress(address));
  const batch = decodeChanges(changes);
  const rows = new Map(initial.map((row) => [rowKey(row.address, row.commandId), row]));
  const changed = new Set<string>();
  let dirty = false;

  const identity = (commandId: string) => {
    if (changed.has(commandId))
      throw failure("Conflict", "A journal batch may change each identity only once");
    changed.add(commandId);

    return rowKey(address, commandId);
  };

  const write = (row: JournalRow, replace: boolean) => {
    if (addressKey(row.address) !== source)
      throw failure("Conflict", "Journal rows must belong to the batch address");
    const key = identity(row.commandId);

    if (rows.has(key) !== replace)
      throw failure("Conflict", "Journal identity already exists or is missing");
    rows.set(key, row);
    dirty = true;
  };

  for (const row of batch.insert) write(row, false);
  for (const row of batch.replace) write(row, true);
  for (const commandId of batch.remove) dirty = rows.delete(identity(commandId)) || dirty;
  if (rows.size > maxRows)
    throw failure("Capacity", "Journal capacity exhausted; unresolved evidence was retained");

  return { rows: Array.from(rows.values()), dirty };
};

const bytes = (value: string) => {
  let size = 0;

  for (const character of value) {
    const point = character.codePointAt(0) ?? 0;

    size += point <= 0x7f ? 1 : point <= 0x7ff ? 2 : point <= 0xffff ? 3 : 4;
  }

  return size;
};

const next = (value: number) => {
  if (value >= Number.MAX_SAFE_INTEGER) throw failure("Capacity", "Persistence counter exhausted");

  return value + 1;
};

const journalState = (value: string | undefined) => {
  if (value === undefined)
    throw failure("Unavailable", "Journal metadata is missing; retain the database for recovery");

  try {
    const state = decodeJournal(value);

    if (
      new Set(state.rows.map((row) => rowKey(row.address, row.commandId))).size !==
      state.rows.length
    )
      throw failure("Unavailable", "Journal contains duplicate identities");

    return state;
  } catch {
    throw failure(
      "Unavailable",
      "Journal format is unreadable; retain the database for migration or recovery",
    );
  }
};

const cacheState = (value: string | undefined) => {
  const decoded = decodeCache(value);

  return decoded._tag === "Some" ? decoded.value : undefined;
};

/**
 * Portable policy shared by durable adapters; databases remain separate driver resources.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export const make = Effect.fn("Persistence.make")(function* <R1, R2>(
  options: Options,
  drivers: {
    readonly journal: Effect.Effect<AtomicStore, PersistenceError, R1>;
    readonly cache: Effect.Effect<AtomicStore, PersistenceError, R2>;
  },
) {
  const limits = yield* configuration(options);
  const journal = yield* drivers.journal;
  const cacheScope = yield* Scope.fork(yield* Effect.scope);

  const cache = yield* drivers.cache.pipe(
    Scope.provide(cacheScope),
    Effect.onError((cause) => Scope.close(cacheScope, Exit.failCause(cause))),
    Effect.catch((error): Effect.Effect<AtomicStore> =>
      Effect.succeed({ read: () => Effect.fail(error), modify: () => Effect.fail(error) }),
    ),
  );

  const key = JSON.stringify([limits.actorId]);

  const initial = yield* journal.modify(key, (stored) => {
    const value =
      stored === undefined
        ? { format: 1 as const, generation: 0, revision: 0, rows: [] }
        : journalState(stored);

    return [value, encodeJournal(value)];
  });

  const generation = initial.generation;

  const check = (value: typeof Journal.Type) => {
    if (value.generation !== generation)
      throw failure("StaleGeneration", "Persistence handle was invalidated by a wipe");

    return value;
  };

  const read = journal
    .read(key)
    .pipe(
      Effect.flatMap((value) =>
        Effect.try({ try: () => check(journalState(value)), catch: storageError }),
      ),
    );

  const bounded = (value: typeof Journal.Type) => {
    const encoded = encodeJournal(value);

    if (value.rows.length > limits.maxJournalRows || bytes(encoded) > limits.maxBytes)
      throw failure("Capacity", "Journal capacity exhausted; unresolved evidence was retained");

    return encoded;
  };

  const cached = Effect.gen(function* () {
    yield* read;
    const value = cacheState(yield* cache.read(key));

    yield* read;

    return value?.generation === generation ? value.rows : [];
  });

  const changeCache = (
    f: (rows: ReadonlyArray<typeof Snapshot.Type>) => ReadonlyArray<typeof Snapshot.Type>,
  ) =>
    Effect.gen(function* () {
      yield* read;
      yield* cache.modify(key, (stored) => {
        const previous = cacheState(stored);

        if (previous !== undefined && previous.generation > generation)
          throw failure("StaleGeneration", "Cache belongs to a newer persistence generation");

        return [
          undefined,
          encodeCache({
            format: 2,
            generation,
            rows: f(previous?.generation === generation ? previous.rows : []),
          }),
        ];
      });
      yield* read;
    });

  const handle: DurableHandle = {
    actorId: limits.actorId,
    intentJournal: {
      load: (address, limit) =>
        read.pipe(
          Effect.flatMap(({ rows }) =>
            Effect.try({ try: () => loadRows(rows, address, limit), catch: storageError }),
          ),
        ),
      commit: (address, changes) =>
        journal.modify(key, (stored) => {
          const current = check(journalState(stored));

          const { rows, dirty } = changeJournal(
            current.rows,
            address,
            changes,
            limits.maxJournalRows,
          );

          return [
            undefined,
            dirty ? bounded({ ...current, revision: next(current.revision), rows }) : stored,
          ];
        }),
    },
    snapshotCache: {
      get: (address) =>
        cached.pipe(
          Effect.map(
            (rows) => rows.find((row) => addressKey(row.address) === addressKey(address))?.value,
          ),
        ),
      put: (address, value) =>
        changeCache((previous) => {
          const rows = [
            ...previous.filter((entry) => addressKey(entry.address) !== addressKey(address)),
            { address, value },
          ];

          while (
            rows.length > 0 &&
            (rows.length > limits.maxSnapshots ||
              bytes(encodeCache({ format: 2, generation, rows })) > limits.maxBytes)
          )
            rows.shift();

          return rows;
        }),
      remove: (address) =>
        changeCache((rows) =>
          rows.filter((row) => addressKey(row.address) !== addressKey(address)),
        ),
    },
  };

  return handle;
});
