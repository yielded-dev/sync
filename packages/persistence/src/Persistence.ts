import { ActorId, CommandId, SourceAddress, type SourceAddress as Address } from "@yielded/sync";
import {
  Client,
  JournalRow,
  PersistenceError,
  ReplicaPersistence,
  type JournalChanges,
  type PersistenceHandle,
} from "@yielded/sync/client";
import { Clock, Effect, Exit, Layer, Schema, Scope } from "effect";

/** A scoped driver. modify must commit the callback's result atomically or fail. */
export interface AtomicStore {
  readonly read: (key: string) => Effect.Effect<string | undefined, PersistenceError>;
  readonly modify: <A>(
    key: string,
    f: (value: string | undefined) => readonly [A, string | undefined],
  ) => Effect.Effect<A, PersistenceError>;
}

export interface Options {
  readonly namespace: string;
  readonly maxJournalRows?: number;
  readonly maxSnapshots?: number;
  readonly maxBytes?: number;
}

export interface DurableHandle extends PersistenceHandle {
  readonly generation: number;
  readonly snapshotCache: NonNullable<PersistenceHandle["snapshotCache"]>;
  /** Explicit destructive policy; ordinary source disposal never calls this. */
  readonly purgeSource: (address: Address) => Effect.Effect<void, PersistenceError>;
  /** Invalidates this handle and every handle captured before the wipe. */
  readonly wipe: Effect.Effect<void, PersistenceError>;
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
  revision: Natural,
  rows: Schema.Array(JournalRow),
});

const Snapshot = Schema.Struct({
  address: SourceAddress,
  savedAt: Natural,
  bytes: Natural,
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

export const failure = (reason: PersistenceError["reason"], message: string) =>
  PersistenceError.make({ reason, message });

export const storageError = (cause: unknown): PersistenceError =>
  Schema.is(PersistenceError)(cause)
    ? cause
    : failure("Unavailable", `Persistence operation failed: ${String(cause)}`);

const addressKey = (address: Address) => JSON.stringify([address.kind, address.id]);

const rowKey = (address: Address, commandId: string) =>
  JSON.stringify([address.kind, address.id, commandId]);

const copyJson = <A extends Schema.Json>(value: A): A => JSON.parse(JSON.stringify(value)) as A;

const loadRows = (rows: ReadonlyArray<JournalRow>, address: Address, limit: number) => {
  const key = addressKey(decodeAddress(address));

  decodeLimit(limit);

  return rows
    .filter((row) => addressKey(row.address) === key)
    .slice(0, limit)
    .map(copyJson);
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
    rows.set(key, copyJson(row));
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

/** Portable policy shared by durable adapters; databases remain separate driver resources. */
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
    generation,
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
        Effect.gen(function* () {
          const savedAt = yield* Clock.currentTimeMillis;

          const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(
            value,
          ).pipe(Effect.mapError(storageError));

          const row = yield* Schema.decodeEffect(Snapshot)({
            address,
            savedAt,
            bytes: bytes(encoded),
            value,
          }).pipe(Effect.mapError(storageError));

          yield* changeCache((previous) => {
            const rows = [
              ...previous.filter((entry) => addressKey(entry.address) !== addressKey(address)),
              row,
            ];

            while (
              rows.length > 0 &&
              (rows.length > limits.maxSnapshots ||
                bytes(encodeCache({ format: 2, generation, rows })) > limits.maxBytes)
            )
              rows.shift();

            return rows;
          });
        }),
      remove: (address) =>
        changeCache((rows) =>
          rows.filter((row) => addressKey(row.address) !== addressKey(address)),
        ),
    },
    purgeSource: (address) =>
      Effect.gen(function* () {
        yield* journal.modify(key, (stored) => {
          const current = check(journalState(stored));

          return [
            undefined,
            encodeJournal({
              ...current,
              revision: next(current.revision),
              rows: current.rows.filter((row) => addressKey(row.address) !== addressKey(address)),
            }),
          ];
        });
        yield* changeCache((rows) =>
          rows.filter((row) => addressKey(row.address) !== addressKey(address)),
        );
      }),
    wipe: Effect.gen(function* () {
      yield* journal.modify(key, (stored) => {
        const current = check(journalState(stored));

        return [
          undefined,
          encodeJournal({
            format: 1,
            generation: next(current.generation),
            revision: next(current.revision),
            rows: [],
          }),
        ];
      });
      yield* cache.modify(key, (stored) => {
        const previous = cacheState(stored);

        // A new handle may already have saved a fresh snapshot after the journal wipe.
        return [
          undefined,
          previous !== undefined && previous.generation > generation ? stored : undefined,
        ];
      });
    }),
  };

  return handle;
});

const MemoryConfiguration = Schema.Struct({
  actorId: ActorId,
  maxJournalRows: Natural,
  maxSnapshots: Natural,
});

type MemoryOptions = Pick<Options, "maxJournalRows" | "maxSnapshots">;

/** Process-local storage only. Reuse the factory to exercise actor remounts and wipes. */
export const memory = (options?: MemoryOptions) => {
  const actors = new Map<
    string,
    {
      generation: number;
      journal: ReadonlyArray<JournalRow>;
      cache: Map<string, Schema.Json>;
    }
  >();

  const open = Effect.gen(function* () {
    const { actorId } = yield* Client.CurrentActor;

    const limits = yield* Schema.decodeEffect(MemoryConfiguration)({
      maxJournalRows: options?.maxJournalRows ?? 4096,
      maxSnapshots: options?.maxSnapshots ?? 128,
      actorId,
    }).pipe(Effect.mapError(() => failure("Unavailable", "Invalid persistence configuration")));

    const state = actors.get(actorId) ?? {
      generation: 0,
      journal: [],
      cache: new Map<string, Schema.Json>(),
    };

    actors.set(actorId, state);
    const generation = state.generation;

    const operation = <A>(body: () => A) =>
      Effect.try({
        try: () => {
          if (generation !== state.generation)
            throw failure("StaleGeneration", "Persistence handle was invalidated by a wipe");

          return body();
        },
        catch: storageError,
      });

    const handle: DurableHandle = {
      actorId,
      generation,
      intentJournal: {
        load: (address, limit) => operation(() => loadRows(state.journal, address, limit)),
        commit: (address, changes) =>
          operation(() => {
            state.journal = changeJournal(
              state.journal,
              address,
              changes,
              limits.maxJournalRows,
            ).rows;
          }),
      },
      snapshotCache: {
        get: (address) =>
          operation(() => {
            const value = state.cache.get(addressKey(address));

            return value === undefined ? undefined : copyJson(value);
          }),
        put: (address, value) =>
          operation(() => {
            const key = addressKey(decodeAddress(address));
            const snapshot = copyJson(Schema.decodeSync(Schema.Json)(value));

            state.cache.delete(key);
            state.cache.set(key, snapshot);
            while (state.cache.size > limits.maxSnapshots) {
              const oldest = state.cache.keys().next();

              if (oldest.done) break;
              state.cache.delete(oldest.value);
            }
          }),
        remove: (address) =>
          operation(() => {
            state.cache.delete(addressKey(address));
          }),
      },
      purgeSource: (address) =>
        operation(() => {
          state.journal = state.journal.filter(
            (row) => addressKey(row.address) !== addressKey(address),
          );
          state.cache.delete(addressKey(address));
        }),
      wipe: operation(() => {
        state.generation = next(state.generation);
        state.journal = [];
        state.cache.clear();
      }),
    };

    return handle;
  });

  return { open };
};

export const layerMemory = (options?: MemoryOptions) =>
  Layer.effect(
    ReplicaPersistence,
    Effect.suspend(() => memory(options).open),
  );
