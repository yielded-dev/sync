import { Clock, Effect, Schema, Semaphore } from "effect";

import { ActorId, SourceAddress, type SourceAddress as Address } from "../Model.ts";
import { copyJson } from "./Model.ts";
import { JournalRow, PersistenceError, type Handle } from "./ReplicaPersistence.ts";

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
  readonly actorId: string;
  readonly maxJournalRows?: number;
  readonly maxSnapshots?: number;
  readonly maxBytes?: number;
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

export const configuration = (options: Options) =>
  Schema.decodeEffect(Configuration)({
    maxJournalRows: 4096,
    maxSnapshots: 128,
    maxBytes: 8 * 1024 * 1024,
    ...options,
  }).pipe(Effect.mapError(() => failure("Unavailable", "Invalid persistence configuration")));

const Journal = Schema.Struct({
  format: Schema.Literal(1),
  generation: Natural,
  revision: Natural,
  rows: Schema.Array(JournalRow),
});

export const SnapshotMetadata = Schema.Struct({
  address: SourceAddress,
  savedAt: Natural,
  bytes: Natural,
});

export type SnapshotMetadata = typeof SnapshotMetadata.Type;

const Snapshot = Schema.Struct({ ...SnapshotMetadata.fields, value: Schema.Json });
const Cache = Schema.Struct({ format: Schema.Literal(1), rows: Schema.Array(Snapshot) });
const journalCodec = Schema.fromJsonString(Journal);
const cacheCodec = Schema.fromJsonString(Cache);
const decodeJournal = Schema.decodeSync(journalCodec);
const encodeJournal = Schema.encodeSync(journalCodec);
const decodeCache = Schema.decodeUnknownOption(cacheCodec);
const encodeCache = Schema.encodeSync(cacheCodec);

export interface DurableHandle extends Handle {
  readonly snapshotCache: Handle["snapshotCache"] & {
    readonly scan: (
      limit: number,
    ) => Effect.Effect<ReadonlyArray<SnapshotMetadata>, PersistenceError>;
  };
}

export const failure = (reason: PersistenceError["reason"], message: string) =>
  PersistenceError.make({ reason, message });

export const storageError = (cause: unknown): PersistenceError =>
  Schema.is(PersistenceError)(cause)
    ? cause
    : failure("Unavailable", `Persistence operation failed: ${String(cause)}`);

const addressKey = (address: Address) => JSON.stringify([address.kind, address.id]);

const rowKey = (row: JournalRow) =>
  JSON.stringify([row.address.kind, row.address.id, row.commandId]);

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

    if (new Set(state.rows.map(rowKey)).size !== state.rows.length)
      throw failure("Unavailable", "Journal contains duplicate identities");

    return state;
  } catch {
    throw failure(
      "Unavailable",
      "Journal format is unreadable; retain the database for migration or recovery",
    );
  }
};

const cacheState = (value: string | undefined): typeof Cache.Type => {
  const decoded = decodeCache(value);

  return decoded._tag === "Some" ? decoded.value : { format: 1, rows: [] };
};

/** Portable policy shared by durable adapters; databases remain separate driver resources. */
export const make = Effect.fn("Persistence.make")(function* (
  options: Options,
  stores: { readonly journal: AtomicStore; readonly cache: AtomicStore },
) {
  const limits = yield* configuration(options);
  const key = JSON.stringify([options.actorId]);

  const initial = yield* stores.journal.modify(key, (stored) => {
    const value =
      stored === undefined
        ? { format: 1 as const, generation: 0, revision: 0, rows: [] }
        : journalState(stored);

    return [value, encodeJournal(value)];
  });

  const generation = initial.generation;
  const cacheKey = JSON.stringify([options.actorId, generation]);
  const lock = Semaphore.makeUnsafe(1);

  const check = (value: typeof Journal.Type) => {
    if (value.generation !== generation)
      throw failure("StaleGeneration", "Persistence handle was invalidated by a wipe");

    return value;
  };

  const read = stores.journal
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

  const transaction: Handle["intentJournal"]["transaction"] = (f) =>
    Effect.gen(function* () {
      const before = yield* read;
      const rows = new Map(before.rows.map((row) => [rowKey(row), row]));
      let active = true;
      let dirty = false;

      const within = <A>(body: () => A) =>
        Effect.try({
          try: () => {
            if (!active) throw failure("Conflict", "Journal transaction is closed");

            return body();
          },
          catch: storageError,
        });

      const write = (input: JournalRow, replace: boolean) =>
        within(() => {
          const row = Schema.decodeSync(JournalRow)(input);
          const id = rowKey(row);

          if (rows.has(id) !== replace)
            throw failure("Conflict", "Journal identity already exists or is missing");
          if (!replace && rows.size >= limits.maxJournalRows)
            throw failure("Capacity", "Journal capacity exhausted");
          rows.set(id, copyJson(row));
          dirty = true;
        });

      const value = yield* Effect.suspend(() =>
        f({
          get: (address, commandId) =>
            within(() => {
              const row = rows.get(rowKey({ address, commandId, value: null }));

              return row === undefined ? undefined : copyJson(row);
            }),
          scan: (address, limit) =>
            within(() => {
              Schema.decodeSync(Positive)(limit);

              return Array.from(rows.values())
                .filter((row) => addressKey(row.address) === addressKey(address))
                .slice(0, limit)
                .map(copyJson);
            }),
          put: (row) => write(row, false),
          replace: (row) => write(row, true),
          remove: (address, commandId) =>
            within(() => {
              dirty = rows.delete(rowKey({ address, commandId, value: null })) || dirty;
            }),
        }),
      ).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            active = false;
          }),
        ),
      );

      yield* stores.journal.modify(key, (stored) => {
        const current = check(journalState(stored));

        if (current.revision !== before.revision)
          throw failure("Conflict", "Journal changed during transaction; no changes committed");

        return [
          undefined,
          dirty
            ? bounded({
                ...current,
                revision: next(current.revision),
                rows: Array.from(rows.values()),
              })
            : stored,
        ];
      });

      return value;
    }).pipe(Semaphore.withPermit(lock));

  const cached = Effect.gen(function* () {
    yield* read;
    const value = cacheState(yield* stores.cache.read(cacheKey));

    yield* read;

    return value;
  });

  const changeCache = (
    f: (rows: ReadonlyArray<typeof Snapshot.Type>) => ReadonlyArray<typeof Snapshot.Type>,
  ) =>
    Effect.gen(function* () {
      yield* read;
      yield* stores.cache.modify(cacheKey, (stored) => [
        undefined,
        encodeCache({ format: 1, rows: f(cacheState(stored).rows) }),
      ]);
      yield* read;
    });

  const handle: DurableHandle = {
    actorId: options.actorId,
    generation,
    intentJournal: { transaction },
    snapshotCache: {
      get: (address) =>
        cached.pipe(
          Effect.map(
            (cache) =>
              cache.rows.find((row) => addressKey(row.address) === addressKey(address))?.value,
          ),
        ),
      scan: (limit) =>
        Effect.gen(function* () {
          yield* Schema.decodeEffect(Positive)(limit).pipe(Effect.mapError(storageError));

          return (yield* cached).rows
            .slice(0, limit)
            .map(({ value: _value, ...metadata }) => metadata);
        }),
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
                bytes(encodeCache({ format: 1, rows })) > limits.maxBytes)
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
        yield* stores.journal.modify(key, (stored) => {
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
      yield* stores.journal.modify(key, (stored) => {
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
      // Only the retired generation's disposable cache is eligible for cleanup.
      yield* stores.cache.modify(cacheKey, () => [undefined, undefined]);
    }),
  };

  return handle;
});
