import { Context, Effect, Layer, Schema, Semaphore } from "effect";

import { ActorId, CommandId, SourceAddress, type SourceAddress as Address } from "../Model.ts";
import { copyJson } from "./Model.ts";

export class PersistenceError extends Schema.TaggedError<PersistenceError>()("PersistenceError", {
  reason: Schema.Literals(["Unavailable", "Capacity", "StaleGeneration", "Conflict"]),
  message: Schema.String,
}) {}

export const JournalRow = Schema.Struct({
  address: SourceAddress,
  commandId: CommandId,
  value: Schema.Json,
});

export type JournalRow = typeof JournalRow.Type;

export interface JournalTransaction {
  readonly get: (
    address: Address,
    commandId: string,
  ) => Effect.Effect<JournalRow | undefined, PersistenceError>;
  readonly scan: (
    address: Address,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<JournalRow>, PersistenceError>;
  /** Inserts only; a reserved identity cannot be overwritten by admission. */
  readonly put: (row: JournalRow) => Effect.Effect<void, PersistenceError>;
  readonly replace: (row: JournalRow) => Effect.Effect<void, PersistenceError>;
  readonly remove: (address: Address, commandId: string) => Effect.Effect<void, PersistenceError>;
}

export interface Handle {
  readonly actorId: string;
  readonly generation: number;
  readonly snapshotCache: {
    readonly get: (address: Address) => Effect.Effect<Schema.Json | undefined, PersistenceError>;
    readonly put: (
      address: Address,
      snapshot: Schema.Json,
    ) => Effect.Effect<void, PersistenceError>;
    readonly remove: (address: Address) => Effect.Effect<void, PersistenceError>;
  };
  readonly intentJournal: {
    readonly transaction: <A>(
      f: (tx: JournalTransaction) => Effect.Effect<A, PersistenceError>,
    ) => Effect.Effect<A, PersistenceError>;
  };
  /** Explicit destructive policy; ordinary source disposal never calls this. */
  readonly purgeSource: (address: Address) => Effect.Effect<void, PersistenceError>;
  /** Invalidates this handle and every handle captured before the wipe. */
  readonly wipe: Effect.Effect<void, PersistenceError>;
}

export class ReplicaPersistence extends Context.Service<ReplicaPersistence, Handle>()(
  "@yielded/sync/client/ReplicaPersistence",
) {
  static layerMemory(options: {
    readonly actorId: string;
    readonly maxJournalRows?: number;
    readonly maxSnapshots?: number;
  }) {
    return layerMemory(options);
  }
}

const addressKey = (address: Address) => JSON.stringify([address.kind, address.id]);

const rowKey = (address: Address, commandId: string) =>
  JSON.stringify([address.kind, address.id, commandId]);

/** Stages one callback; the owning store decides how to commit its result. */
export const stageJournal = Effect.fn("ReplicaPersistence.stageJournal")(function* <A>(
  initial: Iterable<JournalRow>,
  maxRows: number,
  f: (tx: JournalTransaction) => Effect.Effect<A, PersistenceError>,
) {
  const rows = new Map(Array.from(initial, (row) => [rowKey(row.address, row.commandId), row]));
  let active = true;
  let dirty = false;

  const within = <B>(body: () => B) =>
    Effect.try({
      try: () => {
        if (!active)
          throw PersistenceError.make({
            reason: "Conflict",
            message: "Journal transaction is closed",
          });

        return body();
      },
      catch: (cause) =>
        Schema.is(PersistenceError)(cause)
          ? cause
          : PersistenceError.make({ reason: "Unavailable", message: "Invalid journal operation" }),
    });

  const write = (input: JournalRow, replace: boolean) =>
    within(() => {
      const row = Schema.decodeSync(JournalRow)(input);
      const key = rowKey(row.address, row.commandId);

      if (rows.has(key) !== replace)
        throw PersistenceError.make({
          reason: "Conflict",
          message: "Journal identity already exists or is missing",
        });
      if (!replace && rows.size >= maxRows)
        throw PersistenceError.make({ reason: "Capacity", message: "Journal capacity exhausted" });
      rows.set(key, copyJson(row));
      dirty = true;
    });

  const value = yield* Effect.suspend(() =>
    f({
      get: (address, commandId) =>
        within(() => {
          const row = rows.get(rowKey(address, commandId));

          return row === undefined ? undefined : copyJson(row);
        }),
      scan: (address, limit) =>
        within(() => {
          Schema.decodeSync(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)))(limit);

          return Array.from(rows.values())
            .filter((row) => addressKey(row.address) === addressKey(address))
            .slice(0, limit)
            .map(copyJson);
        }),
      put: (row) => write(row, false),
      replace: (row) => write(row, true),
      remove: (address, commandId) =>
        within(() => {
          dirty = rows.delete(rowKey(address, commandId)) || dirty;
        }),
    }),
  ).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        active = false;
      }),
    ),
  );

  return { value, rows, dirty };
});

/** Process-local storage only. Reuse the factory to exercise actor remounts and wipes. */
export const memory = (options?: {
  readonly maxJournalRows?: number;
  readonly maxSnapshots?: number;
}) => {
  const actors = new Map<
    string,
    {
      generation: number;
      journal: Map<string, JournalRow>;
      cache: Map<string, Schema.Json>;
      lock: Semaphore.Semaphore;
    }
  >();

  const open = Effect.fn("ReplicaPersistence.memory.open")(function* (actorId: string) {
    yield* Schema.decodeEffect(ActorId)(actorId).pipe(Effect.orDie);

    const state = actors.get(actorId) ?? {
      generation: 0,
      journal: new Map<string, JournalRow>(),
      cache: new Map<string, Schema.Json>(),
      lock: Semaphore.makeUnsafe(1),
    };

    actors.set(actorId, state);
    const generation = state.generation;

    const assertGeneration = Effect.suspend(() =>
      generation === state.generation
        ? Effect.void
        : Effect.fail(
            PersistenceError.make({
              reason: "StaleGeneration",
              message: "Persistence handle was invalidated by a wipe",
            }),
          ),
    );

    const transaction: Handle["intentJournal"]["transaction"] = (f) =>
      Effect.gen(function* () {
        yield* assertGeneration;

        const { value, rows } = yield* stageJournal(
          state.journal.values(),
          options?.maxJournalRows ?? 4096,
          f,
        );

        yield* assertGeneration;
        state.journal = rows;

        return value;
      }).pipe(Semaphore.withPermit(state.lock));

    const handle: Handle = {
      actorId,
      generation,
      intentJournal: { transaction },
      snapshotCache: {
        get: (address) =>
          Effect.gen(function* () {
            yield* assertGeneration;
            const value = state.cache.get(addressKey(address));

            return value === undefined ? undefined : copyJson(value);
          }).pipe(Semaphore.withPermit(state.lock)),
        put: (address, value) =>
          Effect.gen(function* () {
            yield* assertGeneration;
            const key = addressKey(address);

            state.cache.delete(key);
            state.cache.set(key, copyJson(value));
            while (state.cache.size > (options?.maxSnapshots ?? 128)) {
              const oldest = state.cache.keys().next();

              if (oldest.done) break;
              state.cache.delete(oldest.value);
            }
          }).pipe(Semaphore.withPermit(state.lock)),
        remove: (address) =>
          Effect.gen(function* () {
            yield* assertGeneration;
            state.cache.delete(addressKey(address));
          }).pipe(Semaphore.withPermit(state.lock)),
      },
      purgeSource: (address) =>
        Effect.gen(function* () {
          yield* assertGeneration;
          for (const [key, row] of state.journal) {
            if (addressKey(row.address) === addressKey(address)) state.journal.delete(key);
          }
          state.cache.delete(addressKey(address));
        }).pipe(Semaphore.withPermit(state.lock)),
      wipe: Effect.gen(function* () {
        yield* assertGeneration;
        state.generation += 1;
        state.journal.clear();
        state.cache.clear();
      }).pipe(Semaphore.withPermit(state.lock)),
    };

    return handle;
  });

  return { open };
};

export const layerMemory = (options: {
  readonly actorId: string;
  readonly maxJournalRows?: number;
  readonly maxSnapshots?: number;
}) =>
  Layer.effect(
    ReplicaPersistence,
    Effect.suspend(() => memory(options).open(options.actorId)),
  );
