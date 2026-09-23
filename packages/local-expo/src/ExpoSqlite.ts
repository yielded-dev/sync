import { Persistence, ReplicaPersistence, type PersistenceError } from "@yielded/sync/client";
import { Effect, Layer, Schema, Semaphore } from "effect";

/** The subset of Expo's async database API used by this adapter. */
export interface Database {
  readonly execAsync: (sql: string) => Promise<void>;
  readonly getFirstAsync: <T>(sql: string, ...params: Array<string>) => Promise<T | null>;
  readonly runAsync: (sql: string, ...params: Array<string>) => Promise<unknown>;
  readonly closeAsync: () => Promise<void>;
}

export interface Options extends Persistence.Options {
  readonly directory?: string;
  /** Driver seam for hosts and storage tests; each call must return a fresh connection. */
  readonly openDatabase?: (name: string) => Effect.Effect<Database, PersistenceError>;
}

export const databaseNames = (namespace: string) => ({
  journal: `yielded-sync-${encodeURIComponent(namespace)}-journal.sqlite`,
  cache: `yielded-sync-${encodeURIComponent(namespace)}-cache.sqlite`,
});

const Version = Schema.Struct({ user_version: Schema.Literal(1) });
const Record = Schema.Struct({ value: Schema.String });

const store = Effect.fn("ExpoSqlite.store")(function* (name: string, options: Options) {
  const lock = Semaphore.makeUnsafe(1);

  const db = yield* Effect.acquireRelease(
    options.openDatabase === undefined
      ? Effect.tryPromise({
          try: async () => {
            const { openDatabaseAsync } = await import("expo-sqlite");

            return openDatabaseAsync(name, { useNewConnection: true }, options.directory);
          },
          catch: Persistence.storageError,
        })
      : options.openDatabase(name),
    (database) =>
      Effect.tryPromise({ try: () => database.closeAsync(), catch: Persistence.storageError }).pipe(
        Effect.orDie,
        Semaphore.withPermit(lock),
      ),
  );

  let closed = false;

  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      closed = true;
    }),
  );

  const exclusive = <A>(f: () => Promise<A>) =>
    Effect.tryPromise({
      try: async () => {
        if (closed) throw Persistence.failure("Unavailable", "SQLite handle is closed");
        await db.execAsync("BEGIN IMMEDIATE");
        try {
          const value = await f();

          await db.execAsync("COMMIT");

          return value;
        } catch (cause) {
          await db.execAsync("ROLLBACK");
          throw cause;
        }
      },
      catch: Persistence.storageError,
    }).pipe(Effect.uninterruptible, Semaphore.withPermit(lock));

  // Private connections prevent unrelated Expo queries from joining this transaction.
  yield* Effect.tryPromise({
    try: () => db.execAsync("PRAGMA busy_timeout = 5000; PRAGMA synchronous = FULL;"),
    catch: Persistence.storageError,
  });
  yield* exclusive(async () => {
    const version = await db.getFirstAsync<{ user_version: number }>("PRAGMA user_version");

    if (version?.user_version === 0) {
      const existing = await db.getFirstAsync(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' LIMIT 1",
      );

      if (existing !== null)
        throw Persistence.failure(
          "Unavailable",
          "Unversioned SQLite database requires explicit migration",
        );
      await db.execAsync(
        "CREATE TABLE records (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL); PRAGMA user_version = 1;",
      );
    } else Schema.decodeUnknownSync(Version)(version);
  });

  const load = async (key: string) => {
    const row = await db.getFirstAsync("SELECT value FROM records WHERE key = ?", key);

    return row === null ? undefined : Schema.decodeUnknownSync(Record)(row).value;
  };

  return {
    read: (key) => exclusive(() => load(key)),
    modify: (key, f) =>
      exclusive(async () => {
        const [value, next] = f(await load(key));

        if (next === undefined) await db.runAsync("DELETE FROM records WHERE key = ?", key);
        else
          await db.runAsync(
            "INSERT INTO records (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            key,
            next,
          );

        return value;
      }),
  } satisfies Persistence.AtomicStore;
});

export const open = Effect.fn("ExpoSqlite.open")(function* (options: Options) {
  yield* Persistence.configuration(options);

  const names = yield* Effect.try({
    try: () => databaseNames(options.namespace),
    catch: Persistence.storageError,
  });

  const journal = yield* store(names.journal, options);

  const cache: Persistence.AtomicStore = yield* store(names.cache, options).pipe(
    Effect.catch((error) =>
      Effect.succeed({
        read: () => Effect.succeed(undefined),
        modify: () => Effect.fail(error),
      }),
    ),
  );

  return yield* Persistence.make(options, { journal, cache });
});

export const layer = (options: Options) => Layer.effect(ReplicaPersistence, open(options));
