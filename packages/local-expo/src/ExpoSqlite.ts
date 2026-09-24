import { Persistence, ReplicaPersistence } from "@yielded/sync/client";
import { Effect, Layer, Schema, Semaphore } from "effect";

export interface Options extends Persistence.Options {
  readonly directory?: string;
}

export const databaseNames = (namespace: string) => ({
  journal: `yielded-sync-${encodeURIComponent(namespace)}-journal.sqlite`,
  cache: `yielded-sync-${encodeURIComponent(namespace)}-cache.sqlite`,
});

const Record = Schema.Struct({ value: Schema.String });

const store = Effect.fn("ExpoSqlite.store")(function* (
  name: string,
  options: Options,
  kind: "journal" | "cache",
) {
  const targetVersion = kind === "journal" ? 1 : 2;
  const table = kind === "journal" ? "records" : "snapshots";
  const lock = Semaphore.makeUnsafe(1);

  const db = yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: async () => {
        const { openDatabaseAsync } = await import("expo-sqlite");

        return openDatabaseAsync(name, { useNewConnection: true }, options.directory);
      },
      catch: Persistence.storageError,
    }),
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
        `CREATE TABLE ${table} (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL); PRAGMA user_version = ${targetVersion};`,
      );
    } else if (kind === "cache" && version?.user_version === 1) {
      await db.execAsync(
        "DROP TABLE records; CREATE TABLE snapshots (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL); PRAGMA user_version = 2;",
      );
    } else
      Schema.decodeUnknownSync(Schema.Struct({ user_version: Schema.Literal(targetVersion) }))(
        version,
      );
  });

  const load = async (key: string) => {
    const row = await db.getFirstAsync(`SELECT value FROM ${table} WHERE key = ?`, key);

    return row === null ? undefined : Schema.decodeUnknownSync(Record)(row).value;
  };

  return {
    read: (key) => exclusive(() => load(key)),
    modify: (key, f) =>
      exclusive(async () => {
        const [value, next] = f(await load(key));

        if (next === undefined) await db.runAsync(`DELETE FROM ${table} WHERE key = ?`, key);
        else
          await db.runAsync(
            `INSERT INTO ${table} (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
            key,
            next,
          );

        return value;
      }),
  } satisfies Persistence.AtomicStore;
});

export const open = Effect.fn("ExpoSqlite.open")(function* (options: Options) {
  const names = yield* Effect.try({
    try: () => databaseNames(options.namespace),
    catch: Persistence.storageError,
  });

  return yield* Persistence.make(options, {
    journal: store(names.journal, options, "journal"),
    cache: store(names.cache, options, "cache"),
  });
});

export const layer = (options: Options) => Layer.effect(ReplicaPersistence, open(options));
