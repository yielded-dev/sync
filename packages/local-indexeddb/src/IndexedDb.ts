import { Persistence, ReplicaPersistence, type PersistenceError } from "@yielded/sync/client";
import { Effect, Layer, Schema } from "effect";

export type Options = Persistence.Options;

/** Stable names: changing a format never silently selects a new journal. */
export const databaseNames = (namespace: string) => ({
  journal: `yielded-sync:${encodeURIComponent(namespace)}:journal`,
  cache: `yielded-sync:${encodeURIComponent(namespace)}:cache`,
});

const connect = (name: string) =>
  Effect.acquireRelease(
    Effect.callback<IDBDatabase, PersistenceError>((resume) => {
      let cancelled = false;
      const request = indexedDB.open(name, 1);

      request.onupgradeneeded = () => {
        request.result.createObjectStore("records");
      };
      request.onsuccess = () => {
        if (cancelled) request.result.close();
        else resume(Effect.succeed(request.result));
      };
      request.onerror = () => resume(Effect.fail(Persistence.storageError(request.error)));
      request.onblocked = () => {
        cancelled = true;
        resume(
          Effect.fail(
            Persistence.failure(
              "Unavailable",
              "IndexedDB upgrade is blocked by another connection",
            ),
          ),
        );
      };

      return Effect.sync(() => {
        cancelled = true;
      });
    }).pipe(Effect.catchDefect((cause) => Effect.fail(Persistence.storageError(cause)))),
    (db) => Effect.sync(() => db.close()),
  );

const store = Effect.fn("IndexedDb.store")(function* (name: string) {
  const db = yield* connect(name);
  let closed = false;

  db.onversionchange = () => {
    closed = true;
    db.close();
  };
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      closed = true;
    }),
  );

  const operation = <A>(
    key: string,
    mode: IDBTransactionMode,
    f: (value: string | undefined) => readonly [A, string | undefined],
  ) =>
    Effect.callback<A, PersistenceError>((resume) => {
      if (closed) {
        resume(Effect.fail(Persistence.failure("Unavailable", "IndexedDB handle is closed")));

        return;
      }
      let transaction: IDBTransaction;

      try {
        transaction = db.transaction("records", mode, { durability: "strict" });
      } catch (cause) {
        resume(Effect.fail(Persistence.storageError(cause)));

        return;
      }
      let result: A;
      let failure: PersistenceError | undefined;
      let complete = false;
      const records = transaction.objectStore("records");
      const request = records.get(key);

      request.onsuccess = () => {
        try {
          const stored: unknown = request.result;

          const value =
            stored === undefined ? undefined : Schema.decodeUnknownSync(Schema.String)(stored);

          const [output, next] = f(value);

          result = output;
          if (mode === "readwrite") {
            if (next === undefined) records.delete(key);
            else records.put(next, key);
          }
        } catch (cause) {
          failure = Persistence.storageError(cause);
          transaction.abort();
        }
      };
      transaction.oncomplete = () => {
        complete = true;
        resume(Effect.succeed(result));
      };
      transaction.onabort = () => {
        complete = true;
        resume(
          Effect.fail(
            failure ??
              (transaction.error?.name === "QuotaExceededError"
                ? Persistence.failure("Capacity", "IndexedDB quota exhausted")
                : Persistence.storageError(transaction.error)),
          ),
        );
      };

      return Effect.sync(() => {
        if (!complete) transaction.abort();
      });
    });

  return {
    read: (key) => operation(key, "readonly", (value) => [value, value]),
    modify: (key, f) => operation(key, "readwrite", f),
  } satisfies Persistence.AtomicStore;
});

export const open = Effect.fn("IndexedDb.open")(function* (options: Options) {
  yield* Persistence.configuration(options);

  const names = yield* Effect.try({
    try: () => databaseNames(options.namespace),
    catch: Persistence.storageError,
  });

  const journal = yield* store(names.journal);

  const cache: Persistence.AtomicStore = yield* store(names.cache).pipe(
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
