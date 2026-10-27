/**
 * IndexedDB persistence for scoped Sync clients.
 *
 * The intent journal and disposable snapshot cache use separate databases.
 * Ordinary scope disposal closes connections and retains stored data.
 *
 * @since 0.1.0-beta.0
 */

import { Persistence } from "@yielded/sync-persistence";
import { ReplicaPersistence, type Client, type PersistenceError } from "@yielded/sync/client";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

/**
 * IndexedDB namespace and capacity options for client persistence.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type Options = Persistence.Options;

/**
 * Stable names: changing a format never silently selects a new journal.
 *
 * @category utilities
 * @since 0.1.0-beta.0
 */
export const databaseNames = (namespace: string) => ({
  journal: `yielded-sync:${encodeURIComponent(namespace)}:journal`,
  cache: `yielded-sync:${encodeURIComponent(namespace)}:cache`,
});

const connect = (name: string, version: 1 | 2) =>
  Effect.acquireRelease(
    Effect.callback<IDBDatabase, PersistenceError>((resume) => {
      let cancelled = false;
      const request = indexedDB.open(name, version);

      request.onupgradeneeded = (event) => {
        if (event.oldVersion === 1) request.result.deleteObjectStore("records");
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

const store = Effect.fn("IndexedDb.store")(function* (name: string, version: 1 | 2) {
  const db = yield* connect(name, version);

  db.onversionchange = () => db.close();

  const operation = <A>(
    key: string,
    mode: IDBTransactionMode,
    f: (value: string | undefined) => readonly [A, string | undefined],
  ) =>
    Effect.callback<A, PersistenceError>((resume) => {
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

const acquire = (name: string) =>
  Effect.acquireRelease(
    Effect.callback<() => Promise<void>, PersistenceError>((resume) => {
      if (globalThis.navigator?.locks === undefined) {
        resume(
          Effect.fail(
            Persistence.failure(
              "Unavailable",
              "Source ownership requires Web Locks in a secure context",
            ),
          ),
        );

        return;
      }
      let cancelled = false;
      let release = () => {};

      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      const request = navigator.locks.request(name, { ifAvailable: true }, (lock) => {
        if (lock === null) {
          resume(
            Effect.fail(
              Persistence.failure("Conflict", "Another runtime owns this actor's persisted source"),
            ),
          );

          return;
        }
        if (cancelled) return;
        resume(
          Effect.succeed(() => {
            release();

            return request;
          }),
        );

        return held;
      });

      request.catch((cause) => resume(Effect.fail(Persistence.storageError(cause))));

      return Effect.sync(() => {
        cancelled = true;
        release();
      });
    }).pipe(Effect.catchDefect((cause) => Effect.fail(Persistence.storageError(cause)))),
    (release) => Effect.promise(release),
  ).pipe(Effect.asVoid);

/**
 * Opens generation-fenced IndexedDB persistence for `CurrentActor`.
 *
 * The caller's scope owns the connections. Unreadable journals remain intact
 * for recovery; disposable cache recovery never removes retry evidence. Clients
 * acquire a Web Lock per namespace, actor, and source. A second coordinator fails
 * until the owning source closes; Web Locks require a secure browser context.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export const open = Effect.fn("IndexedDb.open")(function* (
  options: Options,
): Effect.fn.Return<
  Persistence.DurableHandle,
  PersistenceError,
  Client.CurrentActor | Scope.Scope
> {
  const names = yield* Effect.try({
    try: () => databaseNames(options.namespace),
    catch: Persistence.storageError,
  });

  const handle = yield* Persistence.make(options, {
    journal: store(names.journal, 1),
    cache: store(names.cache, 2),
  });

  return {
    ...handle,
    intentJournal: {
      ...handle.intentJournal,
      acquire: (address) =>
        acquire(JSON.stringify([names.journal, handle.actorId, address.kind, address.id])),
    },
  };
});

/**
 * Provides `ReplicaPersistence` using scoped IndexedDB connections.
 *
 * The current actor is captured when the layer is acquired. Scope disposal
 * retains durable data; logout deletion is explicit application policy.
 *
 * @category layers
 * @since 0.1.0-beta.0
 */
export const layer = (
  options: Options,
): Layer.Layer<ReplicaPersistence, PersistenceError, Client.CurrentActor> =>
  Layer.effect(ReplicaPersistence, open(options));
