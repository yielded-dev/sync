import type { ClientError, Persistence, PersistenceError } from "@yielded/sync/client";
import { Effect, Exit, Scope } from "effect";

import * as scenarios from "../../sync/test/persistence-scenarios.ts";
import { IndexedDb } from "../src/index.ts";

export const run = (
  scenario:
    | "seed"
    | "restore"
    | "exercise"
    | "concurrent"
    | "runtimeSeed"
    | "runtimeRestore"
    | "inspect"
    | "quarantine"
    | "unavailableCache",
  namespace: string,
) =>
  Effect.runPromise<unknown, PersistenceError | ClientError>(
    scenarios[scenario](IndexedDb.open, namespace),
  );

export const corrupt = (namespace: string, database: "cache" | "journal", value: string) =>
  new Promise<void>((resolve, reject) => {
    const request = indexedDB.open(IndexedDb.databaseNames(namespace)[database]);

    request.onerror = () => reject(request.error);
    request.onsuccess = () => {
      const db = request.result;
      const tx = db.transaction("records", "readwrite");

      tx.objectStore("records").put(value, JSON.stringify(["alice"]));
      tx.oncomplete = () => {
        db.close();
        resolve();
      };
      tx.onabort = () => {
        db.close();
        reject(tx.error);
      };
    };
  });

let held: Persistence.DurableHandle;
let heldScope: Scope.Closeable;

export const hold = async (namespace: string) => {
  heldScope = Scope.makeUnsafe();
  held = await Effect.runPromise(
    IndexedDb.open({ namespace, actorId: "alice" }).pipe(Scope.provide(heldScope)),
  );
};

export const wipe = () => Effect.runPromise(held.wipe);

export const writeHeld = () =>
  Effect.runPromise(
    held.intentJournal
      .transaction((tx) => tx.put(scenarios.evidence))
      .pipe(Effect.match({ onFailure: (error) => error.reason, onSuccess: () => "Success" })),
  );

export const closeHeld = () => Effect.runPromise(Scope.close(heldScope, Exit.void));

export const upgradeCache = (namespace: string) =>
  new Promise<void>((resolve, reject) => {
    const request = indexedDB.open(IndexedDb.databaseNames(namespace).cache, 3);

    request.onerror = () => reject(request.error);
    request.onsuccess = () => {
      request.result.close();
      resolve();
    };
  });

export const seedLegacy = async (namespace: string) => {
  const journal = JSON.stringify({
    format: 1,
    generation: 3,
    revision: 7,
    rows: [scenarios.evidence],
  });

  const names = IndexedDb.databaseNames(namespace);

  await Promise.all(
    (["journal", "cache"] as const).map(
      (kind) =>
        new Promise<void>((resolve, reject) => {
          const request = indexedDB.open(names[kind], 1);

          request.onupgradeneeded = () => request.result.createObjectStore("records");
          request.onerror = () => reject(request.error);
          request.onsuccess = () => {
            const db = request.result;
            const tx = db.transaction("records", "readwrite");
            const records = tx.objectStore("records");

            if (kind === "journal") records.put(journal, JSON.stringify(["alice"]));
            else {
              records.put('{"format":1,"rows":[]}', JSON.stringify(["alice", 1]));
              records.put('{"format":1,"rows":[]}', JSON.stringify(["alice", 3]));
            }
            tx.oncomplete = () => {
              db.close();
              resolve();
            };
            tx.onabort = () => {
              db.close();
              reject(tx.error);
            };
          };
        }),
    ),
  );

  return { journal, evidence: scenarios.evidence };
};

export const records = (namespace: string, kind: "journal" | "cache") =>
  new Promise<{ version: number; keys: IDBValidKey[]; values: unknown[] }>((resolve, reject) => {
    const request = indexedDB.open(IndexedDb.databaseNames(namespace)[kind]);

    request.onerror = () => reject(request.error);
    request.onsuccess = () => {
      const db = request.result;
      const tx = db.transaction("records", "readonly");
      const records = tx.objectStore("records");
      const keys = records.getAllKeys();
      const values = records.getAll();

      tx.oncomplete = () => {
        db.close();
        resolve({ version: db.version, keys: keys.result, values: values.result });
      };
      tx.onabort = () => {
        db.close();
        reject(tx.error);
      };
    };
  });

export const saveSnapshot = (namespace: string) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const storage = yield* IndexedDb.open({ namespace, actorId: "alice" });

        yield* storage.snapshotCache.put(scenarios.evidence.address, { value: 7 });
      }),
    ),
  );
