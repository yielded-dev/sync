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
    const request = indexedDB.open(IndexedDb.databaseNames(namespace)[database], 1);

    request.onerror = () => reject(request.error);
    request.onsuccess = () => {
      const db = request.result;
      const tx = db.transaction("records", "readwrite");

      tx.objectStore("records").put(
        value,
        JSON.stringify(database === "journal" ? ["alice"] : ["alice", 0]),
      );
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
    const request = indexedDB.open(IndexedDb.databaseNames(namespace).cache, 2);

    request.onerror = () => reject(request.error);
    request.onsuccess = () => {
      request.result.close();
      resolve();
    };
  });
