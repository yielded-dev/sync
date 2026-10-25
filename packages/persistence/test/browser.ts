import type { Persistence } from "@yielded/sync-persistence";
import { Client, type ClientError, type PersistenceError } from "@yielded/sync/client";
import { Effect, Exit, Schema, Scope } from "effect";

import * as scenarios from "../../sync/test/persistence-scenarios.ts";
import { IndexedDb } from "../src/browser.ts";

export const run = (
  scenario: "concurrent" | "runtimeSeed" | "runtimeRestore",
  namespace: string,
) => {
  const proof: Effect.Effect<unknown, PersistenceError | ClientError, Client.CurrentActor> =
    scenarios[scenario](IndexedDb.open, namespace);

  return Effect.runPromise(
    proof.pipe(Effect.provideService(Client.CurrentActor, { actorId: "alice" })),
  );
};

let held: Persistence.DurableHandle;
let heldScope: Scope.Closeable;

export const hold = async (namespace: string) => {
  heldScope = Scope.makeUnsafe();
  held = await Effect.runPromise(
    IndexedDb.open({ namespace }).pipe(
      Effect.provideService(Client.CurrentActor, { actorId: "alice" }),
      Scope.provide(heldScope),
    ),
  );
};

/** Reproduce a pre-existing format-1 writer retiring the handle held by another tab. */
export const retireLegacyGeneration = async (namespace: string) => {
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(IndexedDb.databaseNames(namespace).journal, 1);

    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });

  try {
    await new Promise<void>((resolve, reject) => {
      const transaction = db.transaction("records", "readwrite");
      const records = transaction.objectStore("records");
      const key = JSON.stringify(["alice"]);
      const request = records.get(key);

      request.onsuccess = () => {
        try {
          const current = Schema.decodeUnknownSync(
            Schema.fromJsonString(
              Schema.Struct({
                format: Schema.Literal(1),
                generation: Schema.Int,
                revision: Schema.Int,
                rows: Schema.Array(Schema.Json),
              }),
            ),
          )(request.result);

          records.put(
            JSON.stringify({
              ...current,
              generation: current.generation + 1,
              revision: current.revision + 1,
              rows: [],
            }),
            key,
          );
        } catch {
          transaction.abort();
        }
      };
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error);
    });
  } finally {
    db.close();
  }
};

export const writeHeld = () =>
  Effect.runPromise(
    held.intentJournal
      .commit(scenarios.evidence.address, { insert: [scenarios.evidence], replace: [], remove: [] })
      .pipe(Effect.match({ onFailure: (error) => error.reason, onSuccess: () => "Success" })),
  );

export const closeHeld = () => Effect.runPromise(Scope.close(heldScope, Exit.void));
