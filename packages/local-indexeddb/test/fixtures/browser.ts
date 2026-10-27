import * as IndexedDb from "@yielded/sync-local-indexeddb/IndexedDb";
import type { Persistence } from "@yielded/sync-persistence";
import { Client, type ClientError, type PersistenceError } from "@yielded/sync/client";
import type * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import * as scenarios from "../../../sync/test/client/utils.ts";

export const run = (
  scenario: "concurrent" | "runtimeSeed" | "runtimeRestore" | "atomOffline",
  namespace: string,
) => {
  const proof: Effect.Effect<
    unknown,
    PersistenceError | ClientError | Cause.NoSuchElementError,
    Client.CurrentActor
  > = scenarios[scenario](IndexedDb.open, namespace);

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

let ownerScope: Scope.Closeable | undefined;

export const ownSource = async (namespace: string, admit = false) => {
  ownerScope = Scope.makeUnsafe();

  const result = await Effect.runPromise(
    scenarios.ownedRuntime(IndexedDb.open, namespace).pipe(
      Effect.flatMap(({ source, storage, admit: enqueue }) =>
        Effect.gen(function* () {
          if (admit) yield* enqueue;

          return {
            pending: (yield* source.read).pending.map((row) => row.commandId),
            rows: yield* storage.intentJournal.load(scenarios.evidence.address, 10),
          };
        }),
      ),
      Effect.provideService(Client.CurrentActor, { actorId: "alice" }),
      Scope.provide(ownerScope),
      Effect.result,
    ),
  );

  return result._tag === "Success"
    ? { _tag: "Success" as const, value: result.success }
    : { _tag: "Failure" as const, failure: { reason: result.failure.reason } };
};

export const closeOwner = () =>
  ownerScope === undefined
    ? Promise.resolve()
    : Effect.runPromise(Scope.close(ownerScope, Exit.void));
