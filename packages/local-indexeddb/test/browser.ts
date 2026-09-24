import type { ClientError, Persistence, PersistenceError } from "@yielded/sync/client";
import { Effect, Exit, Scope } from "effect";

import * as scenarios from "../../sync/test/persistence-scenarios.ts";
import { IndexedDb } from "../src/index.ts";

export const run = (
  scenario: "exercise" | "concurrent" | "runtimeSeed" | "runtimeRestore",
  namespace: string,
) =>
  Effect.runPromise<unknown, PersistenceError | ClientError>(
    scenarios[scenario](IndexedDb.open, namespace),
  );

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
