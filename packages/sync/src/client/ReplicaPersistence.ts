import { Context, type Effect, Schema } from "effect";

import { CommandId, SourceAddress, type SourceAddress as Address } from "../Model.ts";

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

export interface JournalChanges {
  readonly insert: ReadonlyArray<JournalRow>;
  readonly replace: ReadonlyArray<JournalRow>;
  readonly remove: ReadonlyArray<string>;
}

/** Captured handles must fence every operation against actor/generation changes. */
export interface IntentJournal {
  readonly load: (
    address: Address,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<JournalRow>, PersistenceError>;
  /**
   * Commit the whole batch or nothing. Rows must belong to address; each identity
   * appears once. Inserts require absence, replacements require an existing row,
   * and capacity failures retain all previously unresolved evidence.
   */
  readonly commit: (
    address: Address,
    changes: JournalChanges,
  ) => Effect.Effect<void, PersistenceError>;
}

/** Disposable snapshots are independent of the required pending-intent journal. */
export interface SnapshotCache {
  readonly get: (address: Address) => Effect.Effect<Schema.Json | undefined, PersistenceError>;
  readonly put: (address: Address, snapshot: Schema.Json) => Effect.Effect<void, PersistenceError>;
  readonly remove: (address: Address) => Effect.Effect<void, PersistenceError>;
}

export interface Handle {
  readonly actorId: string;
  readonly intentJournal: IntentJournal;
  readonly snapshotCache?: SnapshotCache;
}

export class ReplicaPersistence extends Context.Service<ReplicaPersistence, Handle>()(
  "@yielded/sync/client/ReplicaPersistence",
) {}
