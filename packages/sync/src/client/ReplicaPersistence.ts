/**
 * Client persistence ports for atomic intent journals and optional snapshot caches.
 * Implementations capture an actor and fence operations against stale ownership.
 *
 * @since 0.1.0-beta.0
 */

import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { CommandId, SourceAddress, type SourceAddress as Address } from "../Model.ts";

/**
 * An operational storage failure, including capacity, ownership, or atomic batch conflicts.
 *
 * @category errors
 * @since 0.1.0-beta.0
 */
export class PersistenceError extends Schema.TaggedError<PersistenceError>()("PersistenceError", {
  reason: Schema.Literals(["Unavailable", "Capacity", "StaleGeneration", "Conflict"]),
  message: Schema.String,
}) {}

/**
 * One encoded pending command tied to its source address and stable command identity.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export const JournalRow = Schema.Struct({
  address: SourceAddress,
  commandId: CommandId,
  value: Schema.Json,
});

export type JournalRow = typeof JournalRow.Type;

/**
 * An atomic batch of inserts, replacements, and removals for one source address.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface JournalChanges {
  readonly insert: ReadonlyArray<JournalRow>;
  readonly replace: ReadonlyArray<JournalRow>;
  readonly remove: ReadonlyArray<string>;
}

/**
 * Captured handles must fence every operation against actor/generation changes.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
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

/**
 * Disposable snapshots are independent of the required pending-intent journal.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface SnapshotCache {
  readonly get: (address: Address) => Effect.Effect<Schema.Json | undefined, PersistenceError>;
  readonly put: (address: Address, snapshot: Schema.Json) => Effect.Effect<void, PersistenceError>;
  readonly remove: (address: Address) => Effect.Effect<void, PersistenceError>;
}

/**
 * Actor-bound persistence with a required intent journal and optional disposable snapshot cache.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Handle {
  readonly actorId: string;
  readonly intentJournal: IntentJournal;
  readonly snapshotCache?: SnapshotCache;
}

/**
 * The actor-bound persistence handle captured by a persistent client runtime.
 *
 * @category services
 * @since 0.1.0-beta.0
 */
export class ReplicaPersistence extends Context.Service<ReplicaPersistence, Handle>()(
  "@yielded/sync/client/ReplicaPersistence",
) {}
