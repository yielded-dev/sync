/**
 * Authoritative storage, transaction, cryptography, and outbox contracts.
 *
 * Server storage is separate from client persistence. Adapters must preserve
 * atomic commits, exact receipts, ordered replay, and claim-fenced delivery.
 *
 * @since 0.1.0-beta.0
 */

import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { ActorId, CommandId, RequestFingerprint, SourceAddress, SourcePosition } from "../Model.ts";

/**
 * Failure to read or atomically update authoritative storage.
 *
 * @category errors
 * @since 0.1.0-beta.0
 */
export class StorageError extends Schema.TaggedError<StorageError>()("StorageError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/**
 * Schema for the source address, schema version, authority position, and encoded private state.
 * Adapters must retain private `stateVersions` metadata on every head write.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const StoredHead = Schema.Struct({
  address: SourceAddress,
  schemaVersion: Schema.Int,
  position: SourcePosition,
  state: Schema.Json,
  /**
   * Private revisions by slot; absent entries are revision 0.
   * @since 0.1.0-beta.4
   */
  stateVersions: Schema.optionalKey(Schema.Record(Schema.String, Schema.Natural)),
});

/**
 * The source address, schema version, authority position, and encoded private state.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type StoredHead = typeof StoredHead.Type;

/**
 * Schema for the admitted authority, authenticated actor, and original command id.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const ReceiptIdentity = Schema.Struct({
  sourceAuthorityGeneration: Schema.NullOr(SourcePosition.fields.sourceAuthorityGeneration),
  actorId: ActorId,
  commandId: CommandId,
});

/**
 * The admitted authority, authenticated actor, and original command id.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type ReceiptIdentity = typeof ReceiptIdentity.Type;

/**
 * Schema for the exact request fingerprint, captured command, and durable action outcome.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const StoredReceipt = Schema.Struct({
  ...ReceiptIdentity.fields,
  sourceAuthorityGeneration: SourcePosition.fields.sourceAuthorityGeneration,
  fingerprint: RequestFingerprint,
  command: Schema.Json,
  outcome: Schema.Json,
});

/**
 * The exact request fingerprint, captured command, and durable action outcome.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type StoredReceipt = typeof StoredReceipt.Type;

/**
 * Schema for a committed event and the authenticated actor-command pair that produced it.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const StoredEvent = Schema.Struct({
  position: SourcePosition,
  command: Schema.Struct({ actorId: ActorId, commandId: CommandId }),
  event: Schema.Json,
});

/**
 * A committed event and the authenticated actor-command pair that produced it.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type StoredEvent = typeof StoredEvent.Type;

/**
 * Schema for a durable delivery obligation with a stable id, source position, and claim attempt.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const OutboxRecord = Schema.Struct({
  id: Schema.NonEmptyString,
  position: SourcePosition,
  payload: Schema.Json,
  attempts: Schema.Int,
  availableAtMillis: Schema.Finite,
});

/**
 * A durable delivery obligation with a stable id, source position, and claim attempt.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type OutboxRecord = typeof OutboxRecord.Type;

/**
 * The atomic state, event, receipt, and outbox write for one source turn.
 *
 * `expected` fences the commit against the previously read authority position.
 * A failed or interrupted transaction must roll back every part of the plan.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface StorageCommit {
  readonly expected: SourcePosition;
  readonly head: StoredHead;
  readonly events: ReadonlyArray<StoredEvent>;
  readonly receipt: StoredReceipt;
  readonly outbox: ReadonlyArray<OutboxRecord>;
}

/**
 * Storage access valid only within the enclosing serialized transaction.
 *
 * Initialization establishes one authority; reads must form a consistent view.
 * Do not retain this handle or perform unbounded external work in the callback.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface SourceTransaction {
  /**
   * Reads a consistent authority head within this transaction.
   */
  readonly readHead: Effect.Effect<StoredHead | undefined, StorageError>;
  /**
   * Creates the initial authority exactly once; existing authority must not be replaced.
   */
  readonly initialize: (head: StoredHead) => Effect.Effect<void, StorageError>;
  /**
   * Replaces only private state and slot revisions in the current serialized
   * transaction. Requires an existing head; preserves its address, public schema
   * version, and position, and all events, receipts, and outbox records. Failure
   * or interruption must roll back the enclosing transaction. Adapters that omit
   * this capability cannot upgrade existing private state.
   *
   * @since 0.1.0-beta.4
   */
  readonly migrateState?: (
    state: Schema.Json,
    stateVersions: Readonly<Record<string, number>>,
  ) => Effect.Effect<void, StorageError>;
  /**
   * Reads the durable outcome for an authenticated retry identity.
   */
  readonly readReceipt: (
    identity: ReceiptIdentity,
  ) => Effect.Effect<StoredReceipt | undefined, StorageError>;
  /**
   * Reads ordered events after a position, bounded by limit. Gaps must remain detectable.
   */
  readonly readEvents: (
    after: SourcePosition,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<StoredEvent>, StorageError>;
  /**
   * Atomically checks the expected position and commits every part of the plan.
   */
  readonly commit: (plan: StorageCommit) => Effect.Effect<void, StorageError>;
}

/**
 * Optional durable delivery. Commits retain obligations atomically and arm durable
 * wakeups before accepting them. Claims and settlement fence attempts and schedule retries.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface OutboxStorage {
  readonly claim: (
    nowMillis: number,
    leaseMillis: number,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<OutboxRecord>, StorageError>;
  readonly settle: (
    record: OutboxRecord,
    disposition: DeliveryDisposition,
  ) => Effect.Effect<void, StorageError>;
  readonly nextTime: Effect.Effect<number | undefined, StorageError>;
}

/**
 * One handle per authority; callbacks are never automatically re-executed.
 *
 * @category services
 * @since 0.1.0-beta.0
 */
export class SourceStorage extends Context.Service<
  SourceStorage,
  {
    readonly transaction: <A, E, R>(
      body: (tx: SourceTransaction) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | StorageError, R>;
    readonly outbox?: OutboxStorage;
  }
>()("@yielded/sync/server/SourceStorage") {}

/**
 * The application's classification of an outbox delivery attempt.
 *
 * Only `Delivered` removes the obligation. Retries, indeterminate delivery, and
 * permanent failures retain evidence; destinations must be idempotent by record id.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type DeliveryDisposition =
  | { readonly _tag: "Delivered" }
  | { readonly _tag: "Retry" | "Indeterminate"; readonly atMillis: number }
  | { readonly _tag: "PermanentFailure"; readonly reason: string };

/**
 * Host cryptographic primitives for authority ids and request fingerprints.
 *
 * Supply fresh UUID generations and lowercase hexadecimal SHA-256 digests.
 * The runtime owns request canonicalization and correlation.
 *
 * @category services
 * @since 0.1.0-beta.0
 */
export class ServerCrypto extends Context.Service<
  ServerCrypto,
  {
    readonly generation: Effect.Effect<string, StorageError>;
    readonly sha256: (canonicalJson: string) => Effect.Effect<string, StorageError>;
  }
>()("@yielded/sync/server/ServerCrypto") {}
