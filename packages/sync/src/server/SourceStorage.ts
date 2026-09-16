import { Context, type Effect, Schema } from "effect";

import { ActorId, CommandId, RequestFingerprint, SourceAddress, SourcePosition } from "../Model.ts";

export class StorageError extends Schema.TaggedError<StorageError>()("StorageError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export const StoredHead = Schema.Struct({
  address: SourceAddress,
  schemaVersion: Schema.Int,
  position: SourcePosition,
  state: Schema.Json,
});

export type StoredHead = typeof StoredHead.Type;

export const ReceiptIdentity = Schema.Struct({
  sourceAuthorityGeneration: Schema.NullOr(SourcePosition.fields.sourceAuthorityGeneration),
  actorId: ActorId,
  commandId: CommandId,
});

export type ReceiptIdentity = typeof ReceiptIdentity.Type;

export const StoredReceipt = Schema.Struct({
  ...ReceiptIdentity.fields,
  sourceAuthorityGeneration: SourcePosition.fields.sourceAuthorityGeneration,
  fingerprint: RequestFingerprint,
  command: Schema.Json,
  outcome: Schema.Json,
});

export type StoredReceipt = typeof StoredReceipt.Type;

export const StoredEvent = Schema.Struct({
  position: SourcePosition,
  command: Schema.Struct({ actorId: ActorId, commandId: CommandId }),
  event: Schema.Json,
});

export type StoredEvent = typeof StoredEvent.Type;

export const OutboxRecord = Schema.Struct({
  id: Schema.NonEmptyString,
  position: SourcePosition,
  payload: Schema.Json,
  attempts: Schema.Int,
  availableAtMillis: Schema.Finite,
});

export type OutboxRecord = typeof OutboxRecord.Type;

export interface StorageCommit {
  readonly expected: SourcePosition;
  readonly head: StoredHead;
  readonly events: ReadonlyArray<StoredEvent>;
  readonly receipt: StoredReceipt;
  readonly outbox: ReadonlyArray<OutboxRecord>;
}

/** Access is valid only within the enclosing serialized transaction. */
export interface SourceTransaction {
  readonly readHead: Effect.Effect<StoredHead | undefined, StorageError>;
  readonly initialize: (head: StoredHead) => Effect.Effect<void, StorageError>;
  readonly readReceipt: (
    identity: ReceiptIdentity,
  ) => Effect.Effect<StoredReceipt | undefined, StorageError>;
  readonly readEvents: (
    after: SourcePosition,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<StoredEvent>, StorageError>;
  readonly commit: (plan: StorageCommit) => Effect.Effect<void, StorageError>;
}

/** One handle per authority; callbacks are never automatically re-executed. */
export class SourceStorage extends Context.Service<
  SourceStorage,
  {
    readonly transaction: <A, E, R>(
      body: (tx: SourceTransaction) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | StorageError, R>;
    readonly claimOutbox: (
      nowMillis: number,
      leaseMillis: number,
      limit: number,
    ) => Effect.Effect<ReadonlyArray<OutboxRecord>, StorageError>;
    readonly settleOutbox: (
      record: OutboxRecord,
      disposition: DeliveryDisposition,
    ) => Effect.Effect<void, StorageError>;
    readonly nextOutboxTime: Effect.Effect<number | undefined, StorageError>;
  }
>()("@yielded/sync/server/SourceStorage") {}

export type DeliveryDisposition =
  | { readonly _tag: "Delivered" }
  | { readonly _tag: "Retry" | "Indeterminate"; readonly atMillis: number }
  | { readonly _tag: "PermanentFailure"; readonly reason: string };

/** Host cryptographic primitives; the runtime owns canonicalization and correlation. */
export class ServerCrypto extends Context.Service<
  ServerCrypto,
  {
    readonly generation: Effect.Effect<string, StorageError>;
    readonly sha256: (canonicalJson: string) => Effect.Effect<string, StorageError>;
  }
>()("@yielded/sync/server/ServerCrypto") {}
