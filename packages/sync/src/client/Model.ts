/**
 * Client replica views and durable pending-command evidence.
 *
 * @since 0.1.0-beta.0
 */

import * as Schema from "effect/Schema";

import { CommandId, SourceAddress, SourceAuthorityGeneration, SourcePosition } from "../Model.ts";

/**
 * A client lifecycle, recovery, transport, or persistence failure.
 *
 * `OutcomeUnknown` does not imply rejection. Keep the original command id for
 * lookup or retry; `AuthorityChanged` requires application reconciliation.
 *
 * @category errors
 * @since 0.1.0-beta.0
 */
export class ClientError extends Schema.TaggedError<ClientError>()("ClientError", {
  reason: Schema.Literals([
    "Closed",
    "Capacity",
    "AuthorityUnknown",
    "AuthorityChanged",
    "OutcomeUnknown",
    "NotPending",
    "Quarantined",
    "InvalidValue",
    "Disconnected",
    "Gap",
    "Overflow",
    "Unauthorized",
    "Storage",
    "Timeout",
  ]),
  message: Schema.String,
  commandId: Schema.optionalKey(CommandId),
}) {}

/**
 * Schema for the captured JSON command sent on every exact retry.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const EncodedCommand = Schema.Struct({
  protocolVersion: Schema.Literal(1),
  commandId: CommandId,
  address: SourceAddress,
  admittedGeneration: Schema.NullOr(SourceAuthorityGeneration),
  namespace: Schema.NonEmptyString,
  action: Schema.NonEmptyString,
  schemaVersion: Schema.Int.check(Schema.isGreaterThan(0)),
  payload: Schema.Json,
});

/**
 * An encoded command with its original identity, payload, and admitted authority.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type EncodedCommand = typeof EncodedCommand.Type;

/**
 * Schema for durable command and result evidence, independent of snapshot caches.
 *
 * Confirmation may arrive before the result. Retain `ConfirmedAwaitingResult`
 * evidence until exact-result lookup resolves it.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const Intent = Schema.Struct({
  formatVersion: Schema.Literal(1),
  command: EncodedCommand,
  order: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  phase: Schema.Literals(["Pending", "ConfirmedAwaitingResult", "Accepted"]),
  boundGeneration: Schema.NullOr(SourceAuthorityGeneration),
  confirmedAt: Schema.NullOr(SourcePosition),
  outcome: Schema.NullOr(Schema.Json),
});

/**
 * A pending command's captured bytes, ordering, authority, and recovery evidence.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type Intent = typeof Intent.Type;

/**
 * The replica connection state. A parked replica retains unresolved work
 * while waiting for explicit recovery or application reconciliation.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type Connection = "idle" | "connecting" | "live" | "recovering" | "parked" | "closed";

/**
 * Automatic recovery diagnostics for this runtime's current command lifetime.
 * Counts reset on explicit source recovery and are not persisted across reloads.
 * Exhaustion stops automatic attempts; exact manual retry remains available.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface CommandRecovery {
  readonly attempts: number;
  readonly lastError: ClientError | undefined;
  readonly automaticRetryExhausted: boolean;
}

/**
 * An observable replica view with optimistic state, durable progress, and failures.
 *
 * `provisional` is cached or unversioned state; only `authoritative` establishes
 * durable progress. Dismissing a failure does not discard unresolved intent.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Replica<Snapshot, Rejection = never> {
  readonly value: Snapshot | undefined;
  readonly authoritative:
    | { readonly position: SourcePosition; readonly snapshot: Snapshot }
    | undefined;
  readonly provisional: Snapshot | undefined;
  readonly connection: Connection;
  readonly pending: ReadonlyArray<{
    readonly commandId: string;
    readonly phase: Intent["phase"] | "Quarantined" | "AuthorityChanged";
    readonly recovery: CommandRecovery;
  }>;
  readonly failures: ReadonlyArray<{
    readonly commandId: string;
    readonly error: Rejection | ClientError;
  }>;
  readonly error: ClientError | undefined;
  readonly cacheError: ClientError | undefined;
}
