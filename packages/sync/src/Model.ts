/**
 * Shared identities, positions, action outcomes, and protocol failures.
 *
 * @since 0.1.0-beta.0
 */

import * as Schema from "effect/Schema";

/**
 * A shared codec with no encoding or decoding service requirements.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type WireSchema = Schema.Top & Schema.Codec<unknown, unknown>;

/**
 * Schema for a source kind and nonempty instance id.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const SourceAddress = Schema.Struct({
  kind: Schema.NonEmptyString,
  id: Schema.NonEmptyString,
});

/**
 * The kind and id of one source authority.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type SourceAddress = typeof SourceAddress.Type;

/**
 * UUID schema identifying an authority incarnation. Cursors from different
 * generations cannot be ordered.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const SourceAuthorityGeneration = Schema.String.check(Schema.isUUID());
/**
 * Schema for a nonnegative durable event cursor within one authority generation.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const Cursor = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
/**
 * Schema for a positive application contract version.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const SchemaVersion = Schema.Int.check(Schema.isGreaterThan(0));
/**
 * Schema for the supported Sync wire protocol version.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const ProtocolVersion = Schema.Literal(1);
/**
 * Schema for a nonempty command identity, allocated once and reused on every retry.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const CommandId = Schema.NonEmptyString;
/**
 * Schema for a nonempty authenticated actor id. The host supplies its authority.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const ActorId = Schema.NonEmptyString;
/**
 * Schema for a nonempty connection id used by ephemeral messages and departures.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const ConnectionId = Schema.NonEmptyString;
/**
 * Schema for a lowercase, hexadecimal SHA-256 fingerprint of an exact request.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const RequestFingerprint = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));

/**
 * Schema pairing an authority generation with its durable cursor.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const SourcePosition = Schema.Struct({
  sourceAuthorityGeneration: SourceAuthorityGeneration,
  cursor: Cursor,
});

/**
 * Durable progress within a specific source authority generation.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type SourcePosition = typeof SourcePosition.Type;

/**
 * Compares two positions within one generation. Returns `undefined` for
 * different generations, which are unordered.
 *
 * @category ordering
 * @since 0.1.0-beta.0
 */
export const comparePositions = (
  left: SourcePosition,
  right: SourcePosition,
): -1 | 0 | 1 | undefined => {
  if (left.sourceAuthorityGeneration !== right.sourceAuthorityGeneration) return undefined;

  return left.cursor < right.cursor ? -1 : left.cursor > right.cursor ? 1 : 0;
};

/**
 * Builds the durable success-or-rejection schema for an action. Successful
 * outcomes include the committed source position.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const actionOutcome = <Success extends WireSchema, Rejection extends WireSchema>(
  success: Success,
  error: Rejection,
) =>
  Schema.Union([
    Schema.TaggedStruct("Succeeded", { result: success, position: SourcePosition }),
    Schema.TaggedStruct("Rejected", { error }),
  ]);

/**
 * A durable action result: committed success or a declared domain rejection.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type ActionOutcome<Success, Rejection> =
  | { readonly _tag: "Succeeded"; readonly result: Success; readonly position: SourcePosition }
  | { readonly _tag: "Rejected"; readonly error: Rejection };

/**
 * Builds the schema for a found, unknown, or expired action receipt.
 *
 * Neither `Unknown` nor `Expired` authorizes retrying under a new command id.
 *
 * @category schemas
 * @since 0.1.0-beta.0
 */
export const resultLookup = <Outcome extends WireSchema>(outcome: Outcome) =>
  Schema.Union([
    Schema.TaggedStruct("Found", { outcome }),
    Schema.TaggedStruct("Unknown", {}),
    Schema.TaggedStruct("Expired", {}),
  ]);

/**
 * Receipt lookup evidence for an exact command. An absent receipt does not
 * prove that the command was never executed.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type ResultLookup<Success, Rejection> =
  | { readonly _tag: "Found"; readonly outcome: ActionOutcome<Success, Rejection> }
  | { readonly _tag: "Unknown" }
  | { readonly _tag: "Expired" };

/**
 * An operational, authorization, or protocol failure.
 *
 * This error is never a durable domain rejection. An unavailable reply may
 * leave the command outcome unknown; retain its identity for recovery.
 *
 * @category errors
 * @since 0.1.0-beta.0
 */
export class ProtocolError extends Schema.TaggedError<ProtocolError>()("ProtocolError", {
  reason: Schema.Literals([
    "Unauthenticated",
    "Forbidden",
    "Unavailable",
    "AuthorityMismatch",
    "CommandIdConflict",
    "UnsupportedVersion",
  ]),
  message: Schema.String,
}) {}
