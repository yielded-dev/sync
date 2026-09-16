import { Schema } from "effect";

/** Shared codecs cannot acquire services while encoding or decoding. */
export type WireSchema = Schema.Top & Schema.Codec<unknown, unknown>;

export const SourceAddress = Schema.Struct({
  kind: Schema.NonEmptyString,
  id: Schema.NonEmptyString,
});

export type SourceAddress = typeof SourceAddress.Type;

export const SourceAuthorityGeneration = Schema.String.check(Schema.isUUID());
export const Cursor = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
export const SchemaVersion = Schema.Int.check(Schema.isGreaterThan(0));
export const ProtocolVersion = Schema.Literal(1);
export const CommandId = Schema.NonEmptyString;
export const ActorId = Schema.NonEmptyString;
export const ConnectionId = Schema.NonEmptyString;
export const RequestFingerprint = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));

export const SourcePosition = Schema.Struct({
  sourceAuthorityGeneration: SourceAuthorityGeneration,
  cursor: Cursor,
});

export type SourcePosition = typeof SourcePosition.Type;

/** Positions in different authority generations are unordered. */
export const comparePositions = (
  left: SourcePosition,
  right: SourcePosition,
): -1 | 0 | 1 | undefined => {
  if (left.sourceAuthorityGeneration !== right.sourceAuthorityGeneration) return undefined;

  return left.cursor < right.cursor ? -1 : left.cursor > right.cursor ? 1 : 0;
};

export const actionOutcome = <Success extends WireSchema, Rejection extends WireSchema>(
  success: Success,
  error: Rejection,
) =>
  Schema.Union([
    Schema.TaggedStruct("Succeeded", { result: success, position: SourcePosition }),
    Schema.TaggedStruct("Rejected", { error }),
  ]);

export type ActionOutcome<Success, Rejection> =
  | { readonly _tag: "Succeeded"; readonly result: Success; readonly position: SourcePosition }
  | { readonly _tag: "Rejected"; readonly error: Rejection };

export const resultLookup = <Outcome extends WireSchema>(outcome: Outcome) =>
  Schema.Union([
    Schema.TaggedStruct("Found", { outcome }),
    Schema.TaggedStruct("Unknown", {}),
    Schema.TaggedStruct("Expired", {}),
  ]);

export type ResultLookup<Success, Rejection> =
  | { readonly _tag: "Found"; readonly outcome: ActionOutcome<Success, Rejection> }
  | { readonly _tag: "Unknown" }
  | { readonly _tag: "Expired" };

/** Operational failures never stand in for a durable domain rejection. */
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
