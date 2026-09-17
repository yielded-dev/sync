import { Effect, Schema } from "effect";

import {
  CommandId,
  SourceAddress,
  SourceAuthorityGeneration,
  SourcePosition,
  type WireSchema,
} from "../Model.ts";

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

export const error = (reason: ClientError["reason"], message: string, commandId?: string) =>
  ClientError.make({ reason, message, ...(commandId === undefined ? {} : { commandId }) });

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

export type EncodedCommand = typeof EncodedCommand.Type;

/** Exact command bytes and result evidence are independent of the snapshot cache. */
export const Intent = Schema.Struct({
  formatVersion: Schema.Literal(1),
  command: EncodedCommand,
  order: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  phase: Schema.Literals(["Pending", "ConfirmedAwaitingResult", "Accepted"]),
  boundGeneration: Schema.NullOr(SourceAuthorityGeneration),
  confirmedAt: Schema.NullOr(SourcePosition),
  outcome: Schema.NullOr(Schema.Json),
});

export type Intent = typeof Intent.Type;

export type Connection = "idle" | "connecting" | "live" | "recovering" | "parked" | "closed";

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
  }>;
  readonly failures: ReadonlyArray<{
    readonly commandId: string;
    readonly error: Rejection | ClientError;
  }>;
  readonly error: ClientError | undefined;
}

export const initial = <Snapshot, Rejection = never>(
  connection: Connection = "idle",
): Replica<Snapshot, Rejection> => ({
  value: undefined,
  authoritative: undefined,
  provisional: undefined,
  connection,
  pending: [],
  failures: [],
  error: undefined,
});

export const encode = <S extends WireSchema>(
  schema: S,
  value: S["Type"],
): Effect.Effect<Schema.Json, ClientError> =>
  Schema.encodeEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError(() => error("InvalidValue", "Value does not match the source codec")),
  );

export const decode = <S extends WireSchema>(
  schema: S,
  value: unknown,
): Effect.Effect<S["Type"], ClientError> =>
  Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError(() => error("InvalidValue", "Value does not match the source codec")),
  );

export const copyJson = <A extends Schema.Json>(value: A): A =>
  JSON.parse(JSON.stringify(value)) as A;
