import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { ClientError, type Connection, type Replica } from "../client/Model.ts";
import type { WireSchema } from "../Model.ts";

export const error = (reason: ClientError["reason"], message: string, commandId?: string) =>
  ClientError.make({ reason, message, ...(commandId === undefined ? {} : { commandId }) });

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
  cacheError: undefined,
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
