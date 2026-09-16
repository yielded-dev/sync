import { Effect, Schema } from "effect";

import { ProtocolError, type WireSchema } from "../Model.ts";

export const unavailable = (message: string) =>
  ProtocolError.make({ reason: "Unavailable", message });

export const invalid = (message: string) =>
  ProtocolError.make({ reason: "UnsupportedVersion", message });

export const encode = <S extends WireSchema>(
  schema: S,
  value: S["Type"],
): Effect.Effect<Schema.Json, ProtocolError> =>
  Schema.encodeEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError(() => unavailable("Invalid server value")),
  );

export const decode = <S extends WireSchema>(
  schema: S,
  value: unknown,
): Effect.Effect<S["Type"], ProtocolError> =>
  Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(value).pipe(
    Effect.mapError(() => invalid("Value does not match the source contract")),
  );
