import { Schema } from "effect";

export class RegistrationError extends Schema.TaggedError<RegistrationError>()(
  "RegistrationError",
  {
    message: Schema.String,
  },
) {}

export type Unique<
  Values extends ReadonlyArray<string>,
  Seen extends string = never,
> = Values extends readonly [infer Head extends string, ...infer Tail extends ReadonlyArray<string>]
  ? string extends Head
    ? Unique<Tail, Seen>
    : Head extends Seen
      ? { readonly registrationError: `Duplicate registration: ${Head}` }
      : Unique<Tail, Seen | Head>
  : unknown;

export type Names<Values extends ReadonlyArray<{ readonly name: string }>> = {
  readonly [Index in keyof Values]: Values[Index]["name"];
};

export type Ids<Values extends ReadonlyArray<{ readonly id: string }>> = {
  readonly [Index in keyof Values]: Values[Index]["id"];
};

export const assertUnique = (label: string, values: ReadonlyArray<string>): void => {
  const seen = new Set<string>();

  for (const value of values) {
    if (seen.has(value)) throw RegistrationError.make({ message: `Duplicate ${label}: ${value}` });
    seen.add(value);
  }
};

export const validate = <S extends Schema.Codec<unknown, unknown>>(
  schema: S,
  value: S["Type"],
  label: string,
): void => {
  const result = Schema.encodeResult(schema)(value);

  if (result._tag === "Failure")
    throw RegistrationError.make({ message: `Invalid ${label}: ${String(value)}` });
};
