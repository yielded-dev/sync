import { Schema } from "effect";

import { assertUnique, type Unique } from "./internal/registration.ts";

export interface Entry {
  readonly kind: string;
  readonly addressSchema: Schema.Top &
    Schema.Codec<{ readonly kind: string; readonly id: string }, unknown>;
}

type Kinds<Entries extends ReadonlyArray<Entry>> = {
  readonly [Index in keyof Entries]: Entries[Index]["kind"];
};

export const make = <const Entries extends ReadonlyArray<Entry>>(
  entries: Entries & Unique<Kinds<Entries>>,
) => {
  assertUnique(
    "source kind",
    entries.map((entry) => entry.kind),
  );
  const byKind = new Map<string, Entries[number]>(entries.map((entry) => [entry.kind, entry]));

  const addressSchemas = entries.map((entry) => entry.addressSchema) as {
    readonly [Index in keyof Entries]: Entries[Index]["addressSchema"];
  };

  function get<Kind extends Entries[number]["kind"]>(
    kind: Kind,
  ): string extends Entries[number]["kind"]
    ? Entries[number] | undefined
    : Extract<Entries[number], { readonly kind: Kind }>;
  function get(kind: string): Entries[number] | undefined;
  function get(kind: string): Entries[number] | undefined {
    return byKind.get(kind);
  }

  return Object.freeze({
    entries: Object.freeze([...entries]),
    get,
    address: Schema.Union(addressSchemas),
  });
};
