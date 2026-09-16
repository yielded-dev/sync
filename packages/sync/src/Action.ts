import { Schema } from "effect";

import type { bind } from "./internal/action.ts";
import { validate } from "./internal/registration.ts";
import type { WireSchema } from "./Model.ts";

export interface Spec {
  readonly payload: WireSchema;
  readonly success: WireSchema;
  readonly error: WireSchema;
}

export interface Definition<Name extends string = string, S extends Spec = Spec> {
  readonly name: Name;
  readonly payload: S["payload"];
  readonly success: S["success"];
  readonly error: S["error"];
}

export const make = <const Name extends string, S extends Spec>(
  name: Name,
  spec: S,
): Definition<Name, S> => {
  validate(Schema.NonEmptyString, name, "action name");

  return Object.freeze({ name, payload: spec.payload, success: spec.success, error: spec.error });
};

export type Payload<A extends Definition> = A["payload"]["Type"];
export type Success<A extends Definition> = A["success"]["Type"];
export type Error<A extends Definition> = A["error"]["Type"];

export type Bound<
  Kind extends string,
  Version extends number,
  Namespace extends string,
  A extends Definition,
> = ReturnType<typeof bind<Kind, Version, Namespace, A>>;

export type Registry<Actions extends ReadonlyArray<Definition>> = {
  readonly [A in Actions[number] as A["name"]]: A;
};
