import { Schema } from "effect";

import type * as Action from "./Action.ts";
import { registry } from "./internal/action.ts";
import { validate, type Names, type Unique } from "./internal/registration.ts";
import type { WireSchema } from "./Model.ts";

export interface Spec {
  readonly snapshot: WireSchema;
  readonly event: WireSchema;
  readonly message: WireSchema;
  readonly actions: ReadonlyArray<Action.Definition>;
}

export interface Definition<Id extends string = string, S extends Spec = Spec> {
  readonly id: Id;
  readonly spec: S;
  readonly snapshot: S["snapshot"];
  readonly event: S["event"];
  readonly message: S["message"];
  readonly actions: Action.Registry<S["actions"]>;
}

export type ValidId<Id extends string> = Id extends "" | "$source" ? never : Id;

const PluginId = Schema.NonEmptyString.check(
  Schema.makeFilter((id) => id !== "$source", { expected: "a plugin id other than $source" }),
);

export const make = <const Id extends string, const S extends Spec>(
  id: Id & ValidId<Id>,
  input: S & { readonly actions: S["actions"] & Unique<Names<S["actions"]>> },
): Definition<Id, S> => {
  const spec: S = input;

  validate(PluginId, id, "plugin id");
  const actions = registry(spec.actions);
  const frozenSpec = Object.freeze({ ...spec, actions: Object.freeze([...spec.actions]) });

  return Object.freeze({
    id,
    spec: frozenSpec as S,
    snapshot: spec.snapshot,
    event: spec.event,
    message: spec.message,
    actions,
  });
};
