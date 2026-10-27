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

interface Input extends Omit<Spec, "message"> {
  readonly message?: Spec["message"];
}

type Normalized<S extends Input> = S extends Spec
  ? S
  : Omit<S, "message"> & {
      readonly message: "message" extends keyof S
        ?
            | Exclude<S["message"], undefined>
            | (undefined extends S["message"] ? typeof Schema.Never : never)
        : typeof Schema.Never;
    };

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

export function make<const Id extends string, const S extends Input>(
  id: Id & ValidId<Id>,
  input: S & { readonly actions: S["actions"] & Unique<Names<S["actions"]>> } & ([S] extends [Spec]
      ? never
      : unknown),
): Definition<Id, Normalized<S>>;

export function make<const Id extends string, const S extends Spec>(
  id: Id & ValidId<Id>,
  input: S & { readonly actions: S["actions"] & Unique<Names<S["actions"]>> },
): Definition<Id, S>;

export function make(id: string, input: Input): unknown {
  const spec = { ...input, message: input.message ?? Schema.Never };

  validate(PluginId, id, "plugin id");
  const actions = registry(spec.actions);
  const frozenSpec = Object.freeze({ ...spec, actions: Object.freeze([...spec.actions]) });

  return Object.freeze({
    id,
    spec: frozenSpec,
    snapshot: spec.snapshot,
    event: spec.event,
    message: spec.message,
    actions,
  });
}
