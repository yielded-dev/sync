import { Effect, Layer, type Scope } from "effect";

import type * as Action from "../Action.ts";
import {
  assertUnique,
  RegistrationError,
  type Ids,
  type Unique,
} from "../internal/registration.ts";
import type * as Plugin from "../Plugin.ts";
import type * as Source from "../Source.ts";
import type { ClientError } from "./Model.ts";

type Optimistic<S extends Plugin.Spec> = {
  readonly [Name in S["actions"][number]["name"]]: (
    snapshot: S["snapshot"]["Type"],
    payload: Action.Payload<Extract<S["actions"][number], { readonly name: Name }>>,
  ) => S["snapshot"]["Type"];
};

interface Reducers<S extends Plugin.Spec> {
  readonly applyEvent: (
    snapshot: S["snapshot"]["Type"],
    event: S["event"]["Type"],
  ) => S["snapshot"]["Type"];
  readonly optimistic: Optimistic<S>;
  readonly mergeSnapshot?: (
    current: S["snapshot"]["Type"],
    incoming: S["snapshot"]["Type"],
  ) => S["snapshot"]["Type"];
  readonly mergeHistory?: (
    current: S["snapshot"]["Type"],
    history: S["snapshot"]["Type"],
  ) => S["snapshot"]["Type"];
}

/** @internal The registration boundary retains the schema for every erased slot. */
export interface Slot {
  readonly id: string;
  readonly applyEvent: (snapshot: unknown, event: unknown) => unknown;
  readonly optimistic: Readonly<Record<string, (snapshot: unknown, payload: unknown) => unknown>>;
  readonly mergeSnapshot?: (current: unknown, incoming: unknown) => unknown;
  readonly mergeHistory?: (current: unknown, history: unknown) => unknown;
}

export interface PluginDefinition<Id extends string = string, R = never> {
  readonly id: Id;
  readonly contract: Plugin.Definition;
  readonly acquire: Effect.Effect<Slot, ClientError, R | Scope.Scope>;
}

export interface Definition<S extends Source.Spec, R = never> {
  readonly contract: Source.Definition<S>;
  readonly allowUnboundCommands: boolean;
  readonly acquire: Effect.Effect<ReadonlyArray<Slot>, ClientError, R | Scope.Scope>;
}

const check = (contract: Plugin.Spec, reducers: object) => {
  const expected = new Set(contract.actions.map((action) => action.name));

  if (
    Object.keys(reducers).length !== expected.size ||
    Object.keys(reducers).some((key) => !expected.has(key))
  ) {
    throw RegistrationError.make({
      message: "Client optimistic reducers must exactly match the contract's actions",
    });
  }
};

type Exact<S extends Plugin.Spec, O> = Reducers<S> & {
  readonly optimistic: O & Record<Exclude<keyof O, S["actions"][number]["name"]>, never>;
};

export const plugin = <Id extends string, S extends Plugin.Spec, O extends Optimistic<NoInfer<S>>>(
  contract: Plugin.Definition<Id, S>,
  reducers: Exact<NoInfer<S>, O>,
): PluginDefinition<Id> => {
  check(contract.spec, reducers.optimistic);

  // Only the owning slot's schema-decoded values reach these functions.
  const slot = { id: contract.id, ...reducers } as Slot;

  return { id: contract.id, contract, acquire: Effect.succeed(slot) };
};

type Requirements<Plugins extends ReadonlyArray<PluginDefinition<string, unknown>>> = Exclude<
  Effect.Services<Plugins[number]["acquire"]>,
  Scope.Scope
>;

export const definition = <
  S extends Source.Spec,
  O extends Optimistic<NoInfer<S>>,
  const Plugins extends ReadonlyArray<PluginDefinition<string, unknown>>,
>(
  contract: Source.Definition<S>,
  spec: Exact<NoInfer<S>, O> & {
    readonly allowUnboundCommands?: boolean;
    readonly plugins: Plugins &
      Unique<Ids<Plugins>> &
      ([S["plugins"][number]["id"]] extends [Plugins[number]["id"]]
        ? [Plugins[number]["id"]] extends [S["plugins"][number]["id"]]
          ? unknown
          : never
        : never);
  },
): Definition<S, Requirements<Plugins>> => {
  check(contract.spec, spec.optimistic);
  assertUnique(
    "client plugin",
    spec.plugins.map((p) => p.id),
  );

  if (
    spec.plugins.length !== contract.spec.plugins.length ||
    contract.spec.plugins.some((p) => !spec.plugins.some((client) => client.contract === p))
  ) {
    throw RegistrationError.make({
      message: "Client plugins must exactly match the source contract",
    });
  }

  return {
    contract,
    allowUnboundCommands: spec.allowUnboundCommands ?? false,
    acquire: Effect.gen(function* () {
      const slots: Array<Slot> = [{ id: "$source", ...spec } as Slot];

      for (const p of contract.spec.plugins) {
        const client = spec.plugins.find((candidate) => candidate.contract === p);

        if (client === undefined) return yield* Effect.die("Validated client plugin missing");

        slots.push(yield* (client as PluginDefinition<string, Requirements<Plugins>>).acquire);
      }

      return slots;
    }),
  };
};

export const provide = <S extends Source.Spec, R, Out, In>(
  client: Definition<S, R>,
  layer: Layer.Layer<Out, ClientError, In>,
): Definition<S, Exclude<R, Out> | In> => ({
  ...client,
  acquire: Effect.gen(function* () {
    return yield* client.acquire.pipe(Effect.provideContext(yield* Layer.build(layer)));
  }),
});

export const providePlugin = <Id extends string, R, Out, In>(
  client: PluginDefinition<Id, R>,
  layer: Layer.Layer<Out, ClientError, In>,
): PluginDefinition<Id, Exclude<R, Out> | In> => ({
  ...client,
  acquire: Effect.gen(function* () {
    return yield* client.acquire.pipe(Effect.provideContext(yield* Layer.build(layer)));
  }),
});
