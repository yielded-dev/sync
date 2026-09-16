import { Effect, Layer, Schema, type Scope } from "effect";

import type * as Action from "../Action.ts";
import {
  assertUnique,
  RegistrationError,
  type Ids,
  type Unique,
} from "../internal/registration.ts";
import { ProtocolError, type SourceAddress, type WireSchema } from "../Model.ts";
import type * as Plugin from "../Plugin.ts";
import type * as Source from "../Source.ts";
import { decode, encode } from "./codec.ts";

export { open, type Runtime, type Limits, Session, canonicalJson } from "./Runtime.ts";

export interface Commit<State, Event, Result> {
  readonly state: State;
  readonly events: ReadonlyArray<Event>;
  readonly result: Result;
  readonly outbox: ReadonlyArray<Schema.Json>;
}

export const commit = <State, Event, Result>(
  plan: Commit<State, Event, Result>,
): Commit<State, Event, Result> => plan;

export type AuthorizationOperation =
  | { readonly _tag: "snapshot" | "subscribe" | "message" }
  | {
      readonly _tag: "action" | "result";
      readonly namespace: string;
      readonly action: string;
      readonly commandId: string;
    };

export interface AuthorizationInput<P, State> {
  readonly principal: P;
  readonly address: SourceAddress;
  readonly state: State;
  readonly operation: AuthorizationOperation;
}

type Handlers<S extends Plugin.Spec, State, P, R> = {
  readonly [Name in S["actions"][number]["name"]]: (input: {
    readonly state: State;
    readonly payload: Action.Payload<Extract<S["actions"][number], { readonly name: Name }>>;
    readonly principal: P;
    readonly address: SourceAddress;
  }) => Effect.Effect<
    Commit<
      State,
      S["event"]["Type"],
      Action.Success<Extract<S["actions"][number], { readonly name: Name }>>
    >,
    Action.Error<Extract<S["actions"][number], { readonly name: Name }>> | ProtocolError,
    R
  >;
};
interface SlotSpec<S extends Plugin.Spec, State extends WireSchema, P extends WireSchema, R> {
  readonly principal: P;
  readonly state: State;
  readonly initialize: Effect.Effect<State["Type"], ProtocolError, R>;
  readonly snapshot: (state: State["Type"]) => S["snapshot"]["Type"];
  readonly actions: Handlers<S, State["Type"], P["Type"], R>;
}
interface Evaluated {
  readonly state: Schema.Json;
  readonly events: ReadonlyArray<Schema.Json>;
  readonly result: Schema.Json;
  readonly outbox: ReadonlyArray<Schema.Json>;
}

/** @internal Schema-erased slot, after validating the typed definition. */
export interface Slot {
  readonly id: string;
  readonly initialize: Effect.Effect<Schema.Json, ProtocolError>;
  readonly snapshot: (state: Schema.Json) => Effect.Effect<Schema.Json, ProtocolError>;
  readonly actions: ReadonlyMap<
    string,
    (
      state: Schema.Json,
      payload: unknown,
      principal: unknown,
      address: SourceAddress,
    ) => Effect.Effect<
      | { readonly _tag: "Succeeded"; readonly plan: Evaluated }
      | { readonly _tag: "Rejected"; readonly error: Schema.Json },
      ProtocolError
    >
  >;
}

export interface PluginDefinition<Id extends string = string, R = never> {
  readonly id: Id;
  readonly contract: Plugin.Definition;
  readonly acquire: Effect.Effect<Slot, ProtocolError, R | Scope.Scope>;
}

export interface Definition<S extends Source.Spec, R = never> {
  readonly contract: Source.Definition<S>;
  readonly allowUnboundCommands: boolean;
  readonly acquire: Effect.Effect<
    {
      readonly slots: ReadonlyArray<Slot>;
      readonly authorize: (
        input: AuthorizationInput<unknown, Schema.Json>,
      ) => Effect.Effect<void, ProtocolError>;
    },
    ProtocolError,
    R | Scope.Scope
  >;
}

const acquireSlot = <S extends Plugin.Spec, State extends WireSchema, P extends WireSchema, R>(
  id: string,
  contract: S,
  spec: SlotSpec<S, State, P, R>,
): Effect.Effect<Slot, ProtocolError, R> =>
  Effect.gen(function* () {
    const context = yield* Effect.context<R>();

    const actions = new Map<
      string,
      Slot["actions"] extends ReadonlyMap<string, infer H> ? H : never
    >();

    // Dynamic lookup erases action-specific values; their original schemas validate both directions.
    const handlers = spec.actions as Readonly<
      Record<
        string,
        (input: {
          state: State["Type"];
          payload: unknown;
          principal: P["Type"];
          address: SourceAddress;
        }) => Effect.Effect<Commit<State["Type"], S["event"]["Type"], unknown>, unknown, R>
      >
    >;

    for (const action of contract.actions) {
      const handler = handlers[action.name];

      actions.set(
        action.name,
        Effect.fn("Server.evaluate")(function* (state, payload, principal, address) {
          const result = yield* Effect.result(
            handler({
              state: yield* decode(spec.state, state),
              payload: yield* decode(action.payload, payload),
              principal: yield* decode(spec.principal, principal),
              address,
            }).pipe(Effect.provideContext(context)),
          );

          if (result._tag === "Failure") {
            if (Schema.is(ProtocolError)(result.failure)) return yield* result.failure;
            const rejection = yield* encode(action.error, result.failure);

            return { _tag: "Rejected" as const, error: rejection };
          }
          const plan = result.success;

          return {
            _tag: "Succeeded" as const,
            plan: {
              state: yield* encode(spec.state, plan.state),
              events: yield* Effect.forEach(plan.events, (event) => encode(contract.event, event)),
              result: yield* encode(action.success, plan.result),
              outbox: yield* decode(Schema.Array(Schema.Json), plan.outbox),
            },
          };
        }),
      );
    }

    return {
      id,
      actions,
      initialize: spec.initialize.pipe(
        Effect.provideContext(context),
        Effect.flatMap((state) => encode(spec.state, state)),
      ),
      snapshot: Effect.fn("Server.project")(function* (state) {
        return yield* encode(contract.snapshot, spec.snapshot(yield* decode(spec.state, state)));
      }),
    };
  });

const checkHandlers = (contract: Plugin.Spec, actions: object): void => {
  const expected = new Set(contract.actions.map((action) => action.name));
  const actual = Object.keys(actions);

  if (actual.length !== expected.size || actual.some((name) => !expected.has(name))) {
    throw RegistrationError.make({
      message: "Server handlers must exactly match the contract's actions",
    });
  }
};

type HandlerRequirements<H> = H extends (
  ...args: never[]
) => Effect.Effect<unknown, unknown, infer R>
  ? R
  : never;
type HandlerSpec<
  S extends Plugin.Spec,
  State extends WireSchema,
  P extends WireSchema,
  R,
  H,
> = Omit<SlotSpec<S, State, P, R>, "actions"> & {
  readonly actions: H & Record<Exclude<keyof H, S["actions"][number]["name"]>, never>;
};

export const plugin = <
  Id extends string,
  S extends Plugin.Spec,
  State extends WireSchema,
  P extends WireSchema,
  R,
  H extends Handlers<NoInfer<S>, State["Type"], P["Type"], unknown>,
>(
  contract: Plugin.Definition<Id, S>,
  spec: HandlerSpec<NoInfer<S>, State, P, R, H>,
): PluginDefinition<Id, R | HandlerRequirements<H[keyof H]>> => {
  checkHandlers(contract.spec, spec.actions);

  // The handler map retains each method's inferred requirements until this distribution boundary.
  const slot = spec as SlotSpec<S, State, P, R | HandlerRequirements<H[keyof H]>>;

  return { id: contract.id, contract, acquire: acquireSlot(contract.id, contract.spec, slot) };
};

type PluginRequirements<Plugins extends ReadonlyArray<PluginDefinition<string, unknown>>> = Exclude<
  Effect.Services<Plugins[number]["acquire"]>,
  Scope.Scope
>;

export const make = <
  S extends Source.Spec,
  State extends WireSchema,
  P extends WireSchema,
  R,
  H extends Handlers<NoInfer<S>, State["Type"], P["Type"], unknown>,
  const Plugins extends ReadonlyArray<PluginDefinition<string, unknown>> = readonly [],
>(
  contract: Source.Definition<S>,
  spec: HandlerSpec<NoInfer<S>, State, P, R, H> & {
    readonly allowUnboundCommands?: boolean;
    readonly authorize: (
      input: AuthorizationInput<P["Type"], State["Type"]>,
    ) => Effect.Effect<void, ProtocolError, R>;
    readonly plugins: Plugins &
      Unique<Ids<Plugins>> &
      ([S["plugins"][number]["id"]] extends [Plugins[number]["id"]]
        ? [Plugins[number]["id"]] extends [S["plugins"][number]["id"]]
          ? unknown
          : never
        : never);
  },
): Definition<S, R | HandlerRequirements<H[keyof H]> | PluginRequirements<Plugins>> => {
  checkHandlers(contract.spec, spec.actions);
  assertUnique(
    "server plugin",
    spec.plugins.map((p) => p.id),
  );

  if (
    spec.plugins.length !== contract.spec.plugins.length ||
    contract.spec.plugins.some((p) => !spec.plugins.some((server) => server.contract === p))
  ) {
    throw RegistrationError.make({
      message: "Server plugins must exactly match the source contract",
    });
  }

  return {
    contract,
    allowUnboundCommands: spec.allowUnboundCommands ?? false,
    acquire: Effect.gen(function* () {
      const context = yield* Effect.context<R>();
      const slot = spec as SlotSpec<S, State, P, R | HandlerRequirements<H[keyof H]>>;
      const root = yield* acquireSlot("$source", contract.spec, slot);
      const slots = [root];

      const plugins = spec.plugins as ReadonlyArray<
        PluginDefinition<string, PluginRequirements<Plugins>>
      >;

      for (const definition of contract.spec.plugins) {
        const implementation = plugins.find((p) => p.contract === definition);

        if (implementation === undefined) return yield* Effect.die("Validated plugin missing");
        // The validated tuple's acquired slots retain its union of requirements.
        slots.push(yield* implementation.acquire);
      }

      return {
        slots,
        authorize: Effect.fn("Server.authorize")(function* (
          input: AuthorizationInput<unknown, Schema.Json>,
        ) {
          yield* spec
            .authorize({
              ...input,
              principal: yield* decode(spec.principal, input.principal),
              state: yield* decode(spec.state, input.state),
            })
            .pipe(Effect.provideContext(context));
        }),
      };
    }),
  };
};

/** Layer resources remain in the caller's source scope, in acquisition order. */
export const provide = <S extends Source.Spec, R, Out, In>(
  definition: Definition<S, R>,
  layer: Layer.Layer<Out, ProtocolError, In>,
): Definition<S, Exclude<R, Out> | In> => ({
  contract: definition.contract,
  allowUnboundCommands: definition.allowUnboundCommands,
  acquire: Effect.gen(function* () {
    const services = yield* Layer.build(layer);

    return yield* definition.acquire.pipe(Effect.provideContext(services));
  }),
});

export const providePlugin = <Id extends string, R, Out, In>(
  definition: PluginDefinition<Id, R>,
  layer: Layer.Layer<Out, ProtocolError, In>,
): PluginDefinition<Id, Exclude<R, Out> | In> => ({
  ...definition,
  acquire: Effect.gen(function* () {
    const services = yield* Layer.build(layer);

    return yield* definition.acquire.pipe(Effect.provideContext(services));
  }),
});
