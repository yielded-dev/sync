/**
 * Private source handlers, authorization, and the authoritative runtime.
 *
 * Handlers construct commit plans. The runtime validates and atomically commits
 * state, events, receipts, and outbox work before returning success.
 *
 * @since 0.1.0-beta.0
 */

import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import type * as Action from "../Action.ts";
import { assertUnique, type Ids, type Unique } from "../internal/registration.ts";
import type { Session, Slot } from "../internal/server.ts";
import { decode, encode } from "../internal/serverCodec.ts";
import {
  ProtocolError,
  type SourceAddress,
  type WireSchema,
  type SourcePosition,
} from "../Model.ts";
import type * as Plugin from "../Plugin.ts";
import { RegistrationError } from "../RegistrationError.ts";
import type * as Source from "../Source.ts";
import type { StoredEvent } from "./SourceStorage.ts";

/** @since 0.1.0-beta.0 */
export { open, Session } from "../internal/server.ts";
/** @since 0.1.0-beta.0 */
export { host, type Host, type HostOptions, type Connection, type LiveConnection } from "./Host.ts";

/**
 * Positive bounds for a serialized source turn. All fields are required
 * when overriding the default limits.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Limits {
  /**
   * Maximum events per commit or replay batch. Defaults to 512.
   */
  readonly maxEvents: number;
  /**
   * Maximum outbox obligations in one commit. Defaults to 100.
   */
  readonly maxOutbox: number;
  /**
   * Maximum encoded commit or replay batch size. Defaults to 600000 bytes.
   */
  readonly maxBytes: number;
  /**
   * Maximum serialized turn duration. Defaults to 5000 milliseconds.
   */
  readonly turnTimeoutMillis: number;
}

/**
 * An authoritative source instance with authenticated operations.
 *
 * Authorization, receipt lookup, handler evaluation, and commit share one
 * serialized storage turn. The enclosing scope owns acquired handler services.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Runtime<S extends Source.Spec> {
  readonly contract: Source.Definition<S>;
  readonly address: SourceAddress;
  /**
   * Authorizes and reads the current public snapshot.
   */
  readonly snapshot: (
    session: Session,
  ) => Effect.Effect<Source.Definition<S>["envelope"]["snapshotFrame"]["Type"], ProtocolError>;
  /**
   * Returns replay after a position, a snapshot, or an explicit resynchronization gap.
   */
  readonly bootstrap: (
    session: Session,
    after?: SourcePosition,
  ) => Effect.Effect<
    | Source.Definition<S>["envelope"]["snapshotFrame"]["Type"]
    | Source.Definition<S>["envelope"]["eventsFrame"]["Type"]
    | Source.Definition<S>["envelope"]["gapFrame"]["Type"],
    ProtocolError
  >;
  /**
   * Authorizes and executes a typed action, or returns its exact durable outcome.
   */
  readonly execute: <A extends { readonly command: WireSchema; readonly outcome: WireSchema }>(
    session: Session,
    action: A,
    input: A["command"]["Type"],
  ) => Effect.Effect<A["outcome"]["Type"], ProtocolError>;
  /**
   * Looks up the outcome for this authenticated actor and exact command.
   */
  readonly result: <A extends { readonly command: WireSchema; readonly lookup: WireSchema }>(
    session: Session,
    action: A,
    input: A["command"]["Type"],
  ) => Effect.Effect<A["lookup"]["Type"], ProtocolError>;
  /**
   * Encoded action dispatch for transport adapters; events are returned only after commit.
   */
  readonly dispatch: (
    session: Session,
    input: unknown,
  ) => Effect.Effect<
    { readonly outcome: Schema.Json; readonly events: ReadonlyArray<StoredEvent> },
    ProtocolError
  >;
  /**
   * Encoded exact-result lookup for transport adapters.
   */
  readonly lookup: (
    session: Session,
    input: unknown,
  ) => Effect.Effect<
    { readonly _tag: "Unknown" } | { readonly _tag: "Found"; readonly outcome: Schema.Json },
    ProtocolError
  >;
  /**
   * Authorizes an ephemeral message without committing events or advancing the cursor.
   */
  readonly publishMessage: (
    session: Session,
    message: unknown,
  ) => Effect.Effect<Source.Definition<S>["envelope"]["messageFrame"]["Type"], ProtocolError>;
  /**
   * Revalidates subscription authorization and session expiry.
   */
  readonly checkSubscription: (session: Session) => Effect.Effect<void, ProtocolError>;
}

/**
 * A proposed action commit: next private state, public events, result, and
 * JSON outbox obligations. A changed state must emit at least one event.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Commit<State, Event, Result> {
  readonly state: State;
  readonly events: ReadonlyArray<Event>;
  readonly result: Result;
  readonly outbox?: ReadonlyArray<Schema.Json>;
}

/**
 * Constructs a typed commit plan without performing I/O.
 *
 * The runtime validates and commits the complete plan atomically. Keep
 * external side effects in `outbox`, outside the serialized source turn.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export const commit = <State, Event, Result>(
  plan: Commit<State, Event, Result>,
): Required<Commit<State, Event, Result>> => ({ ...plan, outbox: plan.outbox ?? [] });

/**
 * The operation being authorized, including the exact command identity
 * for action execution and result lookup.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type AuthorizationOperation =
  | { readonly _tag: "snapshot" | "subscribe" | "message" }
  | {
      readonly _tag: "action" | "result";
      readonly namespace: string;
      readonly action: string;
      readonly commandId: string;
    };

/**
 * The authenticated principal, source address, private root state, and
 * operation passed to the application's authorization policy.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
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

/**
 * A plugin's private server implementation and its scoped service requirements.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface PluginDefinition<Id extends string = string, R = never> {
  readonly id: Id;
  readonly contract: Plugin.Definition;
  /**
   * @internal Acquires the validated runtime implementation.
   */
  readonly acquire: Effect.Effect<Slot, ProtocolError, R | Scope.Scope>;
}

/**
 * A source's private implementation, authorization policy, and requirements.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Definition<S extends Source.Spec, R = never> {
  readonly contract: Source.Definition<S>;
  readonly allowUnboundCommands: boolean;
  /**
   * @internal Acquires the validated runtime implementation.
   */
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
        }) => Effect.Effect<
          Commit<State["Type"], S["event"]["Type"], Action.Success<S["actions"][number]>>,
          Action.Error<S["actions"][number]> | ProtocolError,
          R
        >
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
              outbox: yield* decode(Schema.Array(Schema.Json), plan.outbox ?? []),
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

/**
 * Implements one plugin's private state, initialization, projection, and actions.
 *
 * Handlers must exactly match the plugin contract or registration throws
 * `RegistrationError`. Each handler can update only its own slot.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
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

/**
 * Implements a shared source contract with private state and authorization.
 *
 * Handlers and plugins must exactly match the contract or registration throws
 * `RegistrationError`. Plugin services are acquired in declaration order and
 * finalized in reverse order. Commands without an admitted authority require
 * explicit `allowUnboundCommands` opt-in on both server and client.
 *
 * Every authorized viewer receives the same public snapshot. Use a separate
 * source authorization boundary for viewer-private data.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
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
    readonly plugins?: Plugins &
      Unique<Ids<Plugins>> &
      ([S["plugins"][number]["id"]] extends [Plugins[number]["id"]]
        ? [Plugins[number]["id"]] extends [S["plugins"][number]["id"]]
          ? unknown
          : never
        : never);
  } & (S["plugins"] extends readonly [] ? unknown : { readonly plugins: Plugins }),
): Definition<S, R | HandlerRequirements<H[keyof H]> | PluginRequirements<Plugins>> => {
  checkHandlers(contract.spec, spec.actions);
  const providedPlugins = spec.plugins ?? [];

  assertUnique(
    "server plugin",
    providedPlugins.map((p) => p.id),
  );

  if (
    providedPlugins.length !== contract.spec.plugins.length ||
    contract.spec.plugins.some((p) => !providedPlugins.some((server) => server.contract === p))
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

      const plugins = providedPlugins as ReadonlyArray<
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

/**
 * Provides services to a source implementation in its owning operation scope.
 *
 * Layer resources remain scoped and are released in reverse acquisition order.
 *
 * @category context
 * @since 0.1.0-beta.0
 */
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

/**
 * Provides services to a server plugin in its owning source operation scope.
 *
 * @category context
 * @since 0.1.0-beta.0
 */
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
