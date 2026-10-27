/**
 * Scoped optimistic replicas with exact retry and ordered recovery.
 *
 * Define pure reducers with `definition`, then construct a runtime with `make`.
 * The runtime captures the actor, transport, and explicit persistence choice
 * for its lifetime. Leases share one coordinator per source address.
 *
 * @since 0.1.0-beta.0
 */

import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";

import type * as Action from "../Action.ts";
import * as internal from "../internal/client.ts";
import type { Slot } from "../internal/client.ts";
import { assertUnique, type Ids, type Unique } from "../internal/registration.ts";
import type * as Plugin from "../Plugin.ts";
import { RegistrationError } from "../RegistrationError.ts";
import type * as Source from "../Source.ts";
import { type CurrentActor } from "./CurrentActor.ts";
import type { ClientError, Replica } from "./Model.ts";
import { type ReplicaPersistence } from "./ReplicaPersistence.ts";
import { type Transport } from "./Transport.ts";

type Optimistic<S extends Plugin.Spec> = {
  readonly [Name in S["actions"][number]["name"]]?: (
    snapshot: S["snapshot"]["Type"],
    payload: Action.Payload<Extract<S["actions"][number], { readonly name: Name }>>,
  ) => S["snapshot"]["Type"];
};

interface Reducers<S extends Plugin.Spec> {
  readonly applyEvent: (
    snapshot: S["snapshot"]["Type"],
    event: S["event"]["Type"],
  ) => S["snapshot"]["Type"];
  readonly optimistic?: Optimistic<S>;
}

/**
 * A plugin's client reducers and the services required to acquire them.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface PluginDefinition<Id extends string = string, R = never> {
  readonly id: Id;
  readonly contract: Plugin.Definition;
  readonly acquire: Effect.Effect<Slot, ClientError, R | Scope.Scope>;
}

/**
 * Client reducers for a shared source contract and their scoped requirements.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Definition<S extends Source.Spec, R = never> {
  readonly contract: Source.Definition<S>;
  readonly allowUnboundCommands: boolean;
  readonly acquire: Effect.Effect<ReadonlyArray<Slot>, ClientError, R | Scope.Scope>;
}

const check = (contract: Plugin.Spec, reducers: object) => {
  const expected = new Set(contract.actions.map((action) => action.name));

  if (Object.keys(reducers).some((key) => !expected.has(key))) {
    throw RegistrationError.make({
      message: "Client optimistic reducers must name declared actions",
    });
  }
};

type Exact<S extends Plugin.Spec, O> = Reducers<S> & {
  readonly optimistic?: O & Record<Exclude<keyof O, S["actions"][number]["name"]>, never>;
};

/**
 * Defines pure reducers for one plugin snapshot slot. Optimistic reducers
 * may cover declared actions; unknown names throw `RegistrationError`.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export const plugin = <Id extends string, S extends Plugin.Spec, O extends Optimistic<NoInfer<S>>>(
  contract: Plugin.Definition<Id, S>,
  reducers: Exact<NoInfer<S>, O>,
): PluginDefinition<Id> => {
  const optimistic = reducers.optimistic ?? {};

  check(contract.spec, optimistic);

  // Only the owning slot's schema-decoded values reach these functions.
  const slot = { id: contract.id, ...reducers, optimistic } as Slot;

  return { id: contract.id, contract, acquire: Effect.succeed(slot) };
};

type Requirements<Plugins extends ReadonlyArray<PluginDefinition<string, unknown>>> = Exclude<
  Effect.Services<Plugins[number]["acquire"]>,
  Scope.Scope
>;

/**
 * Defines source reducers and composes plugin reducers in contract order.
 *
 * Every reducer must be pure and update only its own snapshot slot. Optimistic
 * reducers are optional. Unknown action names or mismatched plugins throw
 * `RegistrationError`. Commands
 * without an admitted authority are disabled unless `allowUnboundCommands` is true.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export const definition = <
  S extends Source.Spec,
  O extends Optimistic<NoInfer<S>>,
  const Plugins extends ReadonlyArray<PluginDefinition<string, unknown>> = readonly [],
>(
  contract: Source.Definition<S>,
  spec: Exact<NoInfer<S>, O> & {
    readonly allowUnboundCommands?: boolean;
    readonly plugins?: Plugins &
      Unique<Ids<Plugins>> &
      ([S["plugins"][number]["id"]] extends [Plugins[number]["id"]]
        ? [Plugins[number]["id"]] extends [S["plugins"][number]["id"]]
          ? unknown
          : never
        : never);
  } & (S["plugins"] extends readonly [] ? unknown : { readonly plugins: Plugins }),
): Definition<S, Requirements<Plugins>> => {
  const optimistic = spec.optimistic ?? {};

  check(contract.spec, optimistic);
  const providedPlugins = spec.plugins ?? [];

  assertUnique(
    "client plugin",
    providedPlugins.map((p) => p.id),
  );

  const plugins = contract.spec.plugins.map((p) =>
    providedPlugins.find((client) => client.contract === p),
  );

  if (providedPlugins.length !== plugins.length || plugins.some((client) => client === undefined)) {
    throw RegistrationError.make({
      message: "Client plugins must exactly match the source contract",
    });
  }

  // Validation retains every implementation in contract order and its union of requirements.
  const implementations = plugins as ReadonlyArray<PluginDefinition<string, Requirements<Plugins>>>;

  return {
    contract,
    allowUnboundCommands: spec.allowUnboundCommands ?? false,
    acquire: Effect.gen(function* () {
      const slots: Array<Slot> = [{ id: "$source", ...spec, optimistic } as Slot];

      for (const client of implementations) {
        slots.push(yield* client.acquire);
      }

      return slots;
    }),
  };
};

/**
 * Provides services while acquiring a client definition. Layer resources
 * belong to the client scope and are released with it.
 *
 * @category context
 * @since 0.1.0-beta.0
 */
export const provide = <S extends Source.Spec, R, Out, In>(
  client: Definition<S, R>,
  layer: Layer.Layer<Out, ClientError, In>,
): Definition<S, Exclude<R, Out> | In> => ({
  ...client,
  acquire: Effect.gen(function* () {
    return yield* client.acquire.pipe(Effect.provideContext(yield* Layer.build(layer)));
  }),
});

/**
 * Provides services to a client plugin in its owning client scope.
 *
 * @category context
 * @since 0.1.0-beta.0
 */
export const providePlugin = <Id extends string, R, Out, In>(
  client: PluginDefinition<Id, R>,
  layer: Layer.Layer<Out, ClientError, In>,
): PluginDefinition<Id, Exclude<R, Out> | In> => ({
  ...client,
  acquire: Effect.gen(function* () {
    return yield* client.acquire.pipe(Effect.provideContext(yield* Layer.build(layer)));
  }),
});

type SourceActions<S extends Source.Spec> =
  | (S["actions"][number] & { readonly namespace: "$source" })
  | {
      [P in S["plugins"][number] as P["id"]]: P["spec"]["actions"][number] & {
        readonly namespace: P["id"];
      };
    }[S["plugins"][number]["id"]];
/**
 * The union of a source's root and plugin actions, including their namespaces.
 *
 * @category type-level
 * @since 0.1.0-beta.0
 */
export type Actions<S extends Source.Spec> = Extract<SourceActions<S>, Action.Definition>;
/**
 * The decoded composite snapshot of a source and its plugins.
 *
 * @category type-level
 * @since 0.1.0-beta.0
 */
export type Snapshot<S extends Source.Spec> = Source.Definition<S>["snapshot"]["Type"];
/**
 * A source-specific replica view with its declared action rejection types.
 *
 * @category type-level
 * @since 0.1.0-beta.0
 */
export type State<S extends Source.Spec> = Replica<Snapshot<S>, Action.Error<Actions<S>>>;

/**
 * Bounds for source coordinators, pending commands, and transport processing.
 *
 * Counts and durations must be finite and positive. Capacity failure refuses
 * new work; it never evicts unresolved commands.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Limits {
  /**
   * Maximum source coordinators. Defaults to 32.
   */
  readonly maxSources: number;
  /**
   * Maximum unresolved commands per source. Defaults to 100.
   */
  readonly maxPendingPerSource: number;
  /**
   * Maximum buffered frames before recovery is required. Defaults to 256.
   */
  readonly frameBuffer: number;
  /**
   * Maximum time for a transport operation. Defaults to 10 seconds.
   */
  readonly operationTimeout?: Duration.Input;
  /**
   * Interval between live-source snapshot checks. Defaults to 30 seconds.
   */
  readonly heartbeatInterval?: Duration.Input;
}

/**
 * Bounded automatic retry of the original command identity and payload.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Retry {
  /**
   * Maximum automatic recovery attempts. Defaults to 8.
   */
  readonly maxAttempts: number;
  /**
   * Initial recovery delay. Defaults to 250 milliseconds.
   */
  readonly initialDelay: Duration.Input;
  /**
   * Maximum exponential recovery delay. Defaults to 30 seconds.
   */
  readonly maxDelay: Duration.Input;
}

/**
 * Client lifetime options. Persistence is an explicit choice; persistent
 * mode requires `ReplicaPersistence` and never falls back to volatile storage.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Options {
  readonly persistence: { readonly mode: "volatile" | "persistent" };
  readonly limits?: Limits;
  readonly retry?: Retry;
}

/**
 * A scoped handle to one source replica.
 *
 * Closing the lease interrupts its operations. Closing the last lease releases
 * the connection while unresolved durable intents remain available for recovery.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Lease<S extends Source.Spec> {
  /**
   * Reads the current view, including provisional and optimistic state.
   */
  readonly read: Effect.Effect<State<S>, ClientError>;
  /**
   * Observes replica views while this lease remains open.
   */
  readonly changes: Stream.Stream<State<S>, ClientError>;
  /**
   * Waits for an authoritative snapshot in the current source generation.
   */
  readonly ready: Effect.Effect<Snapshot<S>, ClientError>;
  /**
   * Journals a command locally and starts background delivery. Returns its
   * command id after local admission without waiting for the network.
   */
  readonly enqueue: <A extends Actions<S>>(
    action: A,
    payload: Action.Payload<NoInfer<A>>,
  ) => Effect.Effect<string, ClientError>;
  /**
   * Captures a new command identity and payload, then submits it. Unknown
   * results retain their identity for exact lookup or retry.
   */
  readonly execute: <A extends Actions<S>>(
    action: A,
    payload: Action.Payload<NoInfer<A>>,
  ) => Effect.Effect<Action.Success<A>, Action.Error<A> | ClientError>;
  /** A bare id can belong to any action in this source; results/errors retain that union. */
  readonly retry: (
    commandId: string,
  ) => Effect.Effect<Action.Success<Actions<S>>, Action.Error<Actions<S>> | ClientError>;
  /**
   * Requests authoritative recovery without discarding pending evidence.
   */
  readonly recover: Effect.Effect<void, ClientError>;
  /**
   * Publishes an ephemeral source message without advancing the cursor.
   */
  readonly publishMessage: (message: S["message"]["Type"]) => Effect.Effect<void, ClientError>;
  /**
   * Publishes an ephemeral message in the selected plugin namespace.
   */
  readonly publishPluginMessage: <Id extends S["plugins"][number]["id"]>(
    id: Id,
    message: Extract<S["plugins"][number], { readonly id: Id }>["message"]["Type"],
  ) => Effect.Effect<void, ClientError>;
  /**
   * Observes ephemeral messages and connection departures.
   */
  readonly messages: Stream.Stream<
    Source.Definition<S>["envelope"]["messageFrame" | "leaveFrame"]["Type"],
    ClientError
  >;
  /**
   * Removes a displayed failure while retaining unresolved command evidence.
   */
  readonly dismissFailure: (commandId: string) => Effect.Effect<void, ClientError>;
}

/**
 * A client session shared by all leases for the same source contract.
 *
 * Passive reads do not connect. Close this runtime's scope before changing the
 * actor, credentials, or persistence namespace.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Runtime<S extends Source.Spec> {
  readonly actorId: string;
  readonly contract: Source.Definition<S>;
  /**
   * Acquires a scoped lease; concurrent opens share a coordinator per address.
   */
  readonly open: (address: Source.Address<S>) => Effect.Effect<Lease<S>, ClientError, Scope.Scope>;
  /**
   * Reads state without acquiring a lease or connecting to the server.
   */
  readonly read: (address: Source.Address<S>) => Effect.Effect<State<S>, ClientError>;
  /** Passive observations never acquire a source lease. Slow observers coalesce views. */
  readonly changes: (address: Source.Address<S>) => Stream.Stream<State<S>, ClientError>;
  /**
   * Waits for queued persistence writes and reports save failures.
   */
  readonly flush: Effect.Effect<void, ClientError>;
}

/**
 * Acquires a client runtime in the caller's scope.
 *
 * Captures `CurrentActor`, `Transport`, and, in persistent mode,
 * `ReplicaPersistence`. Persistent admission is journaled before network I/O.
 * Invalid bounds or an actor mismatch fail with `ClientError`.
 *
 * @example
 * ```ts
 * import * as Client from "@yielded/sync/client/Client"
 * import type * as Source from "@yielded/sync/Source"
 * import * as Effect from "effect/Effect"
 *
 * declare const definition: Client.Definition<Source.Spec>
 *
 * const program = Effect.scoped(Effect.gen(function* () {
 *   const client = yield* Client.make(definition, {
 *     persistence: { mode: "volatile" }
 *   })
 *   const source = yield* client.open(definition.contract.address("shared"))
 *   return yield* source.ready
 * }))
 * ```
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export function make<S extends Source.Spec, R>(
  definition: Definition<S, R>,
  options: Options & { readonly persistence: { readonly mode: "volatile" } },
): Effect.Effect<Runtime<S>, ClientError, R | CurrentActor | Transport | Scope.Scope>;

/**
 * Acquires a client with an explicit persistence choice. Persistent mode
 * captures `ReplicaPersistence` and journals commands before transport.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export function make<S extends Source.Spec, R>(
  definition: Definition<S, R>,
  options: Options,
): Effect.Effect<
  Runtime<S>,
  ClientError,
  R | CurrentActor | Transport | ReplicaPersistence | Scope.Scope
>;

export function make<S extends Source.Spec, R>(definition: Definition<S, R>, options: Options) {
  return internal.make(definition, options);
}

/** @since 0.1.0-beta.0 */
export { CurrentActor } from "./CurrentActor.ts";
/** @since 0.1.0-beta.0 */
export { rpcTransport, layerRpcTransport, layerWebSocketTransport, Transport, type Request, type WebSocketTransportOptions, type SourceWebSocketOptions } from "./Transport.ts";
/** @since 0.1.0-beta.0 */
export { view, type View } from "./View.ts";
/** @since 0.1.0-beta.0 */
export { ClientError, EncodedCommand, Intent, type Replica, type Connection } from "./Model.ts";
