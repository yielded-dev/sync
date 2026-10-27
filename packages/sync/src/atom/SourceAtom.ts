/**
 * Effect Atom bindings over one headless client runtime.
 *
 * Mounted active atoms own source leases. Passive atoms observe the same state
 * without opening connections, and registry disposal releases active leases.
 *
 * @since 0.1.0-beta.0
 */

import type * as Cause from "effect/Cause";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import type * as AsyncResult from "effect/reactivity/AsyncResult";
import * as Atom from "effect/reactivity/Atom";
import * as AtomRegistry from "effect/reactivity/AtomRegistry";
import type * as Reactivity from "effect/reactivity/Reactivity";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import type * as Action from "../Action.ts";
import {
  type Definition,
  make as makeClient,
  type Actions,
  type Lease,
  type Options,
  type Runtime,
  type Snapshot,
} from "../client/Client.ts";
import type { CurrentActor } from "../client/CurrentActor.ts";
import type { ClientError, Connection, Replica } from "../client/Model.ts";
import type { ReplicaPersistence } from "../client/ReplicaPersistence.ts";
import type { Transport } from "../client/Transport.ts";
import { view } from "../client/View.ts";
import { initial } from "../internal/clientModel.ts";
import * as Presence from "../internal/presence.ts";
import type { SourceAddress } from "../Model.ts";
import type * as Plugin from "../Plugin.ts";
import type * as Source from "../Source.ts";

/**
 * One live presence announcement, identified by actor and connection with a local expiry.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Peer<A> {
  readonly actorId: string;
  readonly connectionId: string;
  readonly value: A;
  readonly expiresAtMillis: number;
}

/**
 * Heartbeat, expiry, and peer capacity for an ephemeral presence subscription.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface PresenceOptions {
  /** Defaults to 8 seconds. */
  readonly heartbeatInterval?: Duration.Input;
  /** Defaults to 25 seconds; expiry is checked every 5 seconds or sooner. */
  readonly timeToLive?: Duration.Input;
  /** Defaults to 32 connections, keeping the most recently heard peers. */
  readonly maxPeers?: number;
}

/**
 * Bindings own leases, while every registry observes the same headless state.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export const make = <S extends Source.Spec>(runtime: Runtime<S>): Bindings<S> => {
  type Address = Source.Address<S>;
  const key = (address: SourceAddress) => JSON.stringify([address.kind, address.id]);

  const address = (key: string): Address => {
    const [kind, id] = JSON.parse(key) as [S["kind"], string];

    return Schema.decodeSync(runtime.contract.addressSchema)({ kind, id });
  };

  const passive = Atom.family((key: string) =>
    Atom.make(runtime.changes(address(key)), {
      initialValue: initial<Snapshot<S>, Action.Error<Actions<S>>>(),
    }).pipe(Atom.setIdleTTL(0)),
  );

  const active = Atom.family((key: string) =>
    Atom.make(
      Stream.unwrap(runtime.open(address(key)).pipe(Effect.map((lease) => lease.changes))),
      { initialValue: initial<Snapshot<S>, Action.Error<Actions<S>>>() },
    ).pipe(Atom.setIdleTTL(0)),
  );

  const statuses = Atom.family((key: string) =>
    Atom.make((get) => {
      const result = get(passive(key));

      return result._tag === "Success"
        ? result.value.connection
        : result._tag === "Failure"
          ? ("parked" as const)
          : ("idle" as const);
    }).pipe(Atom.setIdleTTL(0)),
  );

  return {
    replica: (address: Address) => active(key(address)),
    passiveReplica: (address: Address) => passive(key(address)),
    status: (address: Address) => statuses(key(address)),
    execute: <A extends Actions<S>>(
      address: Address,
      action: A,
      payload: Action.Payload<NoInfer<A>>,
    ) =>
      Effect.scoped(
        Effect.gen(function* () {
          const lease = yield* runtime.open(address);

          yield* lease.ready;

          return yield* lease.execute(action, payload);
        }),
      ),
    retry: (address: Address, commandId: string) =>
      Effect.scoped(
        Effect.gen(function* () {
          const lease = yield* runtime.open(address);

          yield* lease.ready;

          return yield* lease.retry(commandId);
        }),
      ),
    recover: (address: Address) =>
      Effect.scoped(
        Effect.gen(function* () {
          const lease = yield* runtime.open(address);

          yield* lease.recover;
          yield* lease.ready;
        }),
      ),
  };
};

type ActionOptions<S extends Plugin.Spec> = {
  readonly [Name in S["actions"][number]["name"]]?: { readonly concurrent?: boolean };
};

interface SlotOptions<S extends Plugin.Spec> {
  /** Uses Effect Atom's default cancellation behavior unless concurrent is enabled. */
  readonly actions?: ActionOptions<S>;
}

/**
 * The Atom runtime, source address, explicit persistence choice, and action concurrency.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface BindOptions<S extends Source.Spec, R, E> extends Options, SlotOptions<S> {
  readonly runtime: Atom.AtomRuntime<R, E>;
  readonly address: Source.Address<S>;
  readonly plugins?: {
    readonly [P in S["plugins"][number] as P["id"]]?: SlotOptions<P["spec"]>;
  };
}

type ResultAtom<A, E> = Atom.Atom<
  AsyncResult.AsyncResult<A, E | ClientError | Cause.NoSuchElementError>
>;

/**
 * Await the original bound action in the current registry. Atom combinators
 * produce derived atoms and need not retain its call signature at runtime.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface ActionAtom<Input, A, E> extends Atom.AtomResultFn<Input, A, E> {
  (input: Input): Effect.Effect<A, E, AtomRegistry.AtomRegistry>;
}

interface SlotBinding<S extends Plugin.Spec, E, Rejection> {
  /** Snapshot fields contain only this slot's data, with source-wide delivery status. */
  readonly replica: ResultAtom<Replica<S["snapshot"]["Type"], Rejection>, E>;
  readonly action: {
    readonly [A in S["actions"][number] as A["name"]]: ActionAtom<
      Action.Payload<A>,
      Action.Success<A>,
      Action.Error<A> | ClientError | E
    >;
  };
  /** Announce this payload and observe other actors while the returned atom is mounted. */
  readonly presence: (
    payload: Atom.Atom<S["message"]["Type"]>,
    options?: PresenceOptions,
  ) => ResultAtom<ReadonlyArray<Peer<S["message"]["Type"]>>, E>;
}

/**
 * Source and plugin views with callable action atoms sharing one scoped client session.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Binding<S extends Source.Spec, E = never, R = never> extends SlotBinding<
  S,
  E,
  Action.Error<Actions<S>>
> {
  /** Create a function atom from a generator using this binding's runtime. */
  readonly fn: <A, E2 = never, Input = void>(
    body: (
      input: Input,
    ) => Effect.fn.Return<
      A,
      E2,
      R | Scope.Scope | AtomRegistry.AtomRegistry | Reactivity.Reactivity
    >,
    options?: {
      readonly initialValue?: A;
      readonly reactivityKeys?: Reactivity.KeysInput;
      readonly concurrent?: boolean;
    },
  ) => Atom.AtomResultFn<Input, A, E | E2>;
  readonly status: Atom.Atom<Connection>;
  readonly retry: Atom.AtomResultFn<
    string,
    Action.Success<Actions<S>>,
    Action.Error<Actions<S>> | ClientError | E
  >;
  readonly recover: Atom.AtomResultFn<void, void, ClientError | E>;
  readonly plugins: {
    readonly [P in S["plugins"][number] as P["id"]]: SlotBinding<
      P["spec"],
      E,
      Action.Error<Actions<S>>
    >;
  };
}

const callable = <Input, A, E>(
  run: (input: Input, get: Atom.FnContext) => Effect.Effect<A, E>,
  options?: { readonly concurrent?: boolean },
): ActionAtom<Input, A, E> => {
  type Invocation = {
    readonly input: Input;
    readonly observe?: (effect: Effect.Effect<A, E>) => Effect.Effect<A, E>;
  };

  const execution = Atom.fn<Invocation>()((invocation, get) => {
    const effect = run(invocation.input, get);

    return invocation.observe?.(effect) ?? effect;
  }, options).pipe(Atom.setIdleTTL(0));

  const writable = Atom.writable(
    (get) => get(execution),
    (get, input: Input | Atom.Reset | Atom.Interrupt) => {
      if (input === Atom.Reset) return get.set(execution, Atom.Reset);
      if (input === Atom.Interrupt) return get.set(execution, Atom.Interrupt);

      get.set(execution, { input });
    },
  ).pipe(Atom.setIdleTTL(0));

  const action: ActionAtom<Input, A, E> = Object.assign(
    (input: Input) =>
      AtomRegistry.AtomRegistry.use((registry) =>
        Effect.callback<A, E>((resume) => {
          const unmount = registry.mount(action);
          let interrupt = Effect.void;
          let cancelled = false;

          registry.set(execution, {
            input,
            observe: (effect) =>
              Effect.withFiber((fiber) => {
                if (cancelled) return Effect.interrupt;
                interrupt = Fiber.interrupt(fiber);

                return effect;
              }).pipe(
                Effect.onExit((exit) =>
                  Effect.sync(() => {
                    interrupt = Effect.void;
                    resume(exit);
                  }),
                ),
              ),
          });
          // Start this invocation before a subsequent write can replace its input.
          registry.get(execution);

          return Effect.gen(function* () {
            cancelled = true;
            yield* interrupt;
            unmount();
          });
        }),
      ),
    writable,
  );

  // Atom's protocol and pipe methods live on its prototype.
  Object.assign(action, Object.getPrototypeOf(writable));

  return action;
};

/**
 * Define once, or use Atom.family for parameterized addresses. The returned atoms
 * share one lazy client and lease per registry; runtime layers supply external services.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export function bind<S extends Source.Spec, R, E, Services = R | CurrentActor | Transport>(
  definition: Definition<S, R>,
  options: BindOptions<NoInfer<S>, NoInfer<R> | CurrentActor | Transport, E> & {
    readonly runtime: Atom.AtomRuntime<Services, E>;
    readonly persistence: { readonly mode: "volatile" };
  },
): Binding<S, E, Services>;

export function bind<
  S extends Source.Spec,
  R,
  E,
  Services = R | CurrentActor | Transport | ReplicaPersistence,
>(
  definition: Definition<S, R>,
  options: BindOptions<
    NoInfer<S>,
    NoInfer<R> | CurrentActor | Transport | ReplicaPersistence,
    E
  > & {
    readonly runtime: Atom.AtomRuntime<Services, E>;
  },
): Binding<S, E, Services>;

export function bind<S extends Source.Spec, R, E, Services>(
  definition: Definition<S, R>,
  options: BindOptions<S, R | CurrentActor | Transport | ReplicaPersistence, E> & {
    readonly runtime: Atom.AtomRuntime<Services, E>;
  },
): Binding<S, E, Services> {
  const session = options.runtime
    .atom(
      Effect.gen(function* () {
        const client = yield* makeClient(definition, options);
        const lease = yield* client.open(options.address);

        return { actorId: client.actorId, lease };
      }),
    )
    .pipe(Atom.setIdleTTL(0));

  const replica = Atom.make(
    (get) => Stream.unwrap(Effect.map(get.result(session), ({ lease }) => view(lease).changes)),
    { initialValue: initial<S["snapshot"]["Type"], Action.Error<Actions<S>>>() },
  ).pipe(Atom.setIdleTTL(0));

  const status = Atom.make((get): Connection => {
    const result = get(replica);

    return result._tag === "Success"
      ? result.value.connection
      : result._tag === "Failure"
        ? "parked"
        : "idle";
  }).pipe(Atom.setIdleTTL(0));

  const withLease = <Input = void, Output = void, Error = never>(
    run: (lease: Lease<S>, input: Input) => Effect.Effect<Output, Error>,
  ) =>
    Effect.fnUntraced(function* (input: Input, get: Atom.FnContext) {
      // FnContext reads are untracked; hold the session for the mutation's lifetime.
      get.mount(session);
      const { lease } = yield* get.result(session);

      return yield* run(lease, input);
    });

  const actionAtoms = <P extends Plugin.Spec>(
    registered: Readonly<Record<string, Action.Definition>>,
    settings?: ActionOptions<P>,
  ): SlotBinding<P, E, Action.Error<Actions<S>>>["action"] => {
    const entries = Object.entries(registered).map(([name, registeredAction]) => {
      const action = registeredAction as Actions<S>;

      return [
        name,
        callable(
          withLease((lease, payload: Action.Payload<typeof action>) =>
            lease.ready.pipe(Effect.andThen(lease.execute(action, payload))),
          ),
          settings?.[name as P["actions"][number]["name"]],
        ),
      ];
    });

    // Each atom closes over the original registered action and its correlated schemas.
    return Object.fromEntries(entries) as SlotBinding<P, E, Action.Error<Actions<S>>>["action"];
  };

  const registeredPlugins: Readonly<Record<string, Pick<Plugin.Definition, "actions">>> =
    definition.contract.plugins;

  const pluginSettings: Readonly<Partial<Record<string, SlotOptions<Plugin.Spec>>>> | undefined =
    options.plugins;

  const plugins = Object.fromEntries(
    definition.contract.spec.plugins.map((plugin) => {
      const id = plugin.id as S["plugins"][number]["id"];

      return [
        id,
        {
          replica: Atom.make(
            (get) =>
              Stream.unwrap(
                Effect.map(get.result(session), ({ lease }) => view(lease, id).changes),
              ),
            { initialValue: initial<unknown, Action.Error<Actions<S>>>() },
          ).pipe(Atom.setIdleTTL(0)),
          action: actionAtoms(registeredPlugins[id].actions, pluginSettings?.[id]?.actions),
          presence: (payload: Atom.Atom<unknown>, settings?: PresenceOptions) =>
            Presence.make(
              options.runtime,
              session,
              status,
              id,
              payload,
              (lease, value) => lease.publishPluginMessage(id, value),
              settings,
            ),
        },
      ];
    }),
  ) as Binding<S, E>["plugins"];

  return {
    fn: (body, settings) => options.runtime.fn(Effect.fnUntraced(body), settings),
    replica,
    status,
    action: actionAtoms<S>(definition.contract.actions, options.actions),
    plugins,
    retry: Atom.fn(
      withLease((lease, commandId: string) =>
        lease.ready.pipe(Effect.andThen(lease.retry(commandId))),
      ),
    ).pipe(Atom.setIdleTTL(0)),
    recover: Atom.fn(
      withLease((lease) => lease.recover.pipe(Effect.andThen(lease.ready), Effect.asVoid)),
    ).pipe(Atom.setIdleTTL(0)),
    presence: (payload, settings) =>
      Presence.make(
        options.runtime,
        session,
        status,
        "$source",
        payload,
        (lease, value) => lease.publishMessage(value),
        settings,
      ),
  };
}

type Address<S extends Source.Spec> = Source.Address<S>;
type ReplicaAtom<S extends Source.Spec> = Atom.Atom<
  AsyncResult.AsyncResult<
    Replica<Snapshot<S>, Action.Error<Actions<S>>>,
    ClientError | Cause.NoSuchElementError
  >
>;

/**
 * Source-addressed atoms and commands sharing the client's replica state.
 *
 * Active atoms own leases; command effects acquire temporary scoped leases.
 * No additional command queue or retry loop is created.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Bindings<S extends Source.Spec> {
  readonly replica: (address: Address<S>) => ReplicaAtom<S>;
  readonly passiveReplica: (address: Address<S>) => ReplicaAtom<S>;
  readonly status: (address: Address<S>) => Atom.Atom<Connection>;
  readonly execute: <A extends Actions<S>>(
    address: Address<S>,
    action: A,
    payload: Action.Payload<NoInfer<A>>,
  ) => Effect.Effect<Action.Success<A>, Action.Error<A> | ClientError>;
  readonly retry: (
    address: Address<S>,
    commandId: string,
  ) => Effect.Effect<Action.Success<Actions<S>>, Action.Error<Actions<S>> | ClientError>;
  readonly recover: (address: Address<S>) => Effect.Effect<void, ClientError>;
}
