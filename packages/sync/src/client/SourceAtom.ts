import { type Cause, Effect, Schema, Stream } from "effect";
import { type AsyncResult, Atom } from "effect/reactivity";

import type * as Action from "../Action.ts";
import type { SourceAddress } from "../Model.ts";
import type * as Plugin from "../Plugin.ts";
import type * as Source from "../Source.ts";
import type { CurrentActor } from "./CurrentActor.ts";
import type { Definition } from "./Definition.ts";
import { initial, type ClientError, type Connection, type Replica } from "./Model.ts";
import * as Presence from "./Presence.ts";
import type { ReplicaPersistence } from "./ReplicaPersistence.ts";
import {
  make as makeClient,
  type Actions,
  type Lease,
  type Options,
  type Runtime,
  type Snapshot,
} from "./Runtime.ts";
import type { Transport } from "./Transport.ts";
import { view } from "./View.ts";

export type { Peer, Options as PresenceOptions } from "./Presence.ts";

/** Bindings own leases, while every registry observes the same headless state. */
export const make = <S extends Source.Spec>(runtime: Runtime<S>) => {
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

interface SlotBinding<S extends Plugin.Spec, E, Rejection> {
  /** Snapshot fields contain only this slot's data, with source-wide delivery status. */
  readonly replica: ResultAtom<Replica<S["snapshot"]["Type"], Rejection>, E>;
  readonly action: {
    readonly [A in S["actions"][number] as A["name"]]: Atom.AtomResultFn<
      Action.Payload<A>,
      Action.Success<A>,
      Action.Error<A> | ClientError | E
    >;
  };
  /** Announce this payload and observe other actors while the returned atom is mounted. */
  readonly presence: (
    payload: Atom.Atom<S["message"]["Type"]>,
    options?: Presence.Options,
  ) => ResultAtom<ReadonlyArray<Presence.Peer<S["message"]["Type"]>>, E>;
}

export interface Binding<S extends Source.Spec, E = never> extends SlotBinding<
  S,
  E,
  Action.Error<Actions<S>>
> {
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

/**
 * Define once, or use Atom.family for parameterized addresses. The returned atoms
 * share one lazy client and lease per registry; runtime layers supply external services.
 */
export function bind<S extends Source.Spec, R, E>(
  definition: Definition<S, R>,
  options: BindOptions<NoInfer<S>, NoInfer<R> | CurrentActor | Transport, E> & {
    readonly persistence: { readonly mode: "volatile" };
  },
): Binding<S, E>;

export function bind<S extends Source.Spec, R, E>(
  definition: Definition<S, R>,
  options: BindOptions<NoInfer<S>, NoInfer<R> | CurrentActor | Transport | ReplicaPersistence, E>,
): Binding<S, E>;

export function bind<S extends Source.Spec, R, E>(
  definition: Definition<S, R>,
  options: BindOptions<S, R | CurrentActor | Transport | ReplicaPersistence, E>,
): Binding<S, E> {
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

  const mutation = <Input = void, Output = void, Error = never>(
    run: (lease: Lease<S>, input: Input) => Effect.Effect<Output, Error>,
    settings?: { readonly concurrent?: boolean },
  ) =>
    Atom.fn<Input>()(
      Effect.fnUntraced(function* (input, get) {
        // FnContext reads are untracked; hold the session for the mutation's lifetime.
        get.mount(session);
        const { lease } = yield* get.result(session);

        return yield* run(lease, input);
      }),
      settings,
    ).pipe(Atom.setIdleTTL(0));

  const actionAtoms = <P extends Plugin.Spec>(
    registered: Readonly<Record<string, Action.Definition>>,
    settings?: ActionOptions<P>,
  ): SlotBinding<P, E, Action.Error<Actions<S>>>["action"] => {
    const entries = Object.entries(registered).map(([name, registeredAction]) => {
      const action = registeredAction as Actions<S>;

      return [
        name,
        mutation(
          (lease, payload: Action.Payload<typeof action>) =>
            lease.ready.pipe(Effect.andThen(lease.execute(action, payload))),
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
          presence: (payload: Atom.Atom<unknown>, settings?: Presence.Options) =>
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
    replica,
    status,
    action: actionAtoms<S>(definition.contract.actions, options.actions),
    plugins,
    retry: mutation((lease, commandId: string) =>
      lease.ready.pipe(Effect.andThen(lease.retry(commandId))),
    ),
    recover: mutation((lease) => lease.recover.pipe(Effect.andThen(lease.ready), Effect.asVoid)),
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
