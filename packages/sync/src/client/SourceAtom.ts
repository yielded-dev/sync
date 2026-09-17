import { Effect, Stream } from "effect";
import { Atom } from "effect/unstable/reactivity";

import type * as Action from "../Action.ts";
import type { SourceAddress } from "../Model.ts";
import type * as Source from "../Source.ts";
import { initial } from "./Model.ts";
import type { Actions, Runtime, Snapshot } from "./Runtime.ts";

/** Bindings own leases, while every registry observes the same headless state. */
export const make = <S extends Source.Spec>(runtime: Runtime<S>) => {
  type Address = { readonly kind: S["kind"]; readonly id: string };
  const key = (address: SourceAddress) => JSON.stringify([address.kind, address.id]);

  const address = (key: string): Address => {
    const [kind, id] = JSON.parse(key) as [S["kind"], string];

    return { kind, id };
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
