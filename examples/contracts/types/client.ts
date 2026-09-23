import { SourceAtom } from "@yielded/sync/atom";
import { Client, type ClientError } from "@yielded/sync/client";
import { Context, Effect, Layer, type Scope } from "effect";

import { Counter, type InvalidValue, Label } from "../src/counter.ts";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Assert<T extends true> = T;
class Session extends Context.Service<Session, { readonly start: Effect.Effect<void> }>()(
  "client-example/Session",
) {}

const label = Client.plugin(Label, {
  applyEvent: (_snapshot, event) => event,
  optimistic: { rename: (_snapshot, payload) => payload },
});

export const definition = Client.provide(
  Client.definition(Counter, {
    applyEvent: (_snapshot, event) => event,
    optimistic: { set: (snapshot, payload) => ({ ...snapshot, value: payload.value }) },
    plugins: [label],
  }),
  Layer.effectDiscard(
    Effect.gen(function* () {
      yield* (yield* Session).start;
    }),
  ),
);

declare const transport: Client.Transport;

const client = Client.make(definition, {
  actorId: "actor",
  transport,
  persistence: { mode: "volatile" },
});

export type Requirements = Assert<Equal<Effect.Services<typeof client>, Session | Scope.Scope>>;
export type Errors = Assert<Equal<Effect.Error<typeof client>, ClientError>>;

declare const lease: Client.Lease<typeof Counter.spec>;
const result = lease.execute(Counter.actions.set, { value: 7 });

export type Success = Assert<
  Equal<Effect.Success<typeof result>, { readonly previous: number; readonly current: number }>
>;

export type Rejection = Assert<Equal<Effect.Error<typeof result>, InvalidValue | ClientError>>;
const renamed = lease.execute(Counter.plugins.label.actions.rename, { text: "hi" });

export type Renamed = Assert<Equal<Effect.Success<typeof renamed>, { readonly text: string }>>;
export type RenameError = Assert<Equal<Effect.Error<typeof renamed>, ClientError>>;

// @ts-expect-error A set cannot receive a rename payload.
export const invalidPayload = lease.execute(Counter.actions.set, { text: "wrong" });
// @ts-expect-error Retry never accepts a replacement payload.
export const invalidRetry = lease.retry("id", { value: 9 });
// @ts-expect-error Persistence must be explicitly selected.
export const invalidPersistence = Client.make(definition, { actorId: "actor", transport });
Client.plugin(Label, {
  applyEvent: (_snapshot, event) => event,
  // @ts-expect-error Every action needs an explicit optimistic reducer (identity is allowed).
  optimistic: {},
});
Client.definition(Counter, {
  applyEvent: (_snapshot, event) => event,
  optimistic: { set: (snapshot) => snapshot },
  // @ts-expect-error Missing client plugin.
  plugins: [],
});

declare const runtime: Client.Runtime<typeof Counter.spec>;
const atoms = SourceAtom.make(runtime);
const mutation = atoms.execute(Counter.address("demo"), Counter.actions.set, { value: 1 });

export type AtomResult = Assert<
  Equal<Effect.Success<typeof mutation>, Effect.Success<typeof result>>
>;
