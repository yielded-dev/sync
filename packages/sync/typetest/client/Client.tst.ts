import { expectTypeOf } from "@effect/vitest";
import { SourceAtom } from "@yielded/sync/atom";
import { Client, type ClientError } from "@yielded/sync/client";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";

import { Counter, type InvalidValue, Label } from "../../test/fixtures/counter.ts";

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

const client = Client.make(definition, {
  persistence: { mode: "volatile" },
});

expectTypeOf<Effect.Services<typeof client>>().toEqualTypeOf<
  Session | Client.CurrentActor | Client.Transport | Scope.Scope
>();

declare const lease: Client.Lease<typeof Counter.spec>;
const result = lease.execute(Counter.actions.set, { value: 7 });

expectTypeOf<Effect.Success<typeof result>>().toEqualTypeOf<{
  readonly previous: number;
  readonly current: number;
}>();

expectTypeOf<Effect.Error<typeof result>>().toEqualTypeOf<InvalidValue | ClientError>();
const renamed = lease.execute(Counter.plugins.label.actions.rename, { text: "hi" });

expectTypeOf<Effect.Success<typeof renamed>>().toEqualTypeOf<{ readonly text: string }>();
expectTypeOf<Effect.Error<typeof renamed>>().toEqualTypeOf<ClientError>();

// @ts-expect-error A set cannot receive a rename payload.
export const invalidPayload = lease.execute(Counter.actions.set, { text: "wrong" });
declare const runtime: Client.Runtime<typeof Counter.spec>;
const atoms = SourceAtom.make(runtime);
const mutation = atoms.execute(Counter.address("demo"), Counter.actions.set, { value: 1 });

expectTypeOf<Effect.Success<typeof mutation>>().toEqualTypeOf<Effect.Success<typeof result>>();

// Requested offline admission seam preserves payload typing without exposing domain rejection.
const admission = atoms.enqueue(Counter.address("demo"), Counter.actions.set, { value: 1 });

expectTypeOf<Effect.Success<typeof admission>>().toEqualTypeOf<string>();
expectTypeOf<Effect.Error<typeof admission>>().toEqualTypeOf<ClientError>();

export const invalidAdmission = atoms.enqueue(Counter.address("demo"), Counter.actions.set, {
  // @ts-expect-error Admission retains the action's payload contract.
  text: "wrong",
});
