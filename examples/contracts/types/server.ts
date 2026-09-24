import { type ProtocolError } from "@yielded/sync";
import { Server } from "@yielded/sync/server";
import { Context, DateTime, Effect, Layer, Schema, type Scope } from "effect";

import { Counter, InvalidValue, Label } from "../src/counter.ts";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Assert<T extends true> = T;
class Access extends Context.Service<Access, { check: Effect.Effect<void, ProtocolError> }>()(
  "server-example/Access",
) {}
class Audit extends Context.Service<Audit, { record: Effect.Effect<void> }>()(
  "server-example/Audit",
) {}
const Principal = Schema.Struct({ actorId: Schema.String });

const label = Server.plugin(Label, {
  principal: Principal,
  state: Label.snapshot,
  initialize: Effect.succeed({ text: "Untitled" }),
  snapshot: (state) => state,
  actions: {
    rename: Effect.fn(function* ({ payload }) {
      const audit = yield* Audit;

      yield* audit.record;

      return Server.commit({ state: payload, events: [payload], result: payload, outbox: [] });
    }),
  },
});

export const definition = Server.make(Counter, {
  principal: Principal,
  state: Counter.spec.snapshot,
  initialize: Effect.succeed({ value: 0, updatedAt: DateTime.makeUnsafe(0) }),
  snapshot: (state) => state,
  authorize: Effect.fn(function* () {
    yield* (yield* Access).check;
  }),
  actions: {
    set: ({ state, payload }) =>
      payload.value > 100
        ? Effect.fail(InvalidValue.make({ maximum: 100 }))
        : Effect.succeed(
            Server.commit({
              state: { ...state, value: payload.value },
              events: [{ ...state, value: payload.value }],
              result: { previous: state.value, current: payload.value },
              outbox: [],
            }),
          ),
  },
  plugins: [label],
});

export type Requirements = Assert<
  Equal<Effect.Services<typeof definition.acquire>, Access | Audit | Scope.Scope>
>;

const supplied = Server.provide(definition, Layer.succeed(Access, { check: Effect.void }));

export type Remaining = Assert<
  Equal<Effect.Services<typeof supplied.acquire>, Audit | Scope.Scope>
>;
