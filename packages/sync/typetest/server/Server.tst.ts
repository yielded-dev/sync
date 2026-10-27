import { expectTypeOf } from "@effect/vitest";
import { type ProtocolError } from "@yielded/sync";
import { Server } from "@yielded/sync/server";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import { Counter, InvalidValue, Label } from "../../test/fixtures/counter.ts";

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

expectTypeOf<Effect.Services<typeof definition.acquire>>().toEqualTypeOf<
  Access | Audit | Scope.Scope
>();

const supplied = Server.provide(definition, Layer.succeed(Access, { check: Effect.void }));

expectTypeOf<Effect.Services<typeof supplied.acquire>>().toEqualTypeOf<Audit | Scope.Scope>();
