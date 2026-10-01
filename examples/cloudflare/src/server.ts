import { ProtocolError } from "@yielded/sync";
import { Server } from "@yielded/sync/server";
import { Effect, Schema } from "effect";

import { Counter, InvalidValue, Label } from "./contract.ts";

const Principal = Schema.Struct({ actorId: Schema.String });

const LabelServer = Server.plugin(Label, {
  principal: Principal,
  state: Schema.Struct({ text: Schema.String, changedBy: Schema.NullOr(Schema.String) }),
  initialize: Effect.succeed({ text: "Untitled", changedBy: null }),
  snapshot: (state) => ({ text: state.text }),
  actions: {
    rename: ({ state, payload, principal }) =>
      Effect.succeed(
        Server.commit({
          state: { ...state, text: payload, changedBy: principal.actorId },
          events: [{ text: payload }],
          result: payload,
          outbox: [],
        }),
      ),
  },
});

export const CounterServer: Server.Definition<typeof Counter.spec> = Server.make(Counter, {
  principal: Principal,
  state: Schema.Struct({ value: Schema.Int, lastActorId: Schema.NullOr(Schema.String) }),
  initialize: Effect.succeed({ value: 0, lastActorId: null }),
  snapshot: (state) => ({ value: state.value }),
  authorize: ({ principal }) =>
    principal.actorId === "blocked"
      ? Effect.fail(ProtocolError.make({ reason: "Forbidden", message: "Access denied" }))
      : Effect.void,
  actions: {
    set: ({ state, payload, principal }) =>
      payload > 100
        ? Effect.fail(InvalidValue.make({ maximum: 100 }))
        : Effect.succeed(
            Server.commit({
              state: { value: payload, lastActorId: principal.actorId },
              events: [{ value: payload }],
              result: { previous: state.value, current: payload },
              outbox: [{ value: payload }],
            }),
          ),
  },
  plugins: [LabelServer],
});
