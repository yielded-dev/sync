import { ProtocolError } from "@yielded/sync";
import { Server } from "@yielded/sync/server";
import { Effect, Schema } from "effect";

import { ActorId, Principal } from "./auth.ts";
import { Counter, InvalidValue, Label } from "./contract.ts";

const LabelServer = Server.plugin(Label, {
  principal: Principal,
  state: Schema.Struct({ text: Schema.String, changedBy: Schema.NullOr(ActorId) }),
  initialize: Effect.succeed({ text: "Untitled", changedBy: null }),
  snapshot: (state) => ({ text: state.text }),
  actions: {
    rename: ({ state, payload, principal }) =>
      Effect.succeed(
        Server.commit({
          state: { ...state, text: payload, changedBy: principal.actorId },
          events: [{ text: payload }],
          result: payload,
        }),
      ),
  },
});

export const CounterServer = Server.make(Counter, {
  principal: Principal,
  state: Schema.Struct({ value: Schema.Int, lastActorId: Schema.NullOr(ActorId) }),
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
