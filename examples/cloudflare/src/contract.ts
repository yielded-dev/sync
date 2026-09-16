import { Action, Plugin, Source } from "@yielded/sync";
import { Schema } from "effect";

export class InvalidValue extends Schema.TaggedError<InvalidValue>()("InvalidValue", {
  maximum: Schema.Int,
}) {}

export const Label = Plugin.make("label", {
  snapshot: Schema.Struct({ text: Schema.String }),
  event: Schema.Struct({ text: Schema.String }),
  message: Schema.Never,
  actions: [
    Action.make("rename", { payload: Schema.String, success: Schema.String, error: Schema.Never }),
  ],
});

export const Counter = Source.make({
  kind: "counter",
  schemaVersion: 1,
  snapshot: Schema.Struct({ value: Schema.Int }),
  event: Schema.Struct({ value: Schema.Int }),
  message: Schema.Struct({ editing: Schema.Boolean }),
  actions: [
    Action.make("set", {
      payload: Schema.Int,
      success: Schema.Struct({ previous: Schema.Int, current: Schema.Int }),
      error: InvalidValue,
    }),
  ],
  plugins: [Label],
});
