import * as Action from "@yielded/sync/Action";
import * as Plugin from "@yielded/sync/Plugin";
import * as Source from "@yielded/sync/Source";
import * as Schema from "effect/Schema";

export class InvalidValue extends Schema.TaggedError<InvalidValue>()("InvalidValue", {
  maximum: Schema.Finite,
}) {}

export const Label = Plugin.make("label", {
  snapshot: Schema.Struct({ text: Schema.String }),
  event: Schema.Struct({ text: Schema.String }),
  message: Schema.Never,
  actions: [
    Action.make("rename", {
      payload: Schema.Struct({ text: Schema.String }),
      success: Schema.Struct({ text: Schema.String }),
      error: Schema.Never,
    }),
  ],
});

export const Counter = Source.make({
  kind: "counter",
  schemaVersion: 1,
  snapshot: Schema.Struct({ value: Schema.Finite, updatedAt: Schema.DateTimeUtc }),
  event: Schema.Struct({ value: Schema.Finite, updatedAt: Schema.DateTimeUtc }),
  message: Schema.Struct({ editing: Schema.Boolean }),
  actions: [
    Action.make("set", {
      payload: Schema.Struct({ value: Schema.Finite }),
      success: Schema.Struct({ previous: Schema.Finite, current: Schema.Finite }),
      error: InvalidValue,
    }),
  ],
  plugins: [Label],
});
