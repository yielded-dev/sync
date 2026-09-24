import { Action, Plugin, Source } from "@yielded/sync";
import { Schema } from "effect";

export const Lane = Schema.Literals(["todo", "doing", "done"]);

export const Card = Schema.Struct({
  id: Schema.NonEmptyString,
  title: Schema.NonEmptyString,
  lane: Lane,
});

export class CardRejected extends Schema.TaggedError<CardRejected>()("CardRejected", {
  reason: Schema.Literals(["Duplicate", "Missing"]),
}) {}

export const Board = Plugin.make("board", {
  snapshot: Schema.Struct({ title: Schema.String }),
  event: Schema.Struct({ title: Schema.String }),
  message: Schema.Never,
  actions: [
    Action.make("rename", {
      payload: Schema.String,
      success: Schema.String,
      error: Schema.Never,
    }),
  ],
});

export const Cards = Source.make({
  kind: "cards",
  schemaVersion: 1,
  snapshot: Schema.Struct({ cards: Schema.Array(Card) }),
  event: Schema.Union([
    Schema.TaggedStruct("Added", { card: Card }),
    Schema.TaggedStruct("Moved", { id: Schema.String, lane: Lane }),
  ]),
  message: Schema.Struct({ editingCardId: Schema.NullOr(Schema.String) }),
  actions: [
    Action.make("add", {
      payload: Schema.Struct({ id: Schema.NonEmptyString, title: Schema.NonEmptyString }),
      success: Schema.Struct({ id: Schema.String, count: Schema.Int }),
      error: CardRejected,
    }),
    Action.make("move", {
      payload: Schema.Struct({ id: Schema.String, lane: Lane }),
      success: Schema.Struct({ id: Schema.String, lane: Lane }),
      error: CardRejected,
    }),
  ],
  plugins: [Board],
});
