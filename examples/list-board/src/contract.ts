import { Action, Source } from "@yielded/sync";
import { Schema } from "effect";

export const Lane = Schema.Literals(["todo", "doing", "done"]);
export const BoardId = Schema.NonEmptyString.pipe(Schema.brand("BoardId"));
export type BoardId = typeof BoardId.Type;

export const Card = Schema.Struct({
  id: Schema.NonEmptyString,
  title: Schema.NonEmptyString,
  lane: Lane,
});

export class CardRejected extends Schema.TaggedError<CardRejected>()("CardRejected", {
  reason: Schema.Literals(["Duplicate", "Missing"]),
}) {}

export const Board = Source.make({
  kind: "boards",
  id: BoardId,
  schemaVersion: 1,
  snapshot: Schema.Struct({ title: Schema.String, cards: Schema.Array(Card) }),
  event: Schema.Union([
    Schema.TaggedStruct("Added", { card: Card }),
    Schema.TaggedStruct("Moved", { id: Schema.String, lane: Lane }),
    Schema.TaggedStruct("Renamed", { title: Schema.String }),
  ]),
  message: Schema.Struct({ editingCardId: Schema.NullOr(Schema.String) }),
  actions: [
    Action.make("addCard", {
      payload: Schema.Struct({ id: Schema.NonEmptyString, title: Schema.NonEmptyString }),
      success: Schema.Struct({ id: Schema.String, count: Schema.Int }),
      error: CardRejected,
    }),
    Action.make("moveCard", {
      payload: Schema.Struct({ id: Schema.String, lane: Lane }),
      success: Schema.Struct({ id: Schema.String, lane: Lane }),
      error: CardRejected,
    }),
    Action.make("rename", {
      payload: Schema.String,
      success: Schema.String,
      error: Schema.Never,
    }),
  ],
});
