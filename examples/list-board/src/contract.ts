import { Action, Source } from "@yielded/sync";
import { Effect, Schema } from "effect";

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

export class RenameRejected extends Schema.TaggedError<RenameRejected>()("RenameRejected", {
  currentRevision: Schema.Int,
}) {}

const Revision = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

export const Board = Source.make({
  kind: "boards",
  id: BoardId,
  schemaVersion: 1,
  snapshot: Schema.Struct({
    title: Schema.String,
    titleRevision: Revision.pipe(Schema.withDecodingDefaultKey(Effect.succeed(0))),
    cards: Schema.Array(Card),
  }),
  event: Schema.Union([
    Schema.TaggedStruct("Added", { card: Card }),
    Schema.TaggedStruct("Moved", { id: Schema.String, lane: Lane }),
    Schema.TaggedStruct("Renamed", {
      title: Schema.String,
      titleRevision: Revision.pipe(Schema.withDecodingDefaultKey(Effect.succeed(0))),
    }),
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
      // Retain legacy command bytes for exact receipt lookup; strings target revision zero.
      payload: Schema.Union([
        Schema.String,
        Schema.Struct({ title: Schema.String, expectedRevision: Revision }),
      ]),
      success: Schema.String,
      error: RenameRejected,
    }),
  ],
});
