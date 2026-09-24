import { type Client, type ClientError } from "@yielded/sync/client";
import { type Effect } from "effect";

import { type CardRejected, Cards } from "../src/contract.ts";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Assert<T extends true> = T;

declare const lease: Client.Lease<typeof Cards.spec>;

const added = lease.execute(Cards.actions.add, { id: "one", title: "First" });
const moved = lease.execute(Cards.actions.move, { id: "one", lane: "doing" });
const renamed = lease.execute(Cards.plugins.board.actions.rename, "Planning");

export type AddResult = Assert<
  Equal<Effect.Success<typeof added>, { readonly id: string; readonly count: number }>
>;

export type MoveResult = Assert<
  Equal<
    Effect.Success<typeof moved>,
    { readonly id: string; readonly lane: "todo" | "doing" | "done" }
  >
>;

export type RenameResult = Assert<Equal<Effect.Success<typeof renamed>, string>>;
export type AddError = Assert<Equal<Effect.Error<typeof added>, CardRejected | ClientError>>;

// @ts-expect-error Moving a card requires a lane, not a title.
export const invalidMove = lease.execute(Cards.actions.move, { id: "one", title: "Wrong" });
// @ts-expect-error Adding a card requires a title, not a lane.
export const invalidAdd = lease.execute(Cards.actions.add, { id: "one", lane: "doing" });
