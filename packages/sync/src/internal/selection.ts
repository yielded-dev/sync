import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import type { Selection, SelectionOptions } from "../client/Client.ts";
import type { Replica } from "../client/Model.ts";
import type { View } from "../client/View.ts";
import { slotValue, type Snapshot as CompositeSnapshot } from "./replica.ts";

export const select = <Snapshot, A>(
  source: Pick<View<unknown, unknown>, "read" | "changes">,
  slot: string,
  selector: (snapshot: Snapshot) => A,
  options?: SelectionOptions<A>,
): Selection<A> => {
  const { from = "visible", equals = Object.is } = options ?? {};

  const project = (replica: Replica<unknown, unknown>): Option.Option<A> => {
    const snapshot = from === "authoritative" ? replica.authoritative?.snapshot : replica.value;

    if (snapshot === undefined) return Option.none();
    // The lease pairs a registered slot's selector with its schema-validated
    // composite snapshot. Check availability before unwrapping the slot, whose
    // own value may legitimately be undefined.
    const value = slotValue(snapshot as CompositeSnapshot, slot) as Snapshot;

    return Option.some(selector(value));
  };

  return {
    read: Effect.map(source.read, project),
    changes: source.changes.pipe(
      Stream.map(project),
      Stream.changesWith(Option.makeEquivalence(equals)),
    ),
  };
};
