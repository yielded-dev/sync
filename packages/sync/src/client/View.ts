import { Effect, Stream } from "effect";

import type * as Action from "../Action.ts";
import type * as Source from "../Source.ts";
import type { ClientError, Replica } from "./Model.ts";
import type { Actions, Lease } from "./Runtime.ts";

interface CompositeSnapshot {
  readonly source?: unknown;
  readonly plugins: Readonly<Record<string, unknown>>;
}

/** A snapshot slot with the owning lease's lifetime and source-wide status. */
export interface View<Snapshot, Rejection = never> {
  readonly read: Effect.Effect<Replica<Snapshot, Rejection>, ClientError>;
  readonly changes: Stream.Stream<Replica<Snapshot, Rejection>, ClientError>;
  readonly ready: Effect.Effect<Snapshot, ClientError>;
}

/** Project root data without changing the source's composite snapshot contract. */
export function view<S extends Source.Spec>(
  lease: Lease<S>,
): View<S["snapshot"]["Type"], Action.Error<Actions<S>>>;

/** Project a declared plugin's data from the same replica and connection. */
export function view<S extends Source.Spec, Id extends S["plugins"][number]["id"]>(
  lease: Lease<S>,
  plugin: Id,
): View<
  Extract<S["plugins"][number], { readonly id: Id }>["snapshot"]["Type"],
  Action.Error<Actions<S>>
>;

export function view<Snapshot, Rejection>(
  lease: View<Snapshot, Rejection>,
  plugin?: string,
): View<unknown, Rejection> {
  // The public overloads accept only leases with the source's composite snapshot schema.
  const select = (snapshot: unknown): unknown =>
    plugin === undefined
      ? (snapshot as CompositeSnapshot).source
      : (snapshot as CompositeSnapshot).plugins[plugin];

  const project = (state: Replica<Snapshot, Rejection>): Replica<unknown, Rejection> => ({
    ...state,
    value: state.value === undefined ? undefined : select(state.value),
    authoritative:
      state.authoritative === undefined
        ? undefined
        : { ...state.authoritative, snapshot: select(state.authoritative.snapshot) },
    provisional: state.provisional === undefined ? undefined : select(state.provisional),
  });

  return {
    read: Effect.map(lease.read, project),
    changes: Stream.map(lease.changes, project),
    ready: Effect.map(lease.ready, select),
  };
}
