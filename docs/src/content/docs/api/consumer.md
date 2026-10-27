---
title: Server actions
---

Using the shared `Counter` from [getting started](../guide/getting-started.md),
you can keep server state that clients never receive. This example records the
last actor to call `set`:

## Implement the server

```ts title="counter-server.ts"
import { Server } from "@yielded/sync/server";
import { Effect } from "effect";

import { authorize, Principal } from "./auth";
import { Counter } from "./counter";
import { migrate, State } from "./counter-state";

export const CounterServer = Server.make(Counter, {
  principal: Principal,
  state: State,
  stateVersion: 2,
  migrate,
  initialize: Effect.succeed({ value: 0, audit: { lastActorId: null } }),
  snapshot: (state) => ({ value: state.value }),
  authorize,
  actions: {
    set: ({ state, payload, principal }) =>
      Effect.succeed({
        state: { value: payload.value, audit: { lastActorId: principal.actorId } },
        events: [{ value: payload.value }],
        result: { previous: state.value, current: payload.value },
      }),
  },
});
```

The auth module supplies `Principal`, a Schema with an `actorId` field, and
`authorize`, which checks the principal, address, state, and operation. Deny
access with `ProtocolError`; an actor ID from a client is not proof of identity.

Only the server sees `audit`: `snapshot` selects the public view. Each
event updates connected clients, while the result goes to the caller. Sync
commits state, events, and the result atomically, and every state change must
emit an event during an action. New sources use `initialize` directly at revision
`2`; existing sources use the migration below. The same options work on
`Server.plugin`.

## Migrate across several versions

Keep each supported old shape and apply the remaining steps in order:

```ts title="counter-state.ts"
import { Effect, Schema } from "effect";

const StateV0 = Schema.Struct({ value: Schema.Finite });
const StateV1 = Schema.Struct({
  value: Schema.Finite,
  lastActorId: Schema.NullOr(Schema.String),
});
export const State = Schema.Struct({
  value: Schema.Finite,
  audit: Schema.Struct({ lastActorId: Schema.NullOr(Schema.String) }),
});

export const migrate = Effect.fn("Counter.migrate")(function* ({
  state,
  fromVersion,
}: {
  state: Schema.Json;
  fromVersion: number;
}) {
  let next = state;

  if (fromVersion < 1) {
    const old = yield* Schema.decodeUnknownEffect(StateV0)(next);
    next = { ...old, lastActorId: null };
  }
  if (fromVersion < 2) {
    const old = yield* Schema.decodeUnknownEffect(StateV1)(next);
    next = { value: old.value, audit: { lastActorId: old.lastActorId } };
  }

  return yield* Schema.decodeUnknownEffect(State)(next);
});
```

| Stored revision                  | Work performed                                             |
| -------------------------------- | ---------------------------------------------------------- |
| `0` (including unversioned data) | Add `lastActorId`, then move it into `audit`: `0 → 1 → 2`. |
| `1`                              | Move the existing actor into `audit`: `1 → 2`.             |
| `2`                              | No migration.                                              |

Sync calls `migrate` once with the stored revision. Both `if` blocks use that
original `fromVersion`, so a source that skipped revision `1` still runs both
steps. The returned state and revision `2` save atomically; a failed step leaves
the stored state unchanged. Keep old steps while any stored source may need them.

The public snapshot remains `{ value }` throughout. Keep public codecs unchanged
and migration work bounded within the source turn; see [upgrade constraints](../RELEASE.md).

## Compose actions and outbox work

Put external side effects
in an [outbox](https://github.com/yielded-dev/sync/blob/main/packages/platform-cloudflare/README.md#durable-data)
and make sure the destination deduplicates deliveries.

Compose reusable operations into one commit when their state shares one owner:

```ts
import { Server } from "@yielded/sync/server";

import { resolveComments } from "./comments";
import { removeSection } from "./documents";
import type { WorkspaceState } from "./state";

export const removeSectionPlan = (state: WorkspaceState, sectionId: string) => {
  const document = removeSection(state.document, sectionId);
  const comments = resolveComments(state.comments, sectionId);

  return Server.commit({
    state: { ...state, document: document.state, comments: comments.state },
    events: [...document.events, ...comments.events],
    result: undefined,
  });
};
```

Call this from a root action. The application's `WorkspaceState` owns both fields;
its source event schema and client reducer handle both event types. Separately
registered plugin slots cannot be updated this way.

For typed outbox work, share an obligation definition between the action and
its delivery callback:

```ts title="counter-outbox.ts"
import { Outbox } from "@yielded/sync/server";
import { Effect, Schema } from "effect";

import { projectCounter } from "./projection";

export const CounterChanged = Outbox.make(
  "CounterChanged",
  Schema.Struct({ value: Schema.Finite }),
);

export const deliver = Outbox.handlers([CounterChanged], {
  CounterChanged: ({ id, payload }) =>
    Effect.as(projectCounter(id, payload.value), { _tag: "Delivered" }),
});
```

In an action's `Effect.gen`, include
`outbox: [yield* Outbox.encode(CounterChanged, { value: payload.value })]` in the
commit. Configure the host with `outbox: { deliver }`; portable hosts also need
a durable `schedule` callback. The application's `projectCounter` returns an
Effect, deduplicates by `id`, and reports retryable failures as `ProtocolError`.
Its service requirements flow into the host. Return a `DeliveryDisposition` from
the handler to choose a retry time or classify a permanent failure.

Unknown obligation names and invalid stored payloads fail delivery and remain
retryable. Keep definitions compatible with queued records across deployments.
Applications can explicitly return `PermanentFailure` to retain a failed record
without retrying it. Only `Delivered` removes it.

Use the [Cloudflare setup](../guide/getting-started.md#connect-a-server) to host
this definition. For Alchemy or a portable `Server.host`, see
[packages and hosts](../reference/packages.mdx).
