---
title: Getting started
---

Start with a shared contract: the state clients can read and the actions they can
submit. Install Sync with its Effect peer:

```sh
bun add @yielded/sync@{{SYNC_VERSION}} effect
```

## Define a source

This counter has one action, `set`, which returns the previous and new values.
Save the contract in a module shared by your server and client:

```ts title="counter.ts"
import { Action, Source } from "@yielded/sync";
import { Schema } from "effect";

export const CounterId = Schema.NonEmptyString.pipe(Schema.brand("CounterId"));

export const Counter = Source.make({
  kind: "counter",
  id: CounterId,
  schemaVersion: 1,
  snapshot: Schema.Struct({ value: Schema.Finite }),
  event: Schema.Struct({ value: Schema.Finite }),
  actions: [
    Action.make("set", {
      payload: Schema.Struct({ value: Schema.Finite }),
      success: Schema.Struct({ previous: Schema.Finite, current: Schema.Finite }),
      error: Schema.Never,
    }),
  ],
});
```

| Field      | What it describes                           |
| ---------- | ------------------------------------------- |
| `snapshot` | State every authorized client can read      |
| `event`    | A durable change the client reducer applies |
| `actions`  | Typed payloads, results, and domain errors  |
| `id`       | Validation and typing for instance IDs      |

A source definition can have many instances:

```ts
const address = Counter.address(CounterId.make("demo"));
```

The default HTTP path is `/sync/counter/demo`. Set `path` on the source to change
the base path. HTTP IDs must be nonempty and cannot be `.` or `..`.

## Add the server and client

Continue with the [counter example](../api/consumer.md) to implement the action
and client reducer, then [connect a client](../client.mdx).

Choose a [host and persistence adapter](../reference/packages.mdx) for your app.
Keep Sync packages on the same exact version; their manifests specify compatible
Effect and framework peers. For a complete frontend and Worker, run the
[React board](https://github.com/yielded-dev/sync/tree/main/examples/list-board).
