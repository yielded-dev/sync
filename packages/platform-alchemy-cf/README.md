# @yielded/sync-platform-alchemy-cf

Optional Alchemy Cloudflare Worker and Durable Object integration around
`Server.host`. Core owns synchronization; this adapter supplies native transport,
lifecycle, and alarm handling. The application supplies `SourceStorage`; this
example selects SQLite.

```ts
import { AlchemyCf } from "@yielded/sync-platform-alchemy-cf";
import * as Cloudflare from "alchemy/Cloudflare";
import { Effect, Layer } from "effect";

import { CounterServer } from "./server";

export class CounterObject extends Cloudflare.DurableObject<CounterObject>()(
  "CounterObject",
  AlchemyCf.make(CounterServer, {
    replay: { maxEvents: 512, maxBatchBytes: 600_000 },
    sockets: { maxConnections: 100, bufferSize: 256 },
  }).pipe(
    Effect.map((runtime) =>
      Effect.gen(function* () {
        const storage = yield* Layer.build(
          AlchemyCf.sqlite({ namespace: "counter-v1", replayWindow: 512 }),
        );

        return yield* runtime.pipe(Effect.provideContext(storage));
      }).pipe(Effect.orDie),
    ),
  ),
) {}
```

`make` supplies the construction and runtime Effects to Alchemy's native
constructor. The construction Effect can run during infrastructure planning.
Build storage inside the returned runtime Effect with `Layer.build` so its
resources share the native object's scope. Provide infrastructure dependencies
around the construction Effect; use `Server.provide` for services acquired within
each source operation. Initialization and alarm failures surface as defects
because those native hooks have no typed error channel.

Construct the Worker with Alchemy's API. After routing and authentication, call
`AlchemyCf.prepareRequest` with `{ request, address, identity }`, then pass the
returned Effect HTTP request to the selected stub's `fetch`. The identity contains
`actorId`, a Schema JSON `principal`, and `expiresAtMillis`. Request preparation
replaces internal identity headers; only authenticated routes should expose the
object binding. WebSocket subscriptions and optional presence use the same
authenticated source lifecycle.

To enable durable delivery, set `maxPendingOutbox` on `AlchemyCf.sqlite` and supply
`outbox: { batchSize, retryMillis, deliver }` to `AlchemyCf.make`. Drain pending work
before disabling delivery. Sync uses the object's alarm for delivery; other alarm
jobs need an application scheduler.

See the [runnable Alchemy counter](../../examples/alchemy-cloudflare/README.md)
and shared [Cloudflare storage and protocol](../platform-cloudflare/README.md).
Preserve object identity when changing frameworks, or explicitly migrate the
Cloudflare namespace to retain its durable data.
