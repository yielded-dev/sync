# @yielded/sync-platform-alchemy-cf

Sync handlers for Alchemy v2 Cloudflare Workers and SQLite Durable Objects.

```ts
import { AlchemyCf } from "@yielded/sync-platform-alchemy-cf";
import * as Cloudflare from "alchemy/Cloudflare";

import { CounterServer } from "./server";

export class CounterObject extends Cloudflare.DurableObject<CounterObject>()(
  "CounterObject",
  AlchemyCf.make(CounterServer, {
    storageNamespace: "counter-v1",
    replay: { maxEvents: 512, maxBatchBytes: 600_000 },
    sockets: { maxConnections: 100, bufferSize: 256 },
  }),
) {}
```

`make` supplies the construction and runtime Effects to Alchemy's native
constructor. Provide infrastructure dependencies around the construction Effect;
use `Server.provide` for services acquired within each source operation.
Initialization and alarm failures surface as defects because those native hooks
have no typed error channel.

Construct the Worker with Alchemy's API. After routing and authentication, call
`AlchemyCf.prepareRequest` with `{ request, address, identity }`, then pass the
returned Effect HTTP request to the selected stub's `fetch`. The identity contains
`actorId`, a Schema JSON `principal`, and `expiresAtMillis`. Request preparation
replaces internal identity headers; only authenticated routes should expose the
object binding.

See the [runnable Alchemy counter](../../examples/alchemy-cloudflare/README.md)
and shared [Cloudflare storage and protocol](../platform-cloudflare/README.md).
Sync owns the object's alarm; other alarm jobs need an application scheduler.
Preserve object identity when changing frameworks, or explicitly migrate the
Cloudflare namespace to retain its durable data.
