# @yielded/sync-platform-effect-cf

Create an effect-cf Durable Object directly from a Sync server definition.
The adapter supplies native SQLite storage, crypto, transport, and lifecycle
handling around the portable `Server.host`.

```ts
import { EffectCf } from "@yielded/sync-platform-effect-cf";

import { CounterServer } from "./server";

export const CounterObject = EffectCf.make(CounterServer, {
  storage: { namespace: "counter-v1" },
});

const Counters = CounterObject.binding("COUNTERS");
```

Register `CounterObject` as a SQLite Durable Object in Wrangler. Supply
`Counters.layer` to `Worker.makeFetchHandler`; its `getByName` and `fetch` methods
use that binding without a handwritten service class.

The application owns routes and authentication. Call `EffectCf.prepareRequest`
with `{ request, address, identity }`, then forward the result with
`Counters.fetch(stub, prepared)`. The identity contains `actorId`, a Schema JSON
`principal`, and `expiresAtMillis`. Request preparation replaces internal identity
headers; only authenticated routes should expose the binding.
`EffectCf.errorResponse` maps a `ProtocolError` to its HTTP response.

To replace SQLite, pass a `SourceStorage` Layer as `storage`; the default store
will not be opened. `crypto` accepts a replacement `ServerCrypto` Layer.
`EffectCf.layer(options)` exposes the same platform services for custom
composition within an effect-cf object. Use `Server.provide` for services acquired
and released within each source operation.

Outbox delivery is optional. Supply `outbox: { batchSize, retryMillis, deliver }`
to enable it with bounded SQLite capacity and native alarm scheduling. Drain
pending work before disabling delivery. Other alarm jobs need an application
scheduler. See the shared [options and defaults](../platform-cloudflare/README.md).

See the [runnable counter](../../examples/cloudflare/README.md) and
[React board](../../examples/list-board/README.md). Keep class names, named-object
keys, and storage namespaces stable to retain authority, receipts, and pending
outbox records.
