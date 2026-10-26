# @yielded/sync-platform-alchemy-cf

Declare an Alchemy Durable Object directly from a Sync server definition.
The adapter supplies native SQLite storage, crypto, transport, and lifecycle
handling around the portable `Server.host`.

```ts
import { AlchemyCf } from "@yielded/sync-platform-alchemy-cf";

import { CounterServer } from "./server";

export const CounterObject = AlchemyCf.make("CounterObject", CounterServer, {
  storage: { namespace: "counter-v1" },
});
```

Use `yield* CounterObject` in an Alchemy Worker to obtain its typed namespace.
Alchemy owns the resource declaration and binding; Sync acquires its platform
services only in the native object's runtime. Infrastructure planning does not
open storage. Initialization and alarm failures surface as defects because
those native hooks have no typed error channel.

The application owns routes and authentication. Call `AlchemyCf.prepareRequest`
with `{ request, address, identity }`, then pass the returned Effect HTTP request
to the selected stub's `fetch`. The identity contains `actorId`, a Schema JSON
`principal`, and `expiresAtMillis`. Request preparation replaces internal identity
headers; only authenticated routes should expose the binding.

To replace SQLite, pass a `SourceStorage` Layer as `storage`; the default store
will not be opened. `crypto` accepts a replacement `ServerCrypto` Layer.
`AlchemyCf.layer(options)` exposes the same platform services for custom
composition inside an Alchemy object's runtime Effect. Use `Server.provide` for
services acquired and released within each source operation.

Outbox delivery is optional. Supply `outbox: { batchSize, retryMillis, deliver }`
to enable it with bounded SQLite capacity and native alarm scheduling. Drain
pending work before disabling delivery. Other alarm jobs need an application
scheduler. See the shared [options and defaults](../platform-cloudflare/README.md).

See the [runnable Alchemy counter](../../examples/alchemy-cloudflare/README.md).
Preserve object identity when changing frameworks, or explicitly migrate the
Cloudflare namespace to retain its durable data.
