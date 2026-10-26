# @yielded/sync-platform-alchemy-cf

Declare an Alchemy Durable Object directly from a Sync server definition.
The adapter supplies native SQLite storage, crypto, transport, and lifecycle
handling around the portable `Server.host`.

```ts
import { AlchemyCf } from "@yielded/sync-platform-alchemy-cf";
import * as Cloudflare from "alchemy/Cloudflare";
import { Effect } from "effect";

import { authenticate } from "./auth";
import { CounterServer } from "./server";

export const CounterObject = AlchemyCf.make("CounterObject", CounterServer, {
  storage: { namespace: "counter-v1" },
});

export default class CounterWorker extends Cloudflare.Worker<CounterWorker>()(
  "CounterWorker",
  { main: import.meta.url },
  Effect.map(CounterObject.handler({ authenticate }), (fetch) => ({ fetch })),
) {}
```

`CounterObject.handler` acquires the native binding and returns the Worker's fetch
Effect. It uses the shared source's `path` and `id` schema. The default URL is
`/sync/<kind>/<id>`; the default object key is `<kind>:<id>`. Supply
`objectName: (id) => string` to retain an existing key scheme independently of the URL.

`authenticate(request, address)` receives a native Request and a typed source
address. Return an Effect with `actorId`, a Schema JSON `principal`, and
`expiresAtMillis`, or fail with `ProtocolError`. The handler replaces internal
identity headers and forwards HTTP or WebSocket requests. Unmatched paths return
404; protocol failures become HTTP error responses.

For custom routing, `yield* CounterObject` obtains its typed namespace.
`AlchemyCf.prepareRequest` accepts `{ request, address, identity }` and returns an
Effect HTTP request for the selected stub's `fetch`. Only authenticated routes
should expose the binding.

Alchemy owns the resource declaration and binding; Sync acquires platform services
only in the native object's runtime. Planning does not open storage. Initialization
and alarm failures surface as defects because those hooks have no typed error channel.

To replace SQLite, pass a `SourceStorage` Layer as `storage`; the default store
will not be opened. `crypto` accepts a replacement `ServerCrypto` Layer.
`AlchemyCf.layer(options)` exposes the same platform services for custom
composition inside an Alchemy object's runtime Effect. Use `Server.provide` for
services acquired and released within each source operation.

Outbox delivery is optional. Supply `outbox: { deliver }`
to enable it with bounded SQLite capacity and native alarm scheduling. Drain
pending work before disabling delivery. Other alarm jobs need an application
scheduler. See the shared [options and defaults](../platform-cloudflare/README.md).

See the [runnable Alchemy counter](../../examples/alchemy-cloudflare/README.md).
Preserve object identity when changing frameworks, or explicitly migrate the
Cloudflare namespace to retain its durable data.
