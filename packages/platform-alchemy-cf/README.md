# @yielded/sync-platform-alchemy-cf

Define a Sync Durable Object with the shared Cloudflare package, then select
Alchemy at application composition. The provider supplies native SQLite storage,
crypto, transport, and lifecycle handling around `Server.host`.

Install the adapter with Alchemy's Cloudflare provider:

```sh
bun add @yielded/sync-platform-alchemy-cf @yielded/sync-platform-cloudflare @alchemy.run/cloudflare alchemy effect
```

```ts
import * as Cloudflare from "@alchemy.run/cloudflare";
import { AlchemyServices } from "@yielded/sync-platform-alchemy-cf";
import { DurableObject } from "@yielded/sync-platform-cloudflare";
import { Effect } from "effect";

import { authenticate } from "./auth";
import { CounterServer } from "./server";

export const CounterObject = DurableObject.make(CounterServer, {
  storage: { namespace: "counter-v1" },
}).provide(AlchemyServices("CounterObject"));

export default class CounterWorker extends Cloudflare.Worker<CounterWorker>()(
  "CounterWorker",
  { main: import.meta.url },
  Effect.map(CounterObject.handler({ authenticate }), (fetch) => ({ fetch })),
) {}
```

`provide(AlchemyServices("CounterObject"))` returns Alchemy's native declaration.
Use `typeof CounterObject.Type` for its declared instance type.
The name identifies the infrastructure resource; keep it stable across deployments.
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
The package's `prepareRequest` export accepts `{ request, address, identity }` and returns an
Effect HTTP request for the selected stub's `fetch`. Only authenticated routes
should expose the binding.

Alchemy owns the resource declaration and binding; Sync acquires platform services
only in the native object's runtime. Planning does not open storage. Initialization
and alarm failures surface as defects because those hooks have no typed error channel.

To replace SQLite, pass a `SourceStorage` Layer as `storage`; the default store
will not be opened. `crypto` accepts a replacement `ServerCrypto` Layer.
The package's `layer(options)` export exposes the same platform services for custom
composition inside an Alchemy object's runtime Effect. Use `Server.provide` for
services acquired and released within each source operation.

Outbox delivery is optional. Supply `outbox: { deliver }`
to enable it with bounded SQLite capacity and native alarm scheduling. Drain
pending work before disabling delivery. Other alarm jobs need an application
scheduler. See the shared [options and defaults](../platform-cloudflare/README.md).

Preserve object identity when changing frameworks, or explicitly migrate the
Cloudflare namespace to retain its durable data.
