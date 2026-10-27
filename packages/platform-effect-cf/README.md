# @yielded/sync-platform-effect-cf

Define a Sync Durable Object with the shared Cloudflare package, then select
effect-cf at application composition. The provider supplies native SQLite
storage, crypto, transport, and lifecycle handling around `Server.host`.

```ts
import { DurableObject } from "@yielded/sync-platform-cloudflare";
import { EffectCfServices } from "@yielded/sync-platform-effect-cf";
import { Worker } from "effect-cf";

import { authenticate } from "./auth";
import { CounterServer } from "./server";

export const CounterObject = DurableObject.make(CounterServer, {
  storage: { namespace: "counter-v1" },
}).provide(EffectCfServices);

const Counters = CounterObject.binding("COUNTERS");
const fetch = Counters.handler({ authenticate });

export default Worker.makeFetchHandler(Counters.layer, { fetch });
```

`provide(EffectCfServices)` returns the native effect-cf class. Register
`CounterObject` as a SQLite Durable Object in Wrangler. Use
`typeof CounterObject.Type` for its instance type. The handler uses
the shared source's `path` and `id` schema. Its default URL is `/sync/<kind>/<id>`;
the default object key is `<kind>:<id>`. Supply `objectName: (id) => string` to retain
an existing key scheme. Changing a URL does not change the object key.

`authenticate(request, address)` returns an Effect with `actorId`, a Schema JSON
`principal`, and `expiresAtMillis`, or fails with `ProtocolError`. Its address has
the source's branded ID type. The handler validates the route, authenticates,
replaces internal identity headers, and forwards HTTP or WebSocket requests.
Unmatched paths return 404. Protocol failures become HTTP error responses.

For custom routing, import `Cloudflare` from `@yielded/sync-platform-cloudflare`
and use `Cloudflare.prepareRequest` with `{ request, address, identity }`, then
`Counters.getByName` and `Counters.fetch`. Only authenticated routes should expose
the binding.

To replace SQLite, pass a `SourceStorage` Layer as `storage`; the default store
will not be opened. `crypto` accepts a replacement `ServerCrypto` Layer.
The package's `layer(options)` export exposes the same platform services for custom
composition within an effect-cf object. Use `Server.provide` for services acquired
and released within each source operation.

Outbox delivery is optional. Supply `outbox: { deliver }`
to enable it with bounded SQLite capacity and native alarm scheduling. Drain
pending work before disabling delivery. Other alarm jobs need an application
scheduler. See the shared [options and defaults](../platform-cloudflare/README.md).

See the [React board](../../examples/list-board/README.md). Keep class names, named-object
keys, and storage namespaces stable to retain authority, receipts, and pending
outbox records.
