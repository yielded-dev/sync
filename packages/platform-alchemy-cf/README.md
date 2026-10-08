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

`make` supplies Alchemy's construction Effect and per-instance runtime Effect to
the native constructor. Infrastructure dependencies resolve in the outer Effect;
SQLite initialization happens in the runtime Effect. Supply application service
dependencies around the construction Effect. Use `Server.provide` for service
Layers acquired within each source operation. The runtime scope owns `RpcServer`
and the Cloudflare socket bridge. Initialization or alarm failures propagate as
defects at Alchemy's native boundary, which has no typed error channel for those hooks.

Construct the Worker with Alchemy's own API. After routing and authentication,
call `AlchemyCf.prepareRequest` with `{ request, address, identity }` and pass the
returned native Effect HTTP request to the typed Alchemy stub's `fetch` method.
Request preparation validates the identity and overwrites internal session/address headers. WebSocket
upgrade responses retain their native socket. Only authenticated routes should
expose the Durable Object binding.

The [runnable Alchemy counter](../../examples/alchemy-cloudflare/README.md) reuses
the effect-cf example's domain contract and server. The frameworks share the same
[protocol and SQLite format](../platform-cloudflare/README.md), including receipts,
authenticated socket attachments, cursor replay after activation loss, and leased
outbox delivery. Effect RPC owns the wire protocol and stream acknowledgements.
Sync owns its object's alarm; other alarm jobs need explicit application scheduling.
A framework change alone
does not transfer a Cloudflare namespace: preserve object identity or use the
framework's native migration facilities.
