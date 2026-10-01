# @yielded/sync-platform-effect-cf

Sync handlers for native effect-cf Workers and SQLite Durable Objects.

```ts
import { EffectCf } from "@yielded/sync-platform-effect-cf";
import { DurableObject } from "effect-cf";
import { CounterServer } from "./server";

const sync = EffectCf.make(CounterServer, {
  storageNamespace: "counter-v1",
  replay: { maxEvents: 512, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 100, bufferSize: 256 },
});

export const CounterObject = DurableObject.make(sync.layer, sync.handlers);
```

The application constructs its Worker with effect-cf's API, chooses routes and
typed namespace bindings, authenticates callers, and calls `EffectCf.prepareRequest`
with `{ request, address, identity }`. `identity` contains `actorId`, a Schema JSON
`principal`, and `expiresAtMillis`. Pass the returned native request to the selected
stub using the namespace client's `fetch` method. Request preparation replaces
internal address/session headers and generates a connection
identity. Only authenticated routes should expose the Durable Object binding.
`EffectCf.errorResponse` maps a `ProtocolError` to its HTTP response.

Source and delivery requirements remain visible in `sync.layer`. Supply instance
services with native Layer composition. For services acquired and released within
each source operation, use `Server.provide(CounterServer, ServicesLive)` before
`EffectCf.make`. Sync's Layer owns the instance's SQLite port, source gate, and
`RpcServer`; effect-cf's native `DurableObjectRpcWebSocket` owns the socket transport.
Long-lived subscriptions use the instance scope and recover through cursor replay
when an activation is lost. The source's outbox uses the object's alarm;
compose other alarm jobs through an explicit application scheduler.

See the [runnable counter](../../examples/cloudflare/README.md) and
[list and board consumer](../../examples/list-board/README.md). Both frameworks
share the [Cloudflare storage and protocol](../platform-cloudflare/README.md).
When replacing the old `Cloudflare.durableObject` / `Cloudflare.worker` assembly,
preserve class names, named-object keys, and storage namespaces to retain authority,
receipts, and pending outbox records.
