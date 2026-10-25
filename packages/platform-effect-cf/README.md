# @yielded/sync-platform-effect-cf

Sync handlers for native effect-cf Workers and Durable Objects. The application
supplies `SourceStorage`; this example selects SQLite.

```ts
import { EffectCf } from "@yielded/sync-platform-effect-cf";
import { Layer } from "effect";
import { DurableObject } from "effect-cf";

import { CounterServer } from "./server";

const sync = EffectCf.make(CounterServer, {
  replay: { maxEvents: 512, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 100, bufferSize: 256 },
});

export const CounterObject = DurableObject.make(
  sync.layer.pipe(Layer.provide(EffectCf.sqlite({ namespace: "counter-v1", replayWindow: 512 }))),
  sync.handlers,
);
```

The application constructs its Worker with effect-cf's API, chooses routes and
typed namespace bindings, authenticates callers, and calls `EffectCf.prepareRequest`
with `{ request, address, identity }`. `identity` contains `actorId`, a Schema JSON
`principal`, and `expiresAtMillis`. Pass the returned native request to the selected
stub using the namespace client's `fetch` method. Request preparation replaces
internal address/session headers and generates a connection
identity. Only authenticated routes should expose the Durable Object binding.
`EffectCf.errorResponse` maps a `ProtocolError` to its HTTP response.

Storage, source, and delivery requirements remain visible in `sync.layer`. Supply
instance services through Layer composition. Use
`Server.provide(CounterServer, ServicesLive)` before `EffectCf.make` for services
acquired and released within each source operation. Subscriptions reconnect and
replay after an activation is lost; optional presence uses connection-scoped messages.

To enable durable delivery, set `maxPendingOutbox` on `EffectCf.sqlite` and supply
`outbox: { batchSize, retryMillis, deliver }` to `EffectCf.make`. Drain pending work
before disabling delivery. Sync uses the object's alarm for delivery; other alarm
jobs need an application scheduler.

See the [runnable counter](../../examples/cloudflare/README.md),
[React board](../../examples/list-board/README.md), and shared
[Cloudflare storage and protocol](../platform-cloudflare/README.md). Keep class
names, named-object keys, and storage namespaces stable to retain authority,
receipts, and pending outbox records.
