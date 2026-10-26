# Alchemy Cloudflare counter

The [counter and label plugin](../cloudflare/README.md), hosted with native Alchemy
Workers and Durable Objects. `AlchemyCf.make` declares the object and supplies
native SQLite and crypto defaults. The application owns the Worker, routes,
object identities, and authentication.

From the repository root:

```sh
vp install
vp run build
vp run sync-alchemy-cloudflare-example#dev
```

`alchemy.run.ts` uses local stack state. The development command builds and runs
the Worker locally. Alchemy may require a configured Cloudflare profile even for
local startup; refresh its OAuth scopes if it reports `NeedsReauth`.

Use the printed URL with `/sync/counters/:id` and
`Authorization: Bearer alice-local` or `Authorization: Bearer bob-local`.
The shared demo authentication and outbox acknowledgement must be replaced before
deployment or real projection delivery.

Use the counter's Effect RPC contract with `RpcSerialization.layerJson`: HTTP
for snapshot, execute, and result lookup; WebSockets for subscriptions and messages.
The [adapter README](../../packages/platform-alchemy-cf/README.md) describes service
composition and object identity requirements.
