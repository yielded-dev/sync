# effect-cf Cloudflare counter

A counter and label plugin hosted in a SQLite Durable Object. It demonstrates
private server state, exact action results, and RPC subscriptions. The
[Alchemy counter](../alchemy-cloudflare/README.md) reuses the same domain code.
`EffectCf.make` creates the Durable Object with native SQLite and crypto defaults;
its binding's `handler({ authenticate })` supplies the Worker request handler.

From the repository root, after `vp install` and `vp run build`:

```sh
vp run sync-cloudflare-example#dev
```

The shared contract declares `/sync/counters/:id` and a branded `CounterId`.
Connect using the source's Effect RPC contract and
`RpcSerialization.layerJson`. Use HTTP for snapshot, execute, and result lookup;
use WebSockets for subscriptions and ephemeral messages. The example retains four
events so replay gaps are easy to exercise.

Demo authentication accepts `Authorization: Bearer alice-local` or
`Authorization: Bearer bob-local`. Replace it with the application's auth provider
before deployment. The outbox handler only acknowledges demo records; replace it
with idempotent destination delivery before adding a projection.

| File                           | Responsibility                                           |
| ------------------------------ | -------------------------------------------------------- |
| [contract.ts](src/contract.ts) | Public counter, label, and action schemas                |
| [auth.ts](src/auth.ts)         | Branded actor/principal schemas and demo authentication  |
| [server.ts](src/server.ts)     | Private state, authorization, handlers, and outbox plans |
| [worker.ts](src/worker.ts)     | Durable Object, binding, and authenticated handler       |

Allocate a command id once and retain its payload for retry. A retried set returns
its original previous/current values, even after later edits. The
[client runtime](../../docs/src/content/docs/client.mdx) manages this automatically.

`vp run sync-cloudflare-example#build` is a deployment dry run.
`vp run @yielded/sync-platform-effect-cf#test` runs the local workerd checks.
Use `vp run sync-cloudflare-example#types` after changing Worker bindings.
