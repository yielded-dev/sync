# Alchemy Cloudflare counter

The same counter, label plugin, private state, and exact action results as the
[effect-cf example](../cloudflare/README.md), hosted with current Alchemy v2.
The application declares `Cloudflare.Worker` and `Cloudflare.DurableObject` itself.
Sync supplies the object handlers and `AlchemyCf.prepareRequest` for attaching
authenticated metadata before the application calls its typed stub's `fetch`.

```sh
vp install
vp run build
vp run sync-alchemy-cloudflare-example#typecheck
vp run sync-alchemy-cloudflare-example#dev
```

`alchemy.run.ts` uses local stack state. `alchemy dev` builds and runs the native
Worker locally. The Alchemy CLI may require a configured Cloudflare profile even
for local startup; refresh its OAuth scopes if the CLI reports `NeedsReauth`.
Use the printed URL with `/sync/counters/:id` and either
`Authorization: Bearer alice-local` or `Bearer bob-local`. The shared demo auth
function and outbox acknowledgement are application placeholders, as in the
effect-cf example.

Use the counter's native Effect RPC contract with `RpcSerialization.layerJson`.
HTTP serves snapshot, action, and result lookup; WebSockets also support
subscriptions and ephemeral messages. The contract, server, and demo auth code
are imported from `examples/cloudflare/src` so both hosts run the same behavior.

The repository pins the Effect family in the root catalog, Alchemy to `2.0.0-beta.79`, and
the CLI's Node/Bun platform peers to compatible releases. Catalog-backed overrides
also align transitive Effect packages; Alchemy's broad RC ranges otherwise select
releases with different module paths.
