# List and board consumer

This example owns its card and board models. It uses the public sync contracts,
server, client, and Cloudflare adapter entry points. Cards are the source slot;
the reusable board slot owns its title. Presence is an ephemeral editing message.

```sh
vp run sync-list-board-example#dev
vp run sync-list-board-example#typecheck
vp run sync-list-board-example#test
vp run sync-list-board-example#build
```

The local endpoint is `/sync/boards/:id`. The demonstration accepts
`Authorization: Bearer alice-local` and `Bearer bob-local`. Replace that function
with the application's auth provider before deployment. The Worker stores state,
events, and exact receipts in its SQLite Durable Object. The test opens two
headless clients through native Effect RPC, verifies a lost response and exact
retry after object eviction, a typed rejection, presence without cursor progress,
and catch-up after a subscription disconnect.

`openBrowserSession` in `src/client.ts` assembles the same client definition with
IndexedDB persistence. Its caller supplies an authenticated `RpcClient.Protocol`
and retains the Effect scope for the signed-in actor. The UI can bind
`SourceAtom.make(session)` to that runtime; it should not create a second replica
store. The browser and Expo adapter suites prove their physical storage behavior
at their own real boundaries.

This example uses no projection destination. Applications that need one own the
projection records, idempotent destination, and outbox delivery Effect. The
[slide-deck pilot](../../docs/slide-deck-pilot.md) maps these same public entry
points to a product source with an external projection.
