# Shared board

A runnable React and Effect Atom frontend over Sync's public client and native
Effect RPC. The effect-cf Worker stores cards and board titles in a SQLite Durable
Object; the browser uses IndexedDB for its snapshot cache and pending-intent journal.

## Run it

From the repository root, after `vp install` and `vp run build`:

```sh
vp run sync-list-board-example#dev
```

Open <http://127.0.0.1:5173/> (or the address printed by Vite+). This one command
serves the frontend and runs its Worker locally. No Cloudflare account is needed.

1. Add a few cards with **Add card** or Enter.
2. Use **Open as Bob** to open the same board as another demo identity.
3. Choose **In progress** or **Done** on a card and watch the other tab update.
4. Rename the board with its pencil button. Both tabs receive the new title.
5. Focus a card's menu to show the other person where you are working.
6. Reload either tab; the board and card positions remain saved.

The identity menu switches between Alice and Bob with a full page navigation,
ending the old page's session before the next one opens. Each identity has its own
persistence partition. To use a different board, open
`/?board=another-board&actor=alice`; the collaboration link preserves that board id.

The UI shows connection and pending-change states. While disconnected, a cached
board remains visible and editing is disabled until authority is restored. An
uncertain command can be retried with its saved command id. Switching identity or
closing a tab never deliberately wipes the pending journal.

## Where the behavior lives

| File                   | Responsibility                                                                                                  |
| ---------------------- | --------------------------------------------------------------------------------------------------------------- |
| `src/contract.ts`      | Card schemas, actions, ephemeral messages, and the reusable board-title plugin                                  |
| `src/worker.ts`        | Application authorization, domain actions, native Worker routing, and the Durable Object                        |
| `src/client.ts`        | Pure client reducers and optimistic updates                                                                     |
| `src/browser/atoms.ts` | Session Layer composition, actor-scoped `Atom.runtime`, `SourceAtom` bindings, mutation workflows, and presence |
| `src/browser/App.tsx`  | React rendering and dispatch through `useAtom`, `useAtomValue`, and `useAtomSet`                                |
| `src/browser/main.tsx` | The application's single `RegistryProvider`                                                                     |

`SourceAtom.make(client)` binds to the same headless runtime that owns the socket,
cursor, recovery, and optimistic replica. The Atom runtime's layer owns the Effect
scope, IndexedDB handle, and a lease for presence; active replica atoms share its
coordinator. Registry disposal releases these resources. React keeps no second
copy of the board state. Draft form values are atoms, so the mutation workflow can
clear or close the form after a successful action.

The browser composition root provides `Client.CurrentActor` once. Both IndexedDB
and the demo socket Layer read it during acquisition. `Client.make` requires that
identity, `Client.Transport`, and (in persistent mode) `ReplicaPersistence` through
context. `IndexedDb.layer({ namespace })` and `Client.layerRpcTransport(Cards)`
provide the implementations. The domain client definition imports neither adapter.
Changing the ambient identity cannot retarget an existing client or storage handle;
switching accounts builds a new session scope and retains each actor's journal.

Presence uses the existing ephemeral message contract. Peers are bounded to 32
entries, checked for expiry every five seconds, and refreshed every eight seconds
while connected. Entries expire after 25 seconds without an announcement, or are
removed when a departure arrives. They never enter the durable replica or journal.

## Demo authentication

The local RPC endpoint is `/sync/boards/:id`. Browser WebSockets select the public
demo identity with `?actor=alice` or `?actor=bob`; they cannot set an Authorization
header. Headless clients can still use `Authorization: Bearer alice-local` or
`Bearer bob-local`. An invalid supplied header does not fall back to a URL identity.
These are public demonstration identities, not production credentials. Replace
this selection in the application's Worker with its auth-provider integration
before deploying it.

## Verification

```sh
vp run sync-list-board-example#typecheck
vp run sync-list-board-example#test
vp run sync-list-board-example#build
```

The build bundles the frontend and Worker with Vite+, then uses the generated
Wrangler configuration for a deployment dry run. It does not deploy anything.
The existing Worker test opens two headless clients through native Effect RPC and
verifies a lost response and exact retry after object eviction and RPC reconnection,
a typed rejection, presence without cursor progress, and subscription catch-up.
The browser steps above exercise the real React consumer over those same contracts.

Native RPC streams reset when an activation is lost. The headless client reconnects
and recovers from its cursor. A request racing that reset can fail with
`Unavailable`; the pending intent retains its command id for an exact retry.

This example uses no projection destination. Applications that need one own the
projection records, idempotent destination, and outbox delivery Effect. The
[slide-deck pilot](../../docs/slide-deck-pilot.md) maps these same public entry
points to a product source with an external projection.
