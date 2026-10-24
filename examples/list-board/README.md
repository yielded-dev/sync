# Shared board

A React and Effect Atom frontend over Sync's public client. An effect-cf Worker
stores cards and board titles in SQLite; the browser uses IndexedDB for snapshots
and pending commands.

## Run it

From the repository root, after `vp install` and `vp run build`:

```sh
vp run sync-list-board-example#dev
```

Open <http://127.0.0.1:5173/> or the address printed by Vite+. The command serves
the frontend and Worker locally without a Cloudflare account.

1. Add cards with **Add card** or Enter.
2. Choose **Open as Bob** to open the same board as another demo identity.
3. Change a card to **In progress** or **Done**, or rename the board with its pencil
   button. Watch the other tab update.
4. Focus a card's menu to show your presence, then reload a tab to restore the board.

Use `/?board=another-board&actor=alice` for another board. Switching identity
navigates to a new page, closes the old session, and opens the selected actor's
persistence partition. It does not wipe pending commands.

While disconnected, cached content remains visible and editing waits for authority.
An uncertain command can be retried using its saved id. The client reconnects and
recovers from its cursor after a lost Durable Object activation.

## Read the example

| File                             | Responsibility                                                      |
| -------------------------------- | ------------------------------------------------------------------- |
| [contract.ts](src/contract.ts)   | Cards, actions, presence messages, and board-title plugin           |
| [worker.ts](src/worker.ts)       | Authorization, domain actions, routing, and Durable Object          |
| [client.ts](src/client.ts)       | Pure event and optimistic reducers                                  |
| [atoms.ts](src/browser/atoms.ts) | Actor-scoped session, persistence, mutation workflows, and presence |
| [App.tsx](src/browser/App.tsx)   | React rendering and dispatch                                        |
| [main.tsx](src/browser/main.tsx) | The application's `RegistryProvider`                                |

`Atom.runtime` owns the session scope. `SourceAtom` leases share its headless
client; registry disposal releases them. `CurrentActor` is provided once to the
client, transport, and IndexedDB Layers. Presence uses ephemeral messages and
stays outside the durable replica.

## Demo boundaries

The endpoint is `/sync/boards/:id`. Browser WebSockets select a public demo
identity through `?actor=alice` or `?actor=bob`. Headless clients can use
`Authorization: Bearer alice-local` or `Authorization: Bearer bob-local`.
An invalid header does not fall back to the URL identity. Replace this selection
with real authentication before deployment.

The example has no projection destination. Applications needing one own the
projection records, idempotent destination, and outbox delivery Effect.

Run `vp run sync-list-board-example#test` for the Worker consumer check and
`vp run sync-list-board-example#build` to build the UI and Worker followed by a
deployment dry run. The browser steps above exercise the React UI.
