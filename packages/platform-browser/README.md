# Browser source bindings

Bind a Sync client to browser WebSockets and Effect Atom. The adapter provides
browser crypto and the current actor; you choose persistence and credentials.

```sh
bun add @yielded/sync @yielded/sync-platform-browser @yielded/sync-local-indexeddb effect
```

```ts
import { IndexedDb } from "@yielded/sync-local-indexeddb";
import { BrowserSource } from "@yielded/sync-platform-browser";

import { BoardClient } from "./client";
import { BoardId } from "./contract";
import { actorId } from "./session";

export const board = BrowserSource.bind(BoardClient, {
  id: BoardId.make("shared"),
  actorId,
  persistence: IndexedDb.layer({ namespace: "boards" }),
  actions: { moveCard: { concurrent: true } },
});
```

Read `board.replica` and dispatch `board.action.moveCard`. To compose actions,
use `board.fn(function* () { ... })`: inside the generator, await
`board.action.moveCard(payload)` and use `Atom.get` / `Atom.set` for local state.
The runtime provides `Crypto.Crypto`, `Client.CurrentActor`, and `Client.Transport`.
Declared plugins have typed bindings under `board.plugins`, and
`board.presence(payloadAtom)` can share the source connection. See the
[runnable board](../../examples/list-board/README.md).

| Option                                  | Behavior                                                                                  |
| --------------------------------------- | ----------------------------------------------------------------------------------------- |
| `id`                                    | Required; uses the source contract's ID type, including brands                            |
| `actorId`                               | Required; scopes the local session and persistence, without authenticating the connection |
| `persistence`                           | Required; a `ReplicaPersistence` Layer or `"volatile"`                                    |
| `origin`                                | Defaults to the current page's origin; the contract supplies the path and ID encoding     |
| `query`                                 | Optional application connection parameters; no actor parameter is added automatically     |
| `actions`, `plugins`, `limits`, `retry` | The corresponding `SourceAtom.bind` options and defaults                                  |

A persistence Layer selects persistent operation and receives the browser services,
including the current actor. If storage setup fails, the error remains a typed
failure; the adapter does not fall back to volatile operation. The adapter has no
persistence driver dependency. Your application chooses storage namespaces and
handles authentication, for example with same-origin cookies or explicit
connection parameters.

Define a binding once or use `Atom.family` for multiple IDs. Browser resources are
acquired lazily within each Atom registry. Dispose the old registry before changing
actors; stored data remains intact. For custom transport or an existing application
runtime, use [`SourceAtom.bind`](https://yielded.dev/sync/client/#atom-bindings).
