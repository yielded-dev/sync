# @yielded/sync

Define shared state and typed actions with Effect. Your server handles actions
and publishes events; connected clients keep a local view up to date and can
show edits optimistically.

Start with the [counter guide](../../docs/src/content/docs/guide/getting-started.md)
or see [how Sync works](../../docs/src/content/docs/guide/concepts.mdx).

| Entry point            | Public API                                                               |
| ---------------------- | ------------------------------------------------------------------------ |
| `@yielded/sync`        | `Source`, `Action`, `Plugin`, Schema envelopes and derived RPC contracts |
| `@yielded/sync/server` | `Server`, `ServerRpc`, `SourceStorage`, `ServerCrypto`                   |
| `@yielded/sync/client` | `Client`, `ReplicaPersistence` ports, typed operational errors           |
| `@yielded/sync/atom`   | `SourceAtom` bindings over the headless client                           |

Public modules also support direct imports:

```ts
import * as Client from "@yielded/sync/client/Client";
import * as Source from "@yielded/sync/Source";
```

The root contract is shared by server and client. Your application supplies the
domain rules, authentication, storage, and UI. The
[platform packages](../../docs/src/content/docs/reference/packages.mdx) provide
Cloudflare hosting and browser persistence, or you can provide your own.

Server action handlers return `{ state, events, result, outbox? }`. Sync commits
these together; a state change must emit an event. Clients need an actor,
transport, persistence choice, and a live scope. Close the session before
switching actors. If an action's outcome is unknown, retry its original command
ID rather than submitting a new one. See the
[client guide](../../docs/src/content/docs/client.mdx) for recovery and persistence.
