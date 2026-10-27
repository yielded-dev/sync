# @yielded/sync

Effect-native realtime contracts, authoritative execution, and scoped clients.

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

Root contracts are portable between server and client. Server implementations and
platform adapters stay separate. Applications own domain schemas, authentication,
authorization, projection destinations, and UI.

`Server.host(definition)` coordinates one storage authority with ordered turns,
subscriptions, publication, and optional outbox delivery. Provide `SourceStorage`,
`ServerCrypto`, and the definition's services in its scope. `ServerRpc.handlers(host)`
connects it to Effect RPC; platform transport and scheduling remain application
choices. The [server guide](../../docs/src/content/docs/api/consumer.md) explains
the host and its platform integrations.

Action handlers return `{ state, events, result, outbox? }`; the runtime atomically
commits state, events, exact outcomes, and outbox obligations. Changed state must emit an event. Declared
rejections are durable outcomes; operational failures are not. Retrying an
identical command returns its original result.

Core owns persistence contracts; applications supply server storage and client
journals. `@yielded/sync-local-indexeddb` provides browser storage;
`@yielded/sync-persistence` supplies shared record policy for custom drivers. Sources may omit messages and
plugins; client definitions may omit optimistic reducers.

Clients require an actor, transport, explicit persistence choice, and a live scope.
Close the session before switching actors. Unknown outcomes retain their command
ids for `retry(id)`; never replace them with a new id to bypass recovery. Atom
bindings share that session and release their leases on registry disposal.

Start with [getting started](../../docs/src/content/docs/guide/getting-started.md),
the [client guide](../../docs/src/content/docs/client.mdx), or the
[React board](../../examples/list-board/README.md). Contributor contracts are in
[API boundaries](../../docs/PUBLIC_API.md).
