# @yielded/sync

Define shared contracts, run an authoritative server, and connect scoped clients
with Effect.

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

Share root contracts between server and client. Server implementations and
platform adapters have separate entry points. Your application supplies domain
schemas, authentication, authorization, projection destinations, and the UI.

`Server.host(definition)` coordinates ordered operations, subscriptions,
publication, and optional outbox delivery for one storage authority. Provide
`SourceStorage`, `ServerCrypto`, and the definition's services in its scope.
`ServerRpc.handlers(host)` connects it to Effect RPC; your application chooses the
transport and scheduling. See the [server guide](../../docs/src/content/docs/api/consumer.md)
for the host and its platform integrations.

Action handlers return `{ state, events, result, outbox? }`. The runtime commits
state, events, exact outcomes, and outbox obligations atomically. A state change
must emit an event. Declared rejections are durable outcomes; operational failures
are not. Retrying an identical command returns its original result.

Core defines the persistence contracts; you supply server storage and client
journals. For browsers, `@yielded/sync-local-indexeddb` provides storage.
`@yielded/sync-persistence` provides shared record handling for custom drivers.
Sources may omit messages and plugins, and client definitions may omit optimistic
reducers.

Clients require an actor, transport, explicit persistence choice, and a live scope.
Close the session before switching actors. Unknown outcomes retain their command
ids for `retry(id)`; never replace them with a new id to bypass recovery. Atom
bindings share that session and release their leases on registry disposal.

Start with [getting started](../../docs/src/content/docs/guide/getting-started.md),
the [client guide](../../docs/src/content/docs/client.mdx), or the
[React board](../../examples/list-board/README.md). Contributor contracts are in
[API boundaries](../../docs/PUBLIC_API.md).
