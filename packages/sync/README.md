# @yielded/sync

Effect-native realtime contracts, authoritative execution, and scoped clients.

| Entry point            | Public API                                                                                |
| ---------------------- | ----------------------------------------------------------------------------------------- |
| `@yielded/sync`        | `Source`, `Action`, `Plugin`, `SourceCatalog`, Schema envelopes and derived RPC contracts |
| `@yielded/sync/server` | `Server`, `SourceStorage`, `ServerCrypto`                                                 |
| `@yielded/sync/client` | `Client`, `ReplicaPersistence`, `Persistence`, typed operational errors                   |
| `@yielded/sync/atom`   | `SourceAtom` bindings over the headless client                                            |

Root contracts are portable between server and client. Server implementations and
platform adapters stay separate. Applications own domain schemas, authentication,
authorization, projection destinations, and UI.

`Server.commit` builds a plan; the runtime atomically commits state, events, exact
outcomes, and outbox obligations. Changed state must emit an event. Declared
rejections are durable outcomes; operational failures are not. Retrying an
identical command returns its original result.

Clients require an actor, transport, explicit persistence choice, and a live scope.
Close the session before switching actors. Unknown outcomes retain their command
ids for `retry(id)`; never replace them with a new id to bypass recovery. Atom
bindings share that session and release their leases on registry disposal.

Start with [getting started](../../docs/src/content/docs/guide/getting-started.md),
the [client guide](../../docs/src/content/docs/client.md), or the
[runnable examples](../../examples/README.md). Contributor contracts are in
[API boundaries](../../docs/PUBLIC_API.md).
