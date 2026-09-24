# @yielded/sync

Effect-native realtime contracts and runtimes.

The root entry point exports `Action`, `Plugin`, `Source`, and `SourceCatalog`,
plus shared Schema identities, positions, exact action outcomes, and protocol errors.
Definitions reject duplicate registrations before acquiring resources. Public
snapshots and namespaced frames compose root and plugin schemas without exposing
server-private state.

Each source has a native Effect `RpcGroup`; bound actions expose their exact
command, outcome, lookup, receipt, execute RPC, and result RPC schemas. JSON codecs
preserve rich Schema values. Native RPC handlers retain their Effect requirements.

See the [external contracts example](../../examples/contracts/README.md) and
[public API contracts](../../docs/PUBLIC_API.md) for usage and remaining runtime
semantics. `@yielded/sync/server` exports `Server`, `SourceStorage`, and `ServerCrypto`.
`Server.make` and `Server.plugin` infer private state, action payloads/results, and
Effect requirements. `Server.provide` and `Server.providePlugin` acquire Layers in
the source scope. `Server.open` provides typed execution/result lookup, snapshots,
replay bootstrap, and authorization over an injected storage handle.

Every command turn authorizes against stored state, finds an exact receipt or
evaluates its handler, then atomically commits private state, events, the outcome,
and outbox obligations. Rejections are durable; operational failures and defects
never become domain rejections. `Server.commit` only constructs a plan. Changed
state must have an event. Native host operations use the runtime's Schema-encoded
`dispatch` and `lookup` transport seams.

`@yielded/sync/client` exports `Client`, `ReplicaPersistence`, and typed operational
errors. `Client.definition`/`plugin` register pure reducers; `Client.make` acquires
an actor runtime with an explicit transport and persistence choice. Scoped
`open(address)` leases share one coordinator. `ready` waits for authority, then
`execute(action, payload)` returns that action's exact result or rejection.
Ambiguous outcomes retain a command id for `retry(id)` without a replacement payload.

`Client.rpcTransport` consumes the application's `RpcClient.Protocol` Layer. It
adds no platform adapter imports. `@yielded/sync/atom` exports `SourceAtom.make`,
whose active atoms acquire the same source leases and whose passive atoms never
connect. Registry disposal releases its leases.

See the [client runtime guide](../../docs/client.md) for lifecycle, retry and
persistence behavior and the [list and board example](../../examples/list-board/README.md)
for the authority used by the two-client integration test.
