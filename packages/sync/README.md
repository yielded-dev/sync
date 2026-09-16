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
semantics. The server, client, and Atom entry points are still empty; this package
does not yet execute commands, persist receipts, or synchronize replicas.
