---
"@yielded/sync": minor
"@yielded/sync-local-indexeddb": minor
"@yielded/sync-local-expo": minor
---

Require `Client.CurrentActor` and `Client.Transport` services when constructing a
client. Persistent clients also require `ReplicaPersistence`; volatile clients do
not. Remove actor, transport, and storage values from `Client.make` options. Use
`Client.layerRpcTransport(contract)` to provide transport from native Effect RPC.

IndexedDB and Expo SQLite constructors read `CurrentActor` instead of accepting
`actorId`. `memory().open` is now an Effect requiring the same service. Provide
identity once around the session's client, transport, and persistence Layers.
Identity is captured at acquisition; changing actors requires a new session scope.
Database names, actor partition keys, stored formats, and retained command evidence
are unchanged.
