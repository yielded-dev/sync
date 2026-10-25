---
"@yielded/sync": patch
"@yielded/sync-platform-cloudflare": minor
"@yielded/sync-platform-effect-cf": minor
"@yielded/sync-platform-alchemy-cf": minor
"@yielded/sync-local-indexeddb": patch
"@yielded/sync-local-expo": patch
---

Add native effect-cf and Alchemy v2 Cloudflare adapters over one shared SQLite and
RPC runtime. Applications construct their own Workers and Durable Objects and own
routing, authentication, typed bindings, and service composition. Replace
`Cloudflare.worker` and `Cloudflare.durableObject` with each framework's constructors
and the new adapter's `make` and `prepareRequest` operations. Applications call
their typed stubs directly with the prepared native request. Preserve object names,
storage namespaces, authority generations, receipts, and unresolved outbox records
when migrating. Update the Effect peer compatibility and consume current effect-cf and Alchemy releases.

Use Effect RpcServer for HTTP and WebSocket procedure dispatch, codecs, typed
responses, stream acknowledgements, and interruption. effect-cf supplies its native
DurableObjectRpcWebSocket transport; Alchemy uses a Cloudflare RpcServer.Protocol
bridge. Sync retains source ordering, bounded publication, durable retries, and
cursor recovery. Provide source-operation services with Server.provide; instance
Layers own the long-lived RPC server.
