# Shared contracts and generic server

A counter and reusable label capability using public Sync exports. The example
shows composed snapshots, namespaced events, action-specific RPCs, and a timestamp
encoded with `Schema.DateTimeUtc`.

From the repository root:

```sh
vp run @yielded/example-contracts#typecheck
vp run @yielded/example-contracts#start
vp run @yielded/example-contracts#server
```

`start` prints the encoded and decoded snapshot. `server` runs a complete counter
through `Server.host`, using only `@yielded/sync`, `@yielded/sync/server`, and
Effect. It connects a trusted session, receives subscription bootstrap and a live
event, dispatches the same command twice, and looks up its retained result. The
final value is `3` at cursor `1`: the retry did not run the action again.

[server.ts](src/server.ts) supplies authorization and Web Crypto; a real transport
authenticates before constructing its session. [storage.ts](src/storage.ts) is a
bounded, process-local demonstration store. It serializes transactions and swaps
copied state, events, and receipts only after success. It refuses work after 64
commands or events, retains every receipt, and rejects outbox work. Restarting the
process loses all data; applications supply their own durable `SourceStorage`.

The typecheck covers public action, service, client, and Atom types and runs as
part of `vp run ready`. See the [Cloudflare counter](../cloudflare/README.md) for a
platform host or the [client guide](../../docs/src/content/docs/client.mdx) for
client setup.
