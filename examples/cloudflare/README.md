# Cloudflare counter

A runnable authority using only public package exports: a counter, a reusable
label plugin, private audit fields, exact action results, and hibernating RPC.

```sh
vp install
vp run sync-cloudflare-example#dev
```

The endpoint is `/sync/counters/:id`. The local demonstration recognizes
`Authorization: Bearer alice-local` and `Authorization: Bearer bob-local`.
Replace this authentication function with the application's provider before
deployment. The outbox delivery function acknowledges demonstration records;
replace it with the destination's idempotent delivery Effect when adding a projection.

Use `RpcClient.make(Counter.rpc, { flatten: true })` with
`RpcSerialization.layerJson`. Supply `RpcClient.layerProtocolHttp` for unary
operations, or `RpcClient.layerProtocolSocket` for all operations including
subscription and ephemeral messages. The application supplies HTTP/socket layers
and credentials. A socket carries one active subscription.

First request `snapshot` with `{ protocolVersion: 1, schemaVersion: 1, address }`.
Use its authority generation in the action's command envelope. Allocate a command
id once and retain the exact payload for retry. `execute/$source/set` returns
the original previous/current values, including after another command changes state.
`execute/label/rename` changes only the label slot. Neither private audit field
appears in the public snapshot.

`subscribe` accepts an optional `after` position. A fresh connection gets a snapshot;
a retained position gets ordered events; a gap produces `ResyncRequired`, after
which a new snapshot supplies current state. This example retains four events to
make gap recovery easy to exercise. Production hosts choose their own bounded window.

```sh
vp run sync-cloudflare-example#types
vp run sync-cloudflare-example#build
vp run @yielded/sync-platform-cloudflare#test
vp run ready
```

The build is a Wrangler dry run. Checks run local workerd and SQLite, including
eviction/hibernation, exact retries, transaction rollback, and outbox lease recovery.
No command above publishes a Worker. The compatibility date matches the supported
test runtime. The generated binding file contains environment declarations; runtime
types come from the pinned Workers types package.
