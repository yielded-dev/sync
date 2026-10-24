# Shared contracts example

A counter and reusable label capability using public Sync exports. The example
shows composed snapshots, namespaced events, action-specific RPCs, and a timestamp
encoded with `Schema.DateTimeUtc`.

From the repository root:

```sh
vp run @yielded/example-contracts#typecheck
vp run @yielded/example-contracts#start
```

`start` prints the encoded and decoded snapshot. The typecheck covers public action,
service, client, and Atom types and runs as part of `vp run ready`. For runtime
setup, see the [counter](../cloudflare/README.md) or [client guide](../../docs/src/content/docs/client.md).
