# Shared contracts example

This external workspace imports only public `@yielded/sync` exports. A counter and
reusable label capability compose a snapshot, namespaced events/messages, and
action-specific RPCs. The counter's timestamp uses `Schema.DateTimeUtc` with the
canonical JSON codec.

From the repository root:

```sh
vp run --filter @yielded/example-contracts typecheck
vp run --filter @yielded/example-contracts start
```

Typechecking verifies action result/error correlation and service requirements
through native RPC and Layer composition, headless client action types, and Atom
mutations. `start` displays the encoded and decoded public snapshot. `vp run ready`
includes these typechecks. Runtime behavior and persistence boundaries are covered
in the [client guide](../../docs/client.md).
