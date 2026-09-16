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

Typechecking verifies action payload/result/rejection correlation, duplicate and
missing registration rejection, and service requirements through native RPC and
Layer composition. `start` round-trips the public snapshot and checks its encoded
JSON shape. `vp run ready` includes the example's typechecks and the core contract
tests. This example supplies contract evidence; two-client convergence, retries,
reconnect, and durable recovery await the runtime implementations.
