# Native persistence probe

An Expo Go consumer of `@yielded/sync-local-expo` for checking SQLite
transactions and recovery after an app restart. It exercises exact retry evidence,
rollback, connection conflicts, and background flushing through the native bridge.

## Run and inspect

From this directory, run `vp run start` and open `exp://localhost:8097` in Expo Go.
If localhost resolves only to IPv6 while Expo advertises IPv4 bundle URLs, use
`NODE_OPTIONS=--dns-result-order=ipv4first vp run start`.

1. On first launch, inspect the `seeded` result. It includes a command whose
   response was lost after durable admission.
2. Terminate Expo Go's process and reopen the project.
3. Inspect `restored`. Compare its `resent` object with the seeded `command`:
   command id, payload, and authority must match. The definitive result removes
   the pending journal row; unsupported-format evidence remains unchanged.

Save the displayed results with the tested revision, device/OS, setup, and restart
steps. The journal stays in Expo Go's application container across restarts.
All databases use probe-only `native-*` namespaces; transaction scenarios create
separate namespaces per run. For a fresh two-launch run, change the probe namespace
in `index.ts`. Remove only probe data when cleaning up.

`vp run ready` typechecks this example. Native execution requires an iOS or Android
host and is separate from desktop SQLite checks. A process restart establishes app
restart recovery, not hardware power-loss durability or every OS background policy.
