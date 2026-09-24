# Native persistence probe

A small Expo Go SDK 57 consumer of the public SQLite adapter. It runs the shared
persistence conformance scenarios on the native bridge: exact evidence, rollback,
cache eviction, independent-connection conflicts, generation fencing, capacity,
actor isolation and deterministic client background flushing.

Run `vp run start` from this directory and open `exp://localhost:8097` in Expo Go.
If localhost resolves only to IPv6 while Expo advertises IPv4 bundle URLs, start
with `NODE_OPTIONS=--dns-result-order=ipv4first vp run start`.

On the first launch, the screen reports `seeded` and includes a command whose
response was lost after durable admission. Terminate Expo Go's process and reopen
the project. The second launch reports `restored`: the same command is resent and
its definitive result removes the journal row. Compare the seeded `command` and
restored `resent` objects, including command id, payload and authority generation.
The static unsupported-format evidence remains byte-for-byte equivalent across
relaunch. All databases use probe-only `native-*` namespaces.

The screen displays the result. The journal stays in Expo Go's application
container across process restarts. For a fresh two-launch run, change the probe namespace in `index.ts`.
The transaction scenarios create separate namespaces for each run.

This example is typechecked by `vp run ready`. Native execution requires an iOS or
Android host and remains a separate verification step from the desktop SQLite CI
suite. A simulator process restart establishes application restart recovery, not
hardware power-loss durability or every operating-system background policy.

Verified on 23 September 2026 with Expo Go 57.0.9, iPhone 17 Pro simulator,
iOS 26.5. The seeded and post-termination `resent` command envelopes matched
exactly, and the recovered journal was empty after the definitive result. The
transaction, eviction, conflict, generation-fence and background-flush scenarios
also passed through the native SQLite bridge. Android execution remains unverified.
