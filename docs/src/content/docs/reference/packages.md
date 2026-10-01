---
title: Packages and hosts
---

The package family uses one beta version under the `@yielded` scope. Keep it exact
across the packages you install. The two framework adapters are pending release;
see [release status](../RELEASE.md).

| Package                                                                                                | Public entry points                                     | Host                                                     |
| ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------- | -------------------------------------------------------- |
| [`@yielded/sync`](https://www.npmjs.com/package/@yielded/sync)                                         | Root contracts, `./server`, `./client`, `./atom`        | Portable Effect code                                     |
| [`@yielded/sync-platform-cloudflare`](https://www.npmjs.com/package/@yielded/sync-platform-cloudflare) | `Cloudflare` protocol and `SqliteStorage`               | Shared Cloudflare runtime                                |
| `@yielded/sync-platform-effect-cf`                                                                     | `EffectCf` Layer, handlers, and request preparation     | Native effect-cf Workers and Durable Objects             |
| `@yielded/sync-platform-alchemy-cf`                                                                    | `AlchemyCf` constructor Effects and request preparation | Native Alchemy v2 Cloudflare Workers and Durable Objects |
| [`@yielded/sync-local-indexeddb`](https://www.npmjs.com/package/@yielded/sync-local-indexeddb)         | `IndexedDb` persistence                                 | Browser IndexedDB                                        |
| [`@yielded/sync-local-expo`](https://www.npmjs.com/package/@yielded/sync-local-expo)                   | `ExpoSqlite` persistence                                | Expo native SQLite                                       |

The core package declares its supported Effect range in its package manifest.
The Expo adapter also peers on `expo-sqlite@57.0.3`. The adapters depend on the
exact core beta version. The framework adapters also depend on the shared
Cloudflare package and peer on their framework. Effect-cf and Alchemy are separate
choices; neither adapter imports the other framework.

## Choose a host

Cloudflare hosting has workerd and SQLite Durable Object validation for commits,
replay, outbox delivery, and socket lifecycle. The IndexedDB adapter has Chromium
coverage for reload, concurrent tabs, cache recovery, and journal fencing. The
Expo adapter has file-backed SQLite checks and an iOS restart probe. Android
execution remains unverified.

The server entry point stays separate from client and root imports. Applications
choose native Worker APIs, routes, typed object bindings, and transport, and
supply their own authorization and projection services.
See the [getting started guide](../guide/getting-started.md) and
[consumer example](../api/consumer.md) for assembly.

## Version policy

This beta may change public APIs or persisted formats between releases. Pin exact
versions and plan explicit reconciliation for authoritative state, receipts, and
unresolved client journals. See the [release guide](../RELEASE.md) and
[source revision](https://github.com/yielded-dev/sync/releases/tag/v0.1.0-beta.0).
