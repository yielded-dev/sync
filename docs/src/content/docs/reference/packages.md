---
title: Packages and hosts
---

Core contracts are portable. Choose a server host and, when clients need restart
recovery, a local persistence adapter.

| Package                             | Public API                                       | Use it for                                              |
| ----------------------------------- | ------------------------------------------------ | ------------------------------------------------------- |
| `@yielded/sync`                     | Root contracts, `./server`, `./client`, `./atom` | Shared definitions, execution, clients, and UI bindings |
| `@yielded/sync-platform-cloudflare` | `Cloudflare`, `SqliteStorage`                    | Shared Cloudflare runtime and custom hosts              |
| `@yielded/sync-platform-effect-cf`  | `EffectCf`                                       | Native effect-cf Workers and Durable Objects            |
| `@yielded/sync-platform-alchemy-cf` | `AlchemyCf`                                      | Native Alchemy Workers and Durable Objects              |
| `@yielded/sync-local-indexeddb`     | `IndexedDb`                                      | Browser client persistence                              |
| `@yielded/sync-local-expo`          | `ExpoSqlite`                                     | Native Expo SQLite persistence                          |

The two framework adapters use the shared Cloudflare runtime. Pick the one that
matches your application; neither requires the other framework. Applications own
Worker routes, typed object bindings, authentication, authorization, and projection
destinations. Server implementations are not exported from root or client entry
points.

IndexedDB storage remains subject to browser quota and eviction. Expo persistence
targets native SQLite; use IndexedDB for Expo web. Android execution is unverified.
Both adapters retain unresolved command journals separately from disposable
snapshot caches. See [client persistence](../client.md#persistence).

Keep Sync packages on the same exact version. Package manifests declare supported
Effect, framework, and Expo peer ranges; the repository catalog pins the development
combination. Read the [upgrade guidance](../RELEASE.md) before changing versions.
