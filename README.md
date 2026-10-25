<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset=".github/assets/lockup-sync-paper.svg" />
    <img src=".github/assets/lockup-sync-ink.svg" alt="Effect Sync" height="48" />
  </picture>
</h1>

<p align="center">
  <a href="https://www.npmjs.com/package/@yielded/sync"><img alt="npm" src="https://img.shields.io/npm/v/@yielded/sync/beta?label=npm&labelColor=121310&color=c6f36a" /></a>
  <a href="https://github.com/yielded-dev/sync/actions/workflows/ci.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/yielded-dev/sync/ci.yml?branch=main&label=ci&labelColor=121310" /></a>
  <a href="LICENSE"><img alt="MIT license" src="https://img.shields.io/badge/license-MIT-f3f1e8?labelColor=121310" /></a>
</p>

<p align="center">
  <a href="https://yielded.dev/sync/"><b>Documentation</b></a>
  ·
  <a href="https://yielded.dev/sync/guide/getting-started/">Getting started</a>
  ·
  <a href="https://yielded.dev">yielded.dev</a>
</p>

Effect-native realtime synchronization with optimistic clients and exact retries.
Define a shared source contract, supply its server handlers, and connect scoped
clients from a browser or native app.

Start with the [documentation](https://yielded.dev/sync/),
[getting started guide](https://yielded.dev/sync/guide/getting-started/), or
[runnable board](examples/list-board/README.md).

## Packages

| Package                             | Responsibility                                                                    |
| ----------------------------------- | --------------------------------------------------------------------------------- |
| `@yielded/sync`                     | Shared contracts, with separate `./server`, `./client`, and `./atom` entry points |
| `@yielded/sync-persistence`         | Optional memory storage and shared durable client persistence policy              |
| `@yielded/sync-platform-cloudflare` | Shared Cloudflare protocol and SQLite storage                                     |
| `@yielded/sync-platform-effect-cf`  | Native effect-cf Worker and Durable Object integration                            |
| `@yielded/sync-platform-alchemy-cf` | Native Alchemy Worker and Durable Object integration                              |
| `@yielded/sync-local-indexeddb`     | Browser persistence                                                               |
| `@yielded/sync-local-expo`          | Expo SQLite persistence                                                           |

Applications own domain models, authentication, authorization, projection
destinations, and UI. Applications supply server storage and may implement client
journals directly; core owns no persistence implementation. Sync coordinates atomic
state/event/receipt commits, optional outbox obligations, ordered replay, and client recovery.
Snapshot caches are disposable; unresolved command
journals retain the identities needed for safe retry.

See [packages and hosts](docs/src/content/docs/reference/packages.md) for adapter
choices and [GitHub releases](https://github.com/yielded-dev/sync/releases) for
release notes.

## Development

Install [Vite+](https://viteplus.dev/guide/), then run:

```sh
vp install
vp -C packages/local-indexeddb exec playwright install chromium
vp run ready
```

The root manifest pins the package manager, dependencies, and supported Node.js
versions. Installation configures Git hooks; typechecking uses the pinned
Rust-based Effect compiler. `vp run ready` builds packages and docs, checks
formatting and types, runs tests, and verifies a clean consumer of the packed
packages.

Use `vp run docs:dev` for the docs site or
`vp run sync-list-board-example#dev` for the React board and its local Worker.
See [all examples](examples/README.md), [toolchain commands](docs/TOOLCHAIN.md),
[API boundaries](docs/PUBLIC_API.md), and [contributor guidance](AGENTS.md).

## License

MIT
