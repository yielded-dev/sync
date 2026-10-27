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

Build realtime apps with Effect. Define a shared source, run it on a server, and
connect clients from a browser or native app. Sync can show edits immediately and
keeps their identities intact when they need to be retried.

Start with the [documentation](https://yielded.dev/sync/),
[getting started guide](https://yielded.dev/sync/guide/getting-started/), or
[runnable board](examples/list-board/README.md).

## Packages

| Package                             | Responsibility                                                      |
| ----------------------------------- | ------------------------------------------------------------------- |
| `@yielded/sync`                     | Shared contracts, portable server hosts, clients, and Atom bindings |
| `@yielded/sync-platform-browser`    | Browser services and Atom bindings with caller-provided persistence |
| `@yielded/sync-persistence`         | Shared client persistence policy and driver contracts               |
| `@yielded/sync-local-indexeddb`     | Browser client persistence                                          |
| `@yielded/sync-platform-cloudflare` | Cloudflare transport, lifecycle, and SQLite storage                 |
| `@yielded/sync-platform-effect-cf`  | Native effect-cf Worker and Durable Object integration              |
| `@yielded/sync-platform-alchemy-cf` | Native Alchemy Worker and Durable Object integration                |

You bring the domain model, authentication, authorization, projection destinations,
and UI. Sync commits state, events, and receipts together, replays changes in order,
and helps clients recover after a disconnect. It also supports an outbox for work
that must be delivered elsewhere.

Supply server storage to a core host, or use the Cloudflare integrations with
their default SQLite storage. Client journals can use an adapter or an application
implementation. A snapshot cache can be discarded; a journal of unresolved
commands holds the identities needed for safe retries.

`Server.host` and `ServerRpc` work with application-provided storage and transport.
The Cloudflare packages add native Worker and Durable Object integration around
the same host, as shown in the [React board](examples/list-board/README.md).

See [packages and hosts](docs/src/content/docs/reference/packages.mdx) for adapter
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
See the [example guide](examples/list-board/README.md), [toolchain commands](docs/TOOLCHAIN.md),
[API boundaries](docs/PUBLIC_API.md), and [contributor guidance](AGENTS.md).

## License

MIT
