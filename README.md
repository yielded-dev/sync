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

Reusable Effect-native realtime synchronization, owned by [Yielded](https://github.com/yielded-dev).

Read the [published documentation](https://yielded.dev/sync/) for installation,
host choices, a complete consumer example, and client lifecycle guidance.

Shared source/action/plugin contracts, Schema envelopes, exact outcome codecs, and
derived Effect RPC contracts, the authoritative server runtime, and the Cloudflare
SQLite/Durable Object runtime with effect-cf and Alchemy v2 adapters, scoped headless client, and Effect Atom bindings are
implemented. The client includes explicit volatile and custom persistence modes and
a process-local memory adapter. Durable IndexedDB and Expo SQLite adapters are implemented. The
[public API document](docs/PUBLIC_API.md) defines the contract,
[consumer sketch](docs/src/content/docs/api/consumer.md), export ownership, and acceptance plan
for the extraction.

## Packages

| Directory                      | Package                             | Responsibility                                                                                 |
| ------------------------------ | ----------------------------------- | ---------------------------------------------------------------------------------------------- |
| `packages/sync`                | `@yielded/sync`                     | Shared contracts and runtimes, with explicit `./server`, `./client`, and `./atom` entry points |
| `packages/platform-cloudflare` | `@yielded/sync-platform-cloudflare` | Shared Cloudflare protocol and authoritative SQLite persistence                                |
| `packages/platform-effect-cf`  | `@yielded/sync-platform-effect-cf`  | Native effect-cf Durable Object integration                                                    |
| `packages/platform-alchemy-cf` | `@yielded/sync-platform-alchemy-cf` | Native Alchemy v2 Durable Object integration                                                   |
| `packages/local-indexeddb`     | `@yielded/sync-local-indexeddb`     | Browser local persistence                                                                      |
| `packages/local-expo`          | `@yielded/sync-local-expo`          | Expo local persistence using SQLite                                                            |

Adapters depend on core. Core stays platform-neutral; server storage and client
persistence have separate contracts. Applications own domain models, auth-provider
integration, projection destinations, and UI. Runnable consumers belong in
`examples/*` and depend on public package entry points.

The extraction must preserve atomic state/event/receipt/outbox commits, exact retries,
ordered replay, gap recovery, and authenticated lifecycle fencing. Disposable snapshot
caches stay separate from durable pending-intent journals. Ephemeral messages never
advance a durable cursor. Effect errors and requirements remain typed, and resources
remain bounded and scoped.

## Development

Install [Vite+](https://viteplus.dev/guide/) and use Node.js 22.18+ or 24.11+.
The repository pins Bun 1.4.2 and Vite+ 0.3.3, matching the other Yielded repositories.

```sh
vp install
vp -C packages/local-indexeddb exec playwright install chromium
vp run ready
```

Installation patches TypeScript with the pinned Effect TypeScript-Go compiler and
installs the Vite+ Git hook dispatcher. `vp run ready` builds all six packages,
then runs formatting, linting, typechecking, tests, and a clean packed-consumer
install, typecheck, and build.

| Command                     | Purpose                                                |
| --------------------------- | ------------------------------------------------------ |
| `vp fmt` / `vp fmt --check` | Format files / check formatting                        |
| `vp lint`                   | Type-aware linting                                     |
| `vp run typecheck`          | Pure TypeScript and Effect diagnostic checks           |
| `vp run check`              | Formatting, linting, and all workspace typechecks      |
| `vp run test`               | All workspace test suites                              |
| `vp run build`              | All package builds and the public docs site            |
| `vp run docs:build`         | Build the public documentation site                    |
| `vp run release:check`      | Pack six tarballs and verify a clean consumer          |
| `vp run ready`              | Full local and CI validation                           |
| `vp run patch:tsgo`         | Reapply the compiler patch after a script-free install |
| `vp run changeset`          | Record a consumer-visible change                       |

The [external contracts example](examples/contracts/README.md) demonstrates typed
public imports; the [Cloudflare counter](examples/cloudflare/README.md) is a runnable
effect-cf authority. The [Alchemy counter](examples/alchemy-cloudflare/README.md)
uses the same contract and server with native Alchemy Worker and Durable Object APIs.
The [list and board consumer](examples/list-board/README.md) has a React and Effect Atom
frontend with live cards, board renaming, presence, and IndexedDB persistence. Run
`vp run sync-list-board-example#dev` and open two tabs as Alice and Bob to try it.
Its Worker check exercises two clients through the same public entry points.
See the [client guide](docs/src/content/docs/client.md)
for headless and Atom usage,
[toolchain details](docs/TOOLCHAIN.md) for verification, and [contributor guidance](AGENTS.md).

## Release status

The original core, Cloudflare, IndexedDB, and Expo packages were published as
`0.1.0-beta.0`. This checkout adds `platform-effect-cf` and `platform-alchemy-cf`
and replaces the Cloudflare Worker factory for the next beta; those changes are
unreleased. All six packages export built ESM and declarations. The [release guide](docs/src/content/docs/RELEASE.md) lists supported hosts, the
beta compatibility policy, exact adoption versions, and the GitHub publication
workflow.
The standalone consumer covers convergence, rejection rollback, lost-response
retry, and reconnect; adapter checks cover browser reload and native iOS restart
storage. Product adoption and destination projection validation remain in the
[slide-deck pilot](docs/slide-deck-pilot.md).

## License

MIT
