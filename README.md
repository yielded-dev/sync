# Effect Sync

Reusable Effect-native realtime synchronization, owned by [Yielded](https://github.com/yielded-dev).

Shared source/action/plugin contracts, Schema envelopes, exact outcome codecs, and
derived Effect RPC contracts, the authoritative server runtime, and the Cloudflare
SQLite/Durable Object adapter, scoped headless client, and Effect Atom bindings are
implemented. The client includes explicit volatile and custom persistence modes and
a process-local memory adapter. Durable IndexedDB and Expo SQLite adapters are implemented. The
[public API document](docs/PUBLIC_API.md) defines the contract,
[consumer sketch](docs/api/consumer.md), export ownership, and acceptance plan
for the extraction.

## Packages

| Directory                      | Package                             | Responsibility                                                                                 |
| ------------------------------ | ----------------------------------- | ---------------------------------------------------------------------------------------------- |
| `packages/sync`                | `@yielded/sync`                     | Shared contracts and runtimes, with explicit `./server`, `./client`, and `./atom` entry points |
| `packages/platform-cloudflare` | `@yielded/sync-platform-cloudflare` | Server hosting and authoritative persistence                                                   |
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
The repository pins Bun 1.4.0, matching Effect Agent.

```sh
vp install
vp -C packages/local-indexeddb exec playwright install chromium
vp run ready
```

Installation patches TypeScript with the pinned Effect TypeScript-Go compiler and
installs the Vite+ Git hook dispatcher. `vp run ready` runs formatting, linting,
typechecking, tests, and ESM/declaration builds for all four packages.

| Command                     | Purpose                                                |
| --------------------------- | ------------------------------------------------------ |
| `vp fmt` / `vp fmt --check` | Format files / check formatting                        |
| `vp lint`                   | Type-aware linting                                     |
| `vp run typecheck`          | Pure TypeScript and Effect diagnostic checks           |
| `vp run check`              | Formatting, linting, and all workspace typechecks      |
| `vp run test`               | All workspace test suites                              |
| `vp run build`              | All package ESM, declaration, and source-map builds    |
| `vp run ready`              | Full local and CI validation                           |
| `vp run patch:tsgo`         | Reapply the compiler patch after a script-free install |
| `vp run changeset`          | Record a consumer-visible change                       |

The [external contracts example](examples/contracts/README.md) demonstrates typed
public imports; the [Cloudflare counter](examples/cloudflare/README.md) is a runnable
authority. The [list and board consumer](examples/list-board/README.md) exercises
two clients through those public entry points. See the [client guide](docs/client.md)
for headless and Atom usage,
[toolchain details](docs/TOOLCHAIN.md) for verification, and [contributor guidance](AGENTS.md).

## Release status

The four package manifests are private while their APIs are under construction.
Changesets defines their eventual shared release group. Package exports resolve local
TypeScript sources for development, and `vp pack` writes build artifacts to `dist/`.
There is no automatic npm publication workflow.

Before the first beta, configure built package exports, remove the private flags,
establish prerelease versioning and npm publishing, and complete the
[slide-deck pilot](docs/slide-deck-pilot.md) against its product destination. The
standalone consumer covers convergence, rejection rollback, lost-response retry,
and reconnect; the adapter suites cover browser reload and native restart storage.

## License

MIT
