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
installs the Vite+ Git hook dispatcher. `vp run ready` builds all four packages,
then runs formatting, linting, typechecking, tests, and a clean packed-consumer
install, typecheck, and build.

| Command                     | Purpose                                                |
| --------------------------- | ------------------------------------------------------ |
| `vp fmt` / `vp fmt --check` | Format files / check formatting                        |
| `vp lint`                   | Type-aware linting                                     |
| `vp run typecheck`          | Pure TypeScript and Effect diagnostic checks           |
| `vp run check`              | Formatting, linting, and all workspace typechecks      |
| `vp run test`               | All workspace test suites                              |
| `vp run build`              | All package ESM, declaration, and source-map builds    |
| `vp run release:check`      | Pack four tarballs and verify a clean consumer         |
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

The four publishable packages are versioned as `0.1.0-beta.0` and export built
ESM and declarations. The [release guide](docs/RELEASE.md) lists supported hosts,
the beta compatibility policy, exact adoption versions, and the manual GitHub
publication workflow, including the credential needed for the first version.
The standalone consumer covers convergence, rejection rollback, lost-response
retry, and reconnect; adapter checks cover browser reload and native iOS restart
storage. Product adoption and destination projection validation remain in the
[slide-deck pilot](docs/slide-deck-pilot.md).

## License

MIT
