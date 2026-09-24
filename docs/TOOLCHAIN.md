# Repository toolchain

This repository follows Effect Agent's Bun workspace and Vite+ conventions.
The root `package.json` catalog is the source of truth for exact shared versions.

| Tool                        | Version              |
| --------------------------- | -------------------- |
| Bun                         | `1.4.0`              |
| Vite+                       | `0.3.0`              |
| Effect                      | `4.0.0-rc.112`       |
| Effect Vitest               | `4.0.0-rc.112`       |
| TypeScript                  | `7.0.2`              |
| Effect TypeScript-Go        | `0.45.0`             |
| effect-cf                   | `0.42.1`             |
| Effect SQLite DO / D1       | `4.0.0-rc.112`       |
| Cloudflare Worker test pool | `0.22.0`             |
| Cloudflare Workers types    | `5.20260825.1`       |
| Wrangler                    | `4.133.0`            |
| Expo / Expo SQLite          | `57.0.24` / `57.0.3` |
| React / React Native        | `19.2.3` / `0.86.3`  |
| Playwright                  | `1.58.2`             |

Effect and Effect Vitest stay aligned on rc.112, the last release whose test
helper supports Vite+'s Vitest 4 runner. rc.113+ requires Vitest 5 and changes
Effect testing module paths. Upgrade both together when the runner supports them.

Workspace manifests inherit shared versions through `catalog:` and refer to core
through `workspace:*`. `bunfig.toml` disables implicit workspace linking. Commit
`bun.lock`; CI uses a frozen install.

## Compiler, linting, and formatting

`tsconfig.base.json` defines strict, platform-neutral TypeScript options and Effect
diagnostics. The IndexedDB adapter adds browser libraries locally. Root tooling has
Node types; core has no ambient Node or DOM types.

`prepare` invokes the upstream `effect-tsgo patch --typescript` command, which
installs the matching native compiler, then configures Vite+ hooks. The explicit
`patch:tsgo` task supports installs that suppress lifecycle scripts. This needs
network access to the upstream compiler release on a fresh machine.

`vp run typecheck` checks root tooling and every workspace with the patched compiler.
Effect diagnostic warnings are configured in the shared compiler plugin, including
`preferTypedSchemaDecoder` and the recommended simplification rules.
`unsafeEffectTypeAssertion` is a warning; `schemaSync` stays off.

`vite.config.ts` owns Oxfmt and Oxlint configuration. It enables type-aware linting,
typechecking, import checks, exhaustive switches, test hygiene, and Effect Agent's
statement-spacing rules. The local `oxlint/plugin-style.js` exposes the one required
Stylistic rule. Core imports cannot cross into platform adapters or host packages.
Copied skills and generated hook internals have explicit tool ignores.

## Builds and tests

Every package has its own `vite.config.ts`, pure `typecheck` task, test task, and
`vp pack` build. Builds produce ESM, declarations, and source maps in ignored `dist/`
directories. Core's root entry point contains the shared contracts; runtime and
adapter entry points implement authoritative execution through `./server` and
the Cloudflare package. Client and Atom entry points implement scoped replicas,
transport/recovery, persistence ports, and view bindings. Local persistence adapter
entry points provide scoped IndexedDB and Expo SQLite handles. The external contracts example participates in
workspace typechecking and has a separate `start` task that displays a codec round trip.

`vp run ready` builds packages first so workspace imports resolve their published
ESM/declaration exports, then runs static checks, workspace tests, and a clean
packed-consumer install, typecheck, and build. The build task also generates the
VitePress documentation site from `docs/`.
Tests use Vite+'s Vitest runner; Effect tests can use the catalog-pinned
`@effect/vitest`. Core retains focused client admission, recovery and lifecycle races.
The Cloudflare package uses the released Worker pool in its Vite
configuration and runs tests against the public Cloudflare example. Workerd and
SQLite provide storage, interruption, eviction, hibernation, and native RPC proof.
The example's build is a Wrangler dry run and participates in `ready`. Its `types`
task generates binding declarations; pinned Workers types supply the runtime
declarations. Generated bindings are excluded from formatting and linting.
The list and board example adds a separate public-consumer Worker test and dry-run
build, including two-client recovery after Durable Object eviction and a forced
subscription disconnect.
The IndexedDB suite runs Chromium with Playwright through Vite+; install Chromium
with `vp -C packages/local-indexeddb exec playwright install chromium`. CI installs
the browser and its system dependencies explicitly. The Expo adapter suite uses
real file-backed Node SQLite through the Expo API subset. The Expo Go probe in
`examples/persistence-expo` is a separate native bridge/restart check and participates
in workspace typechecking.

Use the [testing skill](../.agents/skills/testing/SKILL.md) to choose proof for a
change. These existing entrypoints do not require new tests or a scenario matrix.

Vite Task caches successful script results. Vitest's mutable result cache is
disabled to avoid invalidating task inputs. Use `vp run --no-cache <task>` when
investigating a cache concern.

## Git hooks and CI

`vp install` recreates the ignored dispatcher in `.vite-hooks/_` and sets the
clone's hook path. The committed `.vite-hooks/pre-commit` runs `vp staged`.
Inspect the installation with `vp hooks status`.

GitHub Actions installs dependencies with lifecycle scripts suppressed, explicitly
patches the compiler, and runs `vp run ready`. The `ready` job is the CI gate.
`ci.yml` does not deploy or publish packages. The manually dispatched
`publish-beta.yml` runs the same `ready` gate and publishes its validated tarballs
through npm trusted publishing. It verifies the registry install and records the
source revision and workflow evidence in a GitHub prerelease. A successful beta
publication triggers `deploy-docs.yml` for that same revision. See the
[release guide](RELEASE.md) for host support and credential setup.

`pr-review.yml` uses the published Effect Agent review action and the repository's
`OPENAI_API_KEY` secret. It reviews non-draft, same-repository PRs on opening,
reopening, readiness, and new commits. Fork PRs require an owner, member, or
collaborator to request review with `@effect-agent review full`. The same command
starts a full retry; `@effect-agent review` requests an incremental pass. A manual
workflow dispatch accepts a PR number and starts a full review.

The privileged review job checks out only default-branch guidance and never runs
PR code or dependency installation. It uses `GITHUB_TOKEN` to publish feedback and
the `Effect Agent review` check, with `AGENTS.md` as repository guidance. No GitHub
App secrets are required. Reviews use `gpt-6-sol` with high reasoning and
Fast mode, at most five automatic attempts per PR, a $1 base allowance,
and a $2.50 ceiling per attempt. Manual attempts have the same spending ceiling.
The check reports blockers and incomplete coverage; it is separate from the
required `ready` CI job.

## Documentation site

The public documentation source lives in `docs/`. `vp run docs:dev`,
`vp run docs:build`, and `vp run docs:preview` operate its VitePress site with a
`/sync/` base path. The build fills `{{SYNC_VERSION}}` from the core package
manifest, so installation guidance follows Changesets version bumps.
`site/wrangler.jsonc` deploys a separate static asset Worker
on the `yielded.dev/sync*` route; its handler removes that path prefix before
reading assets. The existing `/auth` route is owned separately.

`deploy-docs.yml` runs after a successful `Publish beta` workflow and checks out
the validated release revision. It skips deployment if `main` has advanced, so
an older run cannot replace newer docs. A manual workflow dispatch from current
`main` can publish docs between package releases or retry a skipped run. The
workflow needs the repository secret `CLOUDFLARE_API_TOKEN` with permission to
deploy the docs Worker and manage its `yielded.dev` route.

`vp run docs:deploy` also builds and deploys the site locally. It requires
Cloudflare access to the Yielded domain's account and Worker routes. With the
local Wrangler OAuth session, unset any unrelated `CLOUDFLARE_API_TOKEN` and
`CLOUDFLARE_ACCOUNT_ID` values before running it. Check the deployed home page,
a guide page, and an asset URL after publication.

## Contributor skills

Dev Kit 2.0.2 supplied the setup, Effect development, testing, Cloudflare Workers,
pull request, and visual explanation skills. They are normal tracked files with individual origin
receipts; `.claude/skills` links to the same content and `CLAUDE.md` links to
`AGENTS.md`.

Dev Kit is a transient setup tool. There is no Dev Kit dependency, runtime import,
managed project manifest, reconciliation step, or lifecycle command.
