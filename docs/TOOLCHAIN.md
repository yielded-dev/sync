# Repository toolchain

This repository follows Effect Agent's Bun workspace and Vite+ conventions.
The root `package.json` catalog is the source of truth for exact shared versions.

| Tool                        | Version        |
| --------------------------- | -------------- |
| Bun                         | `1.4.0`        |
| Vite+                       | `0.3.0`        |
| Effect                      | `4.0.0-rc.112` |
| Effect Vitest               | `4.0.0-rc.112` |
| TypeScript                  | `7.0.2`        |
| Effect TypeScript-Go        | `0.45.0`       |
| effect-cf                   | `0.42.1`       |
| Effect SQLite DO / D1       | `4.0.0-rc.112` |
| Cloudflare Worker test pool | `0.22.0`       |
| Cloudflare Workers types    | `5.20260825.1` |
| Wrangler                    | `4.133.0`      |

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
entry points remain empty. The external contracts example participates in
workspace typechecking and has a separate `start` task for its codec smoke check.

`vp run ready` composes static checks, workspace tests, and package builds.
Tests use Vite+'s Vitest runner; Effect tests can use the catalog-pinned
`@effect/vitest`. Core has behavioral contract, client, lifecycle, and Atom suites and requires tests
to be present. The Cloudflare package uses the released Worker pool in its Vite
configuration and runs tests against the public Cloudflare example. Workerd and
SQLite provide storage, interruption, eviction, hibernation, and native RPC proof.
The example's build is a Wrangler dry run and participates in `ready`. Its `types`
task generates binding declarations; pinned Workers types supply the runtime
declarations. Generated bindings are excluded from formatting and linting.
Empty suites remain allowed for the two local persistence adapters.

Vite Task caches successful script results. Vitest's mutable result cache is
disabled to avoid invalidating task inputs. Use `vp run --no-cache <task>` when
investigating a cache concern.

## Git hooks and CI

`vp install` recreates the ignored dispatcher in `.vite-hooks/_` and sets the
clone's hook path. The committed `.vite-hooks/pre-commit` runs `vp staged`.
Inspect the installation with `vp hooks status`.

GitHub Actions installs dependencies with lifecycle scripts suppressed, explicitly
patches the compiler, and runs `vp run ready`. The `ready` job is the CI gate.
The workflow does not deploy or publish packages.

`pr-review.yml` uses the published Effect Agent review action and the repository's
`OPENAI_API_KEY` secret. It reviews non-draft, same-repository PRs on opening,
reopening, readiness, and new commits. Fork PRs require an owner, member, or
collaborator to request review with `@effect-agent review full`. The same command
starts a full retry; `@effect-agent review` requests an incremental pass. A manual
workflow dispatch accepts a PR number and starts a full review.

The privileged review job checks out only default-branch guidance and never runs
PR code or dependency installation. It uses `GITHUB_TOKEN` to publish feedback and
the `Effect Agent review` check, with `AGENTS.md` as repository guidance. No GitHub
App secrets are required. Reviews use `gpt-6-astra` with medium reasoning and
standard processing, at most two automatic attempts per PR, a $1 base allowance,
and a $2.50 ceiling per attempt. Manual attempts have the same spending ceiling.
The check reports blockers and incomplete coverage; it is separate from the
required `ready` CI job.

## Contributor skills

Dev Kit 2.0.2 supplied the setup, Effect development, testing, Cloudflare Workers,
pull request, and visual explanation skills. They are normal tracked files with individual origin
receipts; `.claude/skills` links to the same content and `CLAUDE.md` links to
`AGENTS.md`.

Dev Kit is a transient setup tool. There is no Dev Kit dependency, runtime import,
managed project manifest, reconciliation step, or lifecycle command.
