# Repository toolchain

This repository follows Effect Agent's Bun workspace and Vite+ conventions.
The root `package.json` catalog is the source of truth for exact shared versions.

| Tool                 | Version        |
| -------------------- | -------------- |
| Bun                  | `1.4.0`        |
| Vite+                | `0.3.0`        |
| Effect               | `4.0.0-rc.112` |
| Effect Vitest        | `4.0.0-rc.112` |
| TypeScript           | `7.0.2`        |
| Effect TypeScript-Go | `0.45.0`       |

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
adapter entry points remain empty. The external contracts example participates in
workspace typechecking and has a separate `start` task for its codec smoke check.

`vp run ready` composes static checks, workspace tests, and package builds.
Tests use Vite+'s Vitest runner; Effect tests can use the catalog-pinned
`@effect/vitest`. Core has a behavioral contract suite and requires tests to be
present. Empty suites are still allowed for the runtime adapters; remove
`--passWithNoTests` from each adapter when its first behavioral suite lands.

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

## Contributor skills

Dev Kit 2.0.2 supplied the setup, Effect development, testing, Cloudflare Workers,
and visual explanation skills. They are normal tracked files with individual origin
receipts; `.claude/skills` links to the same content and `CLAUDE.md` links to
`AGENTS.md`.

Dev Kit is a transient setup tool. There is no Dev Kit dependency, runtime import,
managed project manifest, reconciliation step, or lifecycle command.
