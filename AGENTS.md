# Repository guidance

Read `README.md` and `docs/TOOLCHAIN.md` before changing repository structure or
tooling. Settle the [public API contracts](docs/PUBLIC_API.md) before extracting
runtime implementations. Keep documentation independent of issue trackers and
ticket identifiers; describe behavior, boundaries, and implementation stages directly.

## Effect

Before writing Effect code, read `node_modules/effect/AGENTS.md` completely and
follow its relevant links. For APIs not covered there, inspect
`node_modules/effect/src`. Do not copy library guidance into this file.

Use `.agents/skills/effect-development` for focused setup, Atom, API, and CLI work.
Keep asynchronous public operations in `Effect` or `Stream`, with typed errors and
requirements. Effect Schema owns transported and persisted values. Resources must
be bounded and scoped.

## Commands

Vite+ is the command authority; Bun is the package manager and script runtime.
Use `vp install`, `vp fmt`, `vp lint`, `vp test`, and `vp run <task>`.
Do not invoke wrapped compilers, formatters, linters, or test runners directly.
Run `vp help` for available commands and consult `node_modules/vite-plus/docs`.
Use `vp env doctor` for toolchain troubleshooting.

The root catalog owns exact shared dependency versions. Use `catalog:` for shared
dependencies and `workspace:*` for internal packages. After dependency changes,
run `vp install` and the relevant verification. Fix and release owning libraries;
consume released runtime versions without dependency patches. The supported
Effect TypeScript-Go compiler installation is toolchain setup.

Before handoff, run `vp run ready`. CI runs the same command after a frozen install
and explicit compiler setup.

## Boundaries

- Framework code lives in `packages/*`; runnable consumers live in `examples/*`.
- Platform adapters depend inward on `@yielded/sync`. Core must not import adapters,
  Node/Bun platform implementations, Cloudflare, Expo, or browser persistence APIs.
- Keep shared contracts, server, client, and Atom entry points explicit. Do not
  re-export server implementations from client or root entry points.
- Server storage and client persistence are different contracts. Snapshot caches
  are disposable; pending-intent journals retain unresolved retry evidence.
- Preserve atomic state/event/receipt/outbox commits, exact retries, ordered replay,
  gap recovery, and authenticated lifecycle fencing. Ephemeral messages do not
  advance durable cursors.
- Applications retain domain models, auth-provider integration, projection
  destinations, and UI. Do not move application concerns into the library.

## Verification and skills

Follow `.agents/skills/testing/SKILL.md`. Commit tests when they protect plausible
regressions with independent observable assertions. Use the cheapest faithful
boundary and reuse focused evidence for unchanged inputs. Record unavailable
required proof explicitly; do not equate an empty suite with verified behavior.

Contributor skills in `.agents/skills` are repository tooling, not runtime modules.
Each `.dev-kit-origin.json` receipt records its source. Update them explicitly with
the transient Dev Kit CLI when requested; the repository does not depend on Dev Kit
for installation, checks, builds, or CI.
