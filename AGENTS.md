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

## Testing policy

Default to no new tests or test infrastructure. Verify requested behavior with
existing checks and direct workflow evidence. Prefer E2E for complex features;
this does not require writing an E2E suite or a larger substitute for a rejected
unit test. Save a verifiable, repeatable artifact without building reporting machinery.

Never write unit tests after implementation. If isolation is necessary, first
write the scoped failure inventory, then the necessary failing tests, then the
code. New or expanded committed automation requires a current regression or an
explicit human test request, plus a concrete gap existing proof cannot cover.
Being a library does not waive this bar or require tests for every transition.

Load [testing](.agents/skills/testing/SKILL.md) before planning proof or adding,
retaining, or removing tests. It owns selection, failure-first isolation,
artifacts, evidence reuse and placement. Keep useful public-contract, atomicity,
recovery and authority checks at their strongest boundary; remove redundant
matrices and implementation mirrors. Preserve blocked required proof explicitly.
The final `vp run ready` gate still applies.

## Skills and pull requests

Use [simplify](.agents/skills/simplify/SKILL.md) to consider removing unnecessary
mechanisms within the affected workflow. Preserve required contracts, authority,
durability and retry guarantees; complexity alone does not justify unrelated cleanup.

Use [open-pull-request](.agents/skills/open-pull-request/SKILL.md) for concise PR
descriptions. Include diagrams only for meaningful architecture changes and code
examples only when they clarify the change. Opening a PR does not authorize merging it.

Contributor skills in `.agents/skills` are repository tooling, not runtime modules.
Dev Kit copies record their source in `.dev-kit-origin.json`. Update them explicitly with
the transient Dev Kit CLI when requested; the repository does not depend on Dev Kit
for installation, checks, builds, or CI.
