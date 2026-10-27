# Repository toolchain

Vite+ is the command authority; Bun is the package manager and script runtime.
The root [package.json](../package.json) pins tools, supported Node.js versions,
and shared dependencies. Use `catalog:` for shared versions and `workspace:*`
for internal packages. Commit `bun.lock`; CI installs it frozen.

## Setup and commands

Install Vite+, then run from the repository root:

```sh
vp install
vp -C packages/local-indexeddb exec playwright install chromium
vp run ready
```

Installation configures Vite+ Git hooks. Rust compiler binaries arrive through
platform optional dependencies; keep those enabled during installation. No
compiler patch or post-install download is required. TypeScript remains installed
for declaration builds, and `@effect/tsgo` supplies the shared configuration schema
and editor language-service features. Use `vp env doctor` for toolchain problems
and `vp help` for command options.

| Command                                   | Purpose                                              |
| ----------------------------------------- | ---------------------------------------------------- |
| `vp fmt` / `vp fmt --check`               | Format / check formatting                            |
| `vp lint`                                 | Type-aware linting                                   |
| `vp run typecheck`                        | Root and workspace TypeScript/Effect diagnostics     |
| `vp run check`                            | Formatting, linting, and typechecks                  |
| `vp run test`                             | Workspace tests                                      |
| `vp run build`                            | Packages, the board example, and public docs         |
| `vp run release:check`                    | Pack packages and verify a clean consumer            |
| `vp run ready`                            | Build, check, test, and packed-consumer verification |
| `vp run docs:dev` / `vp run docs:preview` | Serve source / built documentation                   |
| `vp run docs:build`                       | Build the documentation site                         |

`vp run typecheck` checks root tooling, all libraries, and the board example
with `effect-tsrs diagnostics --project tsconfig.json --type-check --strict`. It
checks TypeScript types and Effect diagnostics; configured errors and warnings
fail the check. The packed-consumer check uses `tsrs --noEmit`; the docs workspace
retains its Astro build.

Builds precede typechecks because workspace imports resolve built ESM and
declarations. Use `vp run --no-cache <task>` to investigate stale task results.
The configuration lives in [vite.config.ts](../vite.config.ts) and
[tsconfig.base.json](../tsconfig.base.json); required Effect diagnostic settings
are documented in [AGENTS.md](../AGENTS.md#compiler-diagnostics).

## Verification boundaries

Use the [testing skill](../.agents/skills/testing/SKILL.md) to select sufficient
proof. Runtime suites mirror public modules under each package's `test/` tree;
Effect programs use `@effect/vitest`. Compile-only public contract assertions live
in `typetest/*.tst.ts` and run through the package's normal typecheck. Existing
checks cover different environments:

| Boundary                   | Existing check                                     |
| -------------------------- | -------------------------------------------------- |
| Portable client            | Core suite with Effect's test clock                |
| Cloudflare storage and RPC | effect-cf and board suites in workerd with SQLite  |
| Browser persistence        | IndexedDB suite in Chromium                        |
| Public package exports     | Core type fixtures and clean packed-consumer check |

See the [board example](../examples/list-board/README.md) for development commands
and the browser workflow.

## Hooks and CI

`vp install` configures the ignored dispatcher in `.vite-hooks/_`.
The committed pre-commit hook runs `vp staged`; inspect it with `vp hooks status`.

Pull requests require `ready` and `Effect Agent review`. CI selects formatting,
workflow, and docs checks for limited documentation changes; source, dependency,
release, unknown, or incomplete diffs use the full gate. Pushes to `main` run the
full gate. Title and description edits do not rerun CI. After retargeting a PR,
close and reopen it to validate the new base.

CI installs dependencies without lifecycle scripts, keeps optional platform
binaries enabled, and caches successful Vite Task results. Timeout retries and
fresh processes for stateful test retries are configured in
[ci.yml](../.github/workflows/ci.yml) and the [run-ci action](../.github/actions/run-ci/action.yml).
CI does not deploy or publish packages.

The review workflow requires `OPENAI_API_KEY`. It uses default-branch guidance
and does not execute PR code. An owner, member, or collaborator can request
`@effect-agent review` for an incremental pass or `@effect-agent review full`
for a full retry. Manual dispatch also accepts a PR number. Missing, incomplete,
or blocked reviews prevent merging; a new commit requires its own review.
Review budgets and model settings live in [pr-review.yml](../.github/workflows/pr-review.yml).

## Documentation site

The Astro Starlight workspace publishes `docs/src/content/docs/` under `/sync/`.
Top-level files in `docs/` are contributor guidance and are not published.
[astro.config.ts](astro.config.ts) owns the sidebar; `@yielded/starlight-theme`
owns rendering, colors, and the library switcher.

Use relative Markdown links between public pages. The build rewrites them and
checks links and anchors. Installation examples use bare package names. Follow the
[documentation guidance](../AGENTS.md#documentation) when editing pages.

## Releases and docs deployment

Use `vp run changeset` for consumer-visible release notes,
and `vp run changeset status` to inspect pending releases. Pushes to `main` run
[Release](../.github/workflows/release.yml), which opens or updates
`changeset-release/main`. Its `ci:version` command advances the fixed beta group
and updates manifests, changelogs, and the lockfile. Consumed notes remain in
`.changeset/pre/` for the stable release. The version PR requires the same checks
and review as other pull requests.

The release workflow needs `EFFECT_AGENT_APP_ID` and
`EFFECT_AGENT_APP_PRIVATE_KEY`, with the App installed on this repository and
granted Contents and Pull requests write access. App-authored version commits
trigger PR CI and review.

Configure `publish-beta.yml` as each package's npm trusted publisher.
[Publish beta](../.github/workflows/publish-beta.yml) runs after CI succeeds for
the exact current `main` revision. It publishes only when Changesets reports
unpublished versions with no pending changesets. Manual dispatch from `main`
can retry a release. The publisher uses the same Changesets publish action as
Auth and Agent. Its `release:checked-publish` hook runs `vp run ready`, prepares
npm-compatible manifests, and invokes `changeset publish --tag beta`. Changesets
owns registry checks, package publication, Git tags, and GitHub releases. npm
authenticates through OIDC; the GitHub App token authorizes tags and releases.

Use `vp run release:publish --dry-run` to build and inspect publishable packages
without publishing or tagging. Temporary manifest and prerelease-state changes
are restored after the command. Once npm makes a release available,
`vp run release:check --registry` verifies a clean registry consumer.

Pushes to `main` that change docs, the site Worker, package manifests, or build
inputs trigger [Deploy docs](../.github/workflows/deploy-docs.yml). Deployments run
one at a time and check out current `main` when the job starts. Manual dispatch
from `main` can retry a deployment. The workflow needs `CLOUDFLARE_API_TOKEN` with
access to the site Worker and its `yielded.dev` route.

`vp run docs:deploy` builds and deploys locally using [site/wrangler.jsonc](../site/wrangler.jsonc).
When using Wrangler OAuth, unset unrelated `CLOUDFLARE_API_TOKEN` and
`CLOUDFLARE_ACCOUNT_ID` values. Check the deployed home page, a guide, and an asset.
The Worker strips `/sync/` for asset lookup, restores it on redirects, and serves
missing pages with HTTP 404.

## Contributor skills

Shared skills are authored in [danieljvdm/skills](https://github.com/danieljvdm/skills)
and distributed by Dev Kit into `.agents/skills`. Keep copies identical to their
pinned source and retain their origin receipts. Put repository policy in
`AGENTS.md`; upstream reusable improvements before refreshing local edits.
Load exact repository skill paths. Dev Kit is not required to install, check,
build, or run this library.
