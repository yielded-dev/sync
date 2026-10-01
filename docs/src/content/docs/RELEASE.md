---
title: Beta release
---

The package family shares one version. The first published beta is
`0.1.0-beta.0` for `@yielded/sync`, `@yielded/sync-platform-cloudflare`,
`@yielded/sync-local-indexeddb`, and `@yielded/sync-local-expo`. Consumers
should pin those exact versions. Package manifests declare the supported Effect
peer range; the Expo adapter also peers on `expo-sqlite@57.0.3`.

This checkout adds `@yielded/sync-platform-effect-cf` and
`@yielded/sync-platform-alchemy-cf` for the next beta. The native host APIs described
here are unreleased and replace the first beta's `Cloudflare.worker` and
`Cloudflare.durableObject` factories. All six packages remain in one fixed version
group. The supported combination is declared in the root catalog and package peer ranges, including
effect-cf 0.51.0 and Alchemy
2.0.0-beta.79; keep transitive Effect packages on the same RC.

| Package                             | Host and validation                                                                                                                                                               |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@yielded/sync`                     | Platform-neutral ESM contracts, server runtime, headless client, and Atom bindings. External consumer typecheck and build use public exports.                                     |
| `@yielded/sync-platform-cloudflare` | Shared protocol and SQLite Durable Object storage, preserving the original format and retry evidence.                                                                             |
| `@yielded/sync-platform-effect-cf`  | Native effect-cf Layer and handlers. Existing workerd checks cover commits, replay, outbox, and socket lifecycle.                                                                 |
| `@yielded/sync-platform-alchemy-cf` | Native Alchemy v2 construction/runtime Effects and HTTP/socket conversion. The counter example uses native Alchemy host APIs.                                                     |
| `@yielded/sync-local-indexeddb`     | Browser IndexedDB. Chromium tests cover reload, concurrent tabs, cache recovery, and journal fencing.                                                                             |
| `@yielded/sync-local-expo`          | Expo 57 native SQLite. File-backed adapter tests and a native iOS restart probe are available. Android execution remains unverified. Expo web uses the IndexedDB adapter instead. |

These beta versions can change public APIs and persisted formats between releases.
Pin exact versions, read release notes before upgrading, and migrate or reconcile
authoritative state, receipts, and unresolved client journals explicitly. There is
no automatic reset of durable retry evidence. Disposable snapshot caches can be
rebuilt. Production adoption still requires application integration and
projection-destination checks.

The [first prerelease](https://github.com/yielded-dev/sync/releases/tag/v0.1.0-beta.0)
records its source revision, validated tarballs, and the passing registry
consumer check.

## Publishing subsequent betas

The original four packages have `publish-beta.yml` registered as their npm trusted
publisher. Configure the same trusted publisher for the two new framework packages
before their first workflow publication. The first beta was published from the CLI and has no CI provenance.
Subsequent beta versions use the manual GitHub workflow from `main`, with npm
OIDC and provenance. The workflow uses the frozen Bun lockfile,
Vite+ `ready` gate, and exact tarballs validated by `release:check`. It accepts an
already published version only when its registry integrity matches the tarball,
verifies a clean registry install, and records the source revision and validation
run in a GitHub prerelease.

A successful beta workflow deploys the documentation from the same source
revision. Configure the repository's `CLOUDFLARE_API_TOKEN` Actions secret with
access to the docs Worker and `yielded.dev` route. If `main` advances before the
deployment starts, the older deployment is skipped; dispatch `Deploy docs` from
current `main` to publish the latest docs. The same dispatch can publish docs
between package releases.

`vp run changeset` records subsequent changes. The fixed version group is in
Changesets beta prerelease mode; `vp run changeset:version` advances all six
versions together. Commit the generated manifests, changelogs, and lockfile before
publishing. Run `vp run ready` locally before handoff.
