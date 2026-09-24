# Beta release

The four packages share one version. The first beta candidate is
`0.1.0-beta.0` for `@yielded/sync`, `@yielded/sync-platform-cloudflare`,
`@yielded/sync-local-indexeddb`, and `@yielded/sync-local-expo`. Kommunikasie
should pin those exact versions after they resolve from npm. The shared Effect
peer is `effect@4.0.0-rc.112`; the Expo adapter also peers on
`expo-sqlite@57.0.3`.

| Package                             | Host and validation                                                                                                                                                               |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@yielded/sync`                     | Platform-neutral ESM contracts, server runtime, headless client, and Atom bindings. External consumer typecheck and build use public exports.                                     |
| `@yielded/sync-platform-cloudflare` | Cloudflare Workers with SQLite Durable Objects. Workerd tests cover commits, replay, outbox, and socket lifecycle.                                                                |
| `@yielded/sync-local-indexeddb`     | Browser IndexedDB. Chromium tests cover reload, concurrent tabs, cache recovery, and journal fencing.                                                                             |
| `@yielded/sync-local-expo`          | Expo 57 native SQLite. File-backed adapter tests and a native iOS restart probe are available. Android execution remains unverified. Expo web uses the IndexedDB adapter instead. |

These beta versions can change public APIs and persisted formats between releases.
Pin exact versions, read release notes before upgrading, and migrate or reconcile
authoritative state, receipts, and unresolved client journals explicitly. There is
no automatic reset of durable retry evidence. Disposable snapshot caches can be
rebuilt. Production slide-deck adoption still requires the product integration
and destination projection checks in the [pilot](slide-deck-pilot.md).

## Publishing

The first publish requires an npm account authorized to create public packages
under `@yielded`. From the committed `main` revision, run `vp run ready`, then
publish its validated tarballs in dependency order:

```sh
npm publish .release/sync.tgz --access public --tag beta
npm publish .release/platform-cloudflare.tgz --access public --tag beta
npm publish .release/local-indexeddb.tgz --access public --tag beta
npm publish .release/local-expo.tgz --access public --tag beta
```

After all four packages exist, register `publish-beta.yml` as their trusted
publisher. Run this for each package name:

```sh
npm trust github @yielded/sync --file publish-beta.yml --repo yielded-dev/sync --allow-publish
```

npm requires interactive 2FA for this operation.
Then dispatch the workflow from `main`. The workflow uses the frozen Bun lockfile,
Vite+ `ready` gate, and exact tarballs validated by `release:check`. It accepts an
already published version only when its registry integrity matches the tarball,
verifies a clean registry install, and records the source revision and validation
run in a GitHub prerelease. Subsequent beta versions publish through npm OIDC with
provenance. The initial CLI publish has no CI provenance.

`vp run changeset` records subsequent changes. The fixed version group is in
Changesets beta prerelease mode; `vp run changeset:version` advances all four
versions together. Commit the generated manifests, changelogs, and lockfile before
publishing. Run `vp run ready` locally before handoff.
