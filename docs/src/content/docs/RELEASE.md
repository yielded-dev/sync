---
title: Releases and upgrades
---

Sync packages share one version. Pin the same exact version across the packages
you use and satisfy the peer ranges in their manifests. Release history and
version-specific changes live in [GitHub releases](https://github.com/yielded-dev/sync/releases).

Before upgrading, read the release notes and account for each kind of stored data:

| Data                          | Upgrade contract                                                                                                                           |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Private server state          | Use `stateVersion` and `migrate` on `Server.make` or `Server.plugin`. All affected slots update atomically.                                |
| Snapshot cache                | Incompatible snapshots are discarded and fetched again.                                                                                    |
| Event history                 | Keep public event codecs and their interpretation compatible with retained events.                                                         |
| Pending commands and receipts | Keep command and result codecs unchanged; retain original requests, identities, and results. Unsupported pending commands are quarantined. |
| Outbox                        | Keep delivery codecs compatible with queued payloads and preserve obligation IDs.                                                          |

Private-state revisions default to `0`, including existing unversioned state.
Migration runs on the first source operation; failures roll back and downgrades
are rejected. It preserves the public snapshot **value**, authority, and cursor.
Use ordinary action commits for public-data changes. See the
[server example](api/consumer.md).

Replace old server instances when deploying a migration. Keep `Source.schemaVersion`
and public codecs unchanged: changing that version is not a state migration, and
breaking public-contract upgrades are not supported by this mechanism. Even adding
a decoding default to a command payload can change retry identity. Native SQLite
supports private-state migrations; custom storage adapters must implement
`SourceTransaction.migrateState` and retain state revisions.

Keep source addresses, Durable Object identities, and storage namespaces stable
when retaining existing data. Changing a namespace creates a different storage
location; it does not migrate the old one. Resolve or migrate outstanding work
before switching.

Contributors publish through the repository's validated release workflow. See
[release operations](https://github.com/yielded-dev/sync/blob/main/docs/TOOLCHAIN.md#releases-and-docs-deployment)
for setup and commands.
