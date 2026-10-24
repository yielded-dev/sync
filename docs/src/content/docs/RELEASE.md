---
title: Releases and upgrades
---

Sync packages share one version. Pin the same exact version across the packages
you use and satisfy the peer ranges in their manifests. Release history and
version-specific changes live in [GitHub releases](https://github.com/yielded-dev/sync/releases).

The beta API and persisted formats may change between releases. Before upgrading,
read the release notes and plan any required reconciliation of authoritative
state, receipts, outbox obligations, and unresolved client journals. Sync does
not automatically reset durable retry evidence. Snapshot caches can be rebuilt.

Keep source addresses, Durable Object identities, and storage namespaces stable
when retaining existing data. Changing a namespace creates a different storage
location; it does not migrate the old one. Resolve or migrate outstanding work
before switching.

Contributors publish through the repository's validated release workflow. See
[release operations](https://github.com/yielded-dev/sync/blob/main/docs/TOOLCHAIN.md#releases-and-docs-deployment)
for setup and commands.
