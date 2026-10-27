# @yielded/sync-persistence

This package provides shared record handling for Sync client storage adapters. Use
[`@yielded/sync-local-indexeddb`](../local-indexeddb/README.md) in browsers or
supply your own storage drivers.

## Custom storage

Implement the core [`ReplicaPersistence` port](../sync/src/client/ReplicaPersistence.ts)
directly, or import `Persistence` from this package and use
`Persistence.make(options, { journal, cache })` with two scoped `AtomicStore`
drivers. Each supplies `read(key)` and `modify(key, callback)`. `modify` must
atomically read the record, apply the synchronous callback, and commit the result.
If it fails, the previous record must remain intact. Drivers also handle namespace
isolation and resource cleanup.

The resulting handle captures `Client.CurrentActor` and provides a journal of
pending commands and a snapshot cache. Core requires only the actor-bound journal,
so a custom implementation may omit the cache. Keep handles alive for the complete
client session and close the session before changing actors.

## Options and recovery

| Option           | Default / meaning                                         |
| ---------------- | --------------------------------------------------------- |
| `namespace`      | Required, application/environment storage boundary        |
| `maxJournalRows` | 4,096 unresolved entries per actor                        |
| `maxSnapshots`   | 128 cached snapshots per actor                            |
| `maxBytes`       | 8 MiB per actor per store, including JSON record overhead |

Bounds must be positive integers. When the journal is full, it refuses new work
and keeps unresolved commands. Unsupported intents retain their identities and
capacity for recovery. Snapshot caches evict the oldest writes first.

Corrupt cache records become misses. Cache failures appear through the client's
`cacheError` without preventing journal use. Unreadable or unsupported journals
remain intact for recovery; never delete one to repair a cache. Close sessions
and reconcile unresolved commands before deleting local data or changing
namespaces. See the [client guide](../../docs/src/content/docs/client.mdx#persistence)
for session ownership and retry behavior.
