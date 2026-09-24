# @yielded/sync-local-expo

Scoped Expo SQLite persistence for Effect Sync clients on iOS and Android.
`ExpoSqlite.open({ namespace, actorId, directory? })` acquires a durable handle;
`ExpoSqlite.layer(options)` provides it as `ReplicaPersistence`. Supply that handle
to `Client.make` with `{ mode: "persistent", storage }`. Keep its scope alive for
the authenticated actor, and await `client.flush` from the application's background
lifecycle. Journal admission is durable before transport and does not await a flush.

The application owns namespace, identity changes, logout deletion and AppState
subscriptions. Closing the scope closes connections and retains data. `purgeSource`
and `wipe` are explicit destructive operations; reopen after a wipe.

The adapter uses Expo SQLite 57's async API and private connections
(`useNewConnection: true`). Each connection serializes operations, with
`BEGIN IMMEDIATE`, a five-second busy timeout, and `synchronous = FULL`.
Transactions finish or roll back before interruption releases the connection.

## Format and recovery

`databaseNames(namespace)` returns
`yielded-sync-<encodeURIComponent(namespace)>-journal.sqlite` and `-cache.sqlite`.
The journal uses `PRAGMA user_version = 1` and
`records(key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)`. The cache uses
`PRAGMA user_version = 2` and a `snapshots` table with the same columns.
The default directory is Expo's document SQLite directory. The databases are
separate so cache recovery never drops the journal file.

Schema JSON records, keys, capacity limits, snapshot metadata, revision conflicts,
quarantine and generation fencing follow the portable
[persistence format](../local-indexeddb/README.md#format-and-recovery). Default
limits are 4,096 journal rows, 128 snapshots and 8 MiB per actor per store. Writes
operate on one bounded actor document. Independent connections/processes serialize
at SQLite's database lock and check the durable revision and generation.

Malformed cache records can be discarded and replaced. An unavailable cache opens
cacheless: reads miss and saves/flush report failure while journal operations remain
available. Opening the known cache version 1 atomically drops its legacy `records`
table, creates `snapshots`, and advances its version to 2. This discards only
disposable cache data and leaves the journal unchanged. The distinct table name
prevents already-open old connections from restoring legacy cache rows. Neither
database file is deleted automatically, and the journal is never reset. Unknown
physical versions, nonempty unversioned databases and undecodable journals require
explicit recovery or migration and remain untouched. Unknown intent formats retain
their exact JSON and reserved identities for client quarantine. Wipe rotates the
journal fence first; cleanup failure can be reported after invalidation. Changing
namespace is not migration: reconcile old unresolved evidence before switching.

`vp run test` verifies real file-backed SQLite transactions through a Node SQLite
implementation of the Expo API subset. It verifies SQL and recovery behavior, not
the native bridge. The [Expo persistence probe](../../examples/persistence-expo/README.md)
runs the same public-contract scenarios through actual Expo SQLite and supports
process termination/relaunch proof. Expo web persistence is not a supported target;
use the IndexedDB package for browsers.
