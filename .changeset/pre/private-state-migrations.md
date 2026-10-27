---
"@yielded/sync": patch
"@yielded/sync-platform-cloudflare": patch
---

Upgrade private server and plugin state atomically with `stateVersion` and `migrate`, preserving public snapshots, authority, receipts, replay, and queued outbox work. Support these upgrades in native SQLite storage.
