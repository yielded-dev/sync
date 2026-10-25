---
"@yielded/sync": minor
"@yielded/sync-persistence": minor
"@yielded/sync-local-indexeddb": minor
"@yielded/sync-local-expo": minor
"@yielded/sync-platform-cloudflare": minor
"@yielded/sync-platform-effect-cf": minor
"@yielded/sync-platform-alchemy-cf": minor
---

Move client persistence implementations to `@yielded/sync-persistence`, reduce the core journal to `load` and atomic `commit`, and require caller-provided server storage. Make plugins, messages, optimistic reducers, and outbox configuration optional; add typed client conveniences and remove unused catalog and history hooks.
