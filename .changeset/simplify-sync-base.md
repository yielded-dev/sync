---
"@yielded/sync": minor
"@yielded/sync-persistence": minor
"@yielded/sync-local-indexeddb": minor
"@yielded/sync-local-expo": minor
"@yielded/sync-platform-cloudflare": minor
"@yielded/sync-platform-effect-cf": minor
"@yielded/sync-platform-alchemy-cf": minor
---

Expose platform-independent server coordination and RPC handlers from `@yielded/sync/server`, with application-provided `SourceStorage` and optional outbox delivery. Construct Durable Objects directly from server definitions through the optional Effect CF or Alchemy integrations, with native SQLite and crypto defaults and replaceable platform services.

Keep core persistence to journal and optional cache ports. Browser and Expo storage remain in `@yielded/sync-local-indexeddb` and `@yielded/sync-local-expo`, backed by shared `@yielded/sync-persistence` policy. Applications can also implement the core ports directly.

Omit unused plugin, message, reducer, and outbox configuration, and use the smaller client API without catalog, history, manual hydration, or persistence-management hooks.
