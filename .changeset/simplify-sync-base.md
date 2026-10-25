---
"@yielded/sync": minor
"@yielded/sync-persistence": minor
"@yielded/sync-platform-cloudflare": minor
"@yielded/sync-platform-effect-cf": minor
"@yielded/sync-platform-alchemy-cf": minor
---

Keep core persistence to journal and optional cache ports, and select browser or Expo storage from `@yielded/sync-persistence/indexeddb` or `/expo`. Supply server storage explicitly, omit unused plugin/message/reducer/outbox configuration, and use the smaller client API without catalog, history, manual hydration, or persistence-management hooks.
