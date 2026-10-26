---
"@yielded/sync": minor
"@yielded/sync-persistence": minor
"@yielded/sync-local-indexeddb": minor
"@yielded/sync-platform-cloudflare": minor
"@yielded/sync-platform-effect-cf": minor
"@yielded/sync-platform-alchemy-cf": minor
---

Host sources with caller-provided storage or Cloudflare platform defaults and shared, typed routes; optional outbox delivery defaults to batches of 10 and a 60-second retry interval.

Simplify client setup and optional plugins, keep IndexedDB and custom persistence separate from core, and remove the Expo adapter in favor of one runnable browser example.
