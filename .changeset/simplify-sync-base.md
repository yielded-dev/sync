---
"@yielded/sync": minor
"@yielded/sync-persistence": minor
"@yielded/sync-local-indexeddb": minor
"@yielded/sync-platform-cloudflare": minor
"@yielded/sync-platform-effect-cf": minor
"@yielded/sync-platform-alchemy-cf": minor
---

Host sources with caller-provided storage, or use `DurableObject.make(...).provide(...)` with `EffectCfServices` or `AlchemyServices` for Cloudflare defaults, typed routes, and `.Type` instance inference; optional outbox delivery defaults to batches of 10 and a 60-second retry interval.

Bind replicas, typed action atoms, and optional presence to an application's Atom runtime; keep plugins optional and IndexedDB or custom persistence separate from core, and replace the Expo adapter with one runnable browser example.
