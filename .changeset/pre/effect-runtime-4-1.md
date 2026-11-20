---
"@yielded/sync": patch
"@yielded/sync-persistence": patch
"@yielded/sync-platform-browser": patch
"@yielded/sync-platform-cloudflare": patch
"@yielded/sync-platform-effect-cf": patch
"@yielded/sync-platform-alchemy-cf": patch
"@yielded/sync-local-indexeddb": patch
---

Require Effect 4.1 and effect-cf 0.59, and align the runtime companion packages
with that release.

The browser runtime provides `SecureRandom.SecureRandom` from `effect/crypto` in
place of the removed `Crypto.Crypto` service. Use its `uuidV4()` method instead of
`randomUUIDv4()` in application workflows.
