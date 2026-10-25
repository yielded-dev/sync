---
"@yielded/sync": patch
---

Add `lease.enqueue` for durable local admission with background delivery.
Retain cached authority identities when admitting offline edits, including after
connection retries are exhausted. Cached state remains provisional and queued
commands cannot be rebound to a replacement authority.
