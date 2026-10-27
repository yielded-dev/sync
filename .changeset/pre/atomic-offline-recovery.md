---
"@yielded/sync": patch
"@yielded/sync-platform-browser": patch
"@yielded/sync-local-indexeddb": patch
---

Publish complete action event batches, admit offline edits through callable Atom `enqueue` bindings, and expose command recovery diagnostics. Enforce one IndexedDB coordinator per namespace, actor, and source through scoped Web Locks; custom persistence must provide ownership or guarantee a single coordinator.
