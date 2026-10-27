---
"@yielded/sync": minor
---

Preserve action result and rejection types after local admission with
`lease.submit`, a command status stream, passive result waiting, and exact retry.
Restore typed handles with `lease.restore`, which validates retained evidence and
rejects unavailable, quarantined, or mismatched commands.
