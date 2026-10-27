---
"@yielded/sync": minor
---

Add `lease.transitions({ capacity })` for bounded, ordered observation of local
admissions, complete live commits, command outcomes, and replica status. Each
subscription starts with a captured baseline and resets explicitly on overflow
or resynchronization. Slow observers do not block replication or command
admission; subscriptions share their lease's lifetime.
