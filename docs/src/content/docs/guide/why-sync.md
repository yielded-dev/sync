---
title: Why Sync?
---

Sync keeps clients up to date with server state. It tracks versions and handles
retries, optimistic updates, rollbacks, and recovery after disconnects.

## What it takes care of

| Problem                   | What Sync does                                                                                                                      |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Versioning                | Tracks which updates a client has seen and applies new ones in order. Checks that data uses the expected schema version.            |
| Automatic retries         | Retries the same edit after a lost connection. If the server already saved it, Sync returns that result without applying it again.  |
| Client rollbacks          | Lets you show an edit before the server replies. If the server rejects it, Sync removes that preview and gives you the error.       |
| Recovery                  | Brings a reconnecting client up to date with missed changes, or reloads the current state if those changes are no longer available. |
| Reloads and offline edits | With persistence enabled, keeps pending edits across reloads and resumes them when the connection returns.                          |

If a result cannot be recovered, the original edit stays visible for your app to
resolve.

## What you write

You write the actions that change your data, the rules for who can call them,
and how those changes update the client. Your app still owns its UI, conflict
rules, and data migrations.

[Get started](./getting-started.md) with a counter, a Cloudflare Durable Object,
and a React client.
