---
title: Why Sync?
---

You click Save. The server accepts the edit, but the connection drops before the
reply gets back. Retrying should not apply the same edit twice, and the UI still
needs to find out whether it succeeded.

Sync handles that coordination. You define your data and the changes people can
make; it keeps clients caught up with the server.

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
