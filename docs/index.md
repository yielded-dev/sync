---
layout: home

hero:
  name: Yielded Sync
  text: Realtime state, with receipts.
  tagline: Effect-native contracts, an authoritative server, and scoped clients that keep the same command identity through retries.
  actions:
    - theme: brand
      text: Get started
      link: /guide/getting-started
    - theme: alt
      text: Explore the API
      link: /reference/packages

features:
  - title: One source of authority
    details: Commit state, durable events, exact outcomes, and outbox obligations together. Replay stays ordered and gaps trigger recovery.
  - title: Honest optimistic state
    details: Clients expose provisional edits and declared rejections while retaining unresolved commands for exact retry.
  - title: Explicit host boundaries
    details: Keep your domain model and authorization in the app. Add Cloudflare hosting and browser or Expo persistence only where needed.
---

## Published beta

The four packages share version **`{{SYNC_VERSION}}`**. Pin exact versions while the
API and persisted formats are in beta. Start with the [installation guide](./guide/getting-started.md)
and choose the adapters for your host.

| Package                             | Use it for                                                      |
| ----------------------------------- | --------------------------------------------------------------- |
| `@yielded/sync`                     | Shared contracts, server and client runtimes, and Atom bindings |
| `@yielded/sync-platform-cloudflare` | Cloudflare Workers and SQLite Durable Objects                   |
| `@yielded/sync-local-indexeddb`     | Browser persistence                                             |
| `@yielded/sync-local-expo`          | Expo SQLite persistence                                         |

The [consumer example](./api/consumer.md) assembles the public entry points into
a counter and a reusable label capability. The [release guide](./RELEASE.md)
describes supported hosts and the beta compatibility policy.
