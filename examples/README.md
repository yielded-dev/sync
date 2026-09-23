# Examples

Runnable library consumers belong in this workspace directory. They must depend on
public `@yielded/sync` entry points and selected adapters.

- [Contracts](contracts/README.md): shared codecs and external type proofs.
- [Cloudflare](cloudflare/README.md): runnable counter authority and label plugin,
  native RPC, SQLite receipts/outbox, and hibernating subscriptions.

- [Expo persistence](persistence-expo/README.md): native SQLite transaction and
  process-restart recovery probe.

The standalone consumer will exercise convergence, rejection rollback,
lost-response retry, reconnect, and reload/restart recovery before the first beta.
