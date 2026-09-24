# Examples

Runnable library consumers belong in this workspace directory. They must depend on
public `@yielded/sync` entry points and selected adapters.

- [Contracts](contracts/README.md): shared codecs and external type proofs.
- [Cloudflare](cloudflare/README.md): runnable counter authority and label plugin,
  native RPC, SQLite receipts/outbox, and hibernating subscriptions.
- [List and board](list-board/README.md): independent two-client consumer with typed
  actions, optimistic reducers, presence, and browser persistence assembly.

- [Expo persistence](persistence-expo/README.md): native SQLite transaction and
  process-restart recovery probe.

The list and board consumer exercises convergence, rejection rollback, lost-response
retry, and reconnect against a SQLite Worker. The adapter suites and native probe
exercise browser reload and mobile restart storage boundaries. The
[slide-deck pilot](../docs/slide-deck-pilot.md) maps the public API to the product
source before broad adoption.
