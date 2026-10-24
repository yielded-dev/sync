# Examples

Runnable consumers using public Sync entry points. Run commands from the
repository root after `vp install` and `vp run build`.

| Example                                         | Purpose                                             | Run                                                   |
| ----------------------------------------------- | --------------------------------------------------- | ----------------------------------------------------- |
| [Contracts](contracts/README.md)                | Shared schemas, composition, and typed RPC          | `vp run @yielded/example-contracts#start`             |
| [effect-cf counter](cloudflare/README.md)       | Counter and label plugin in a SQLite Durable Object | `vp run sync-cloudflare-example#dev`                  |
| [Alchemy counter](alchemy-cloudflare/README.md) | The same domain with native Alchemy hosting         | `vp run sync-alchemy-cloudflare-example#dev`          |
| [React board](list-board/README.md)             | Optimistic UI, Atom, presence, and IndexedDB        | `vp run sync-list-board-example#dev`                  |
| [Expo persistence](persistence-expo/README.md)  | Native SQLite and process-restart probe             | `vp run @yielded/sync-example-persistence-expo#start` |

Demo authentication must be replaced before deployment. The server examples use
local development services; see each README for setup and constraints.
