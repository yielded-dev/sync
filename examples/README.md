# Example

The [React board](list-board/README.md) is the runnable integration: shared
contracts, a Cloudflare SQLite host, optimistic UI, an optional plugin, presence,
and IndexedDB persistence.

From the repository root, after `vp install` and `vp run build`:

```sh
vp run sync-list-board-example#dev
```

It runs locally without a Cloudflare account. See its README for the workflow and
demo authentication boundaries.
