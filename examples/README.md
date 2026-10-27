# Example

The [React board](list-board/README.md) brings the pieces together: shared
contracts, a Cloudflare SQLite host, optimistic UI, presence, and IndexedDB
persistence.

From the repository root, after `vp install` and `vp run build`:

```sh
vp run sync-list-board-example#dev
```

It runs locally without a Cloudflare account. See its README for the workflow and
demo authentication boundaries.
