# @yielded/sync-platform-browser

## 0.1.0-beta.4

### Patch Changes

- fa5ec1f: Publish complete action event batches, admit offline edits through callable Atom `enqueue` bindings, and expose command recovery diagnostics. Enforce one IndexedDB coordinator per namespace, actor, and source through scoped Web Locks; custom persistence must provide ownership or guarantee a single coordinator.
- Updated dependencies [fa5ec1f]
- Updated dependencies [0111e0c]
- Updated dependencies [df571d8]
- Updated dependencies [ebb37d4]
- Updated dependencies [85664d7]
- Updated dependencies [298f319]
  - @yielded/sync@0.1.0-beta.4

## 0.1.0-beta.3

### Minor Changes

- 6d4a1ae: Bind a browser source with a typed ID, actor, and caller-provided persistence, using default WebSocket and crypto services. Compose callable actions and local state with `binding.fn` generators using the same runtime.
- dae2bab: Expose direct module imports with documented public contracts, defaults, and resource lifetimes. Keep runtime helpers private, including the previously exposed `Server.Slot` and `Server.canonicalJson` internals.

### Patch Changes

- Updated dependencies [dae2bab]
- Updated dependencies [6d4a1ae]
  - @yielded/sync@0.1.0-beta.3
