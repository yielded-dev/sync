# @yielded/sync-platform-effect-cf

## 0.1.0-beta.4

### Patch Changes

- Updated dependencies [fa5ec1f]
- Updated dependencies [0111e0c]
- Updated dependencies [df571d8]
- Updated dependencies [ebb37d4]
- Updated dependencies [85664d7]
- Updated dependencies [298f319]
  - @yielded/sync@0.1.0-beta.4
  - @yielded/sync-platform-cloudflare@0.1.0-beta.4

## 0.1.0-beta.3

### Minor Changes

- dae2bab: Expose direct module imports with documented public contracts, defaults, and resource lifetimes. Keep runtime helpers private, including the previously exposed `Server.Slot` and `Server.canonicalJson` internals.
- 6d4a1ae: Host sources with caller-provided storage, or use `DurableObject.make(...).provide(...)` with `EffectCfServices` or `AlchemyServices` for Cloudflare defaults, typed routes, and `.Type` instance inference; optional outbox delivery defaults to batches of 10 and a 60-second retry interval.

  Bind replicas, callable action atoms, generator functions, and optional presence to an application's Atom runtime; keep plugins optional and IndexedDB or custom persistence separate from core, and replace the Expo adapter with one runnable browser example.

### Patch Changes

- 4bafbc9: Require effect-cf 0.58 for the native effect-cf adapter.
- cfae88b: Use Alchemy's separate `@alchemy.run/cloudflare` provider package by installing it
  alongside the matching Alchemy release, starting with `2.0.0-beta.84`.

  Support `effect-cf` 0.58 alongside the existing supported releases.

- Updated dependencies [dae2bab]
- Updated dependencies [6d4a1ae]
  - @yielded/sync@0.1.0-beta.3
  - @yielded/sync-platform-cloudflare@0.1.0-beta.3

## 0.1.0-beta.2

### Patch Changes

- d0c336b: Support effect-cf 0.57 alongside 0.53.
- @yielded/sync-platform-cloudflare@0.1.0-beta.2
  - @yielded/sync@0.1.0-beta.2

## 0.1.0-beta.1

### Minor Changes

- 40d7ebd: Add native effect-cf and Alchemy v2 Cloudflare adapters over one shared SQLite and
  RPC runtime. Applications construct their own Workers and Durable Objects and own
  routing, authentication, typed bindings, and service composition. Replace
  `Cloudflare.worker` and `Cloudflare.durableObject` with each framework's constructors
  and the new adapter's `make` and `prepareRequest` operations. Applications call
  their typed stubs directly with the prepared native request. Preserve object names,
  storage namespaces, authority generations, receipts, and unresolved outbox records
  when migrating. Update the Effect peer compatibility and consume current effect-cf and Alchemy releases.

  Use Effect RpcServer for HTTP and WebSocket procedure dispatch, codecs, typed
  responses, stream acknowledgements, and interruption. effect-cf supplies its native
  DurableObjectRpcWebSocket transport; Alchemy uses a Cloudflare RpcServer.Protocol
  bridge. Sync retains source ordering, bounded publication, durable retries, and
  cursor recovery. Provide source-operation services with Server.provide; instance
  Layers own the long-lived RPC server.

### Patch Changes

- 3100667: Require the stable Effect release and use its stable module paths. Update the native
  Cloudflare adapters to Alchemy 2.0.0-beta.80 and effect-cf 0.53.0.
- 4e088f2: Update the required Effect release and align its runtime companion packages with
  the root catalog.
- Updated dependencies [40d7ebd]
- Updated dependencies [3100667]
- Updated dependencies [40d7ebd]
- Updated dependencies [fdf06c3]
- Updated dependencies [4e088f2]
- Updated dependencies [b551c7c]
  - @yielded/sync@0.1.0-beta.1
  - @yielded/sync-platform-cloudflare@0.1.0-beta.1
