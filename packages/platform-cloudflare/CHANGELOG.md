# @yielded/sync-platform-cloudflare

## 0.1.0-beta.5

### Patch Changes

- 71519bd: Require Effect 4.1 and effect-cf 0.59, and align the runtime companion packages
  with that release.

  The browser runtime provides `SecureRandom.SecureRandom` from `effect/crypto` in
  place of the removed `Crypto.Crypto` service. Use its `uuidV4()` method instead of
  `randomUUIDv4()` in application workflows.

- Updated dependencies [71519bd]
  - @yielded/sync@0.1.0-beta.5

## 0.1.0-beta.4

### Patch Changes

- ebb37d4: Upgrade private server and plugin state atomically with `stateVersion` and `migrate`, preserving public snapshots, authority, receipts, replay, and queued outbox work. Support these upgrades in native SQLite storage.
- Updated dependencies [fa5ec1f]
- Updated dependencies [0111e0c]
- Updated dependencies [df571d8]
- Updated dependencies [ebb37d4]
- Updated dependencies [85664d7]
- Updated dependencies [298f319]
  - @yielded/sync@0.1.0-beta.4

## 0.1.0-beta.3

### Minor Changes

- dae2bab: Expose direct module imports with documented public contracts, defaults, and resource lifetimes. Keep runtime helpers private, including the previously exposed `Server.Slot` and `Server.canonicalJson` internals.
- 6d4a1ae: Host sources with caller-provided storage, or use `DurableObject.make(...).provide(...)` with `EffectCfServices` or `AlchemyServices` for Cloudflare defaults, typed routes, and `.Type` instance inference; optional outbox delivery defaults to batches of 10 and a 60-second retry interval.

  Bind replicas, callable action atoms, generator functions, and optional presence to an application's Atom runtime; keep plugins optional and IndexedDB or custom persistence separate from core, and replace the Expo adapter with one runnable browser example.

### Patch Changes

- Updated dependencies [dae2bab]
- Updated dependencies [6d4a1ae]
  - @yielded/sync@0.1.0-beta.3

## 0.1.0-beta.2

### Patch Changes

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

- b551c7c: Expose the shared outbox alarm as a reusable Effect. Framework adapters continue
  providing their native alarm callbacks.

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
  - @yielded/sync@0.1.0-beta.1

## 0.1.0-beta.0

### Minor Changes

- Publish the first beta with built ESM and TypeScript declarations for the shared runtime, Cloudflare host, and browser and Expo persistence adapters.

### Patch Changes

- Updated dependencies
  - @yielded/sync@0.1.0-beta.0
