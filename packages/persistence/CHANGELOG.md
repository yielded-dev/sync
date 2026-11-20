# @yielded/sync-persistence

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
