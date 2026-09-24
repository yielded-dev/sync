import { it } from "@effect/vitest";
import { Persistence, type PersistenceError } from "@yielded/sync/client";
import { Deferred, Effect, Fiber } from "effect";
import { expect } from "vite-plus/test";

const address = { kind: "counter", id: "room" };
const options = { namespace: "cache-races", actorId: "alice", maxSnapshots: 1 };

// Synchronous callbacks faithfully model AtomicStore's single-record transaction.
const records = () => {
  const values = new Map<string, string>();

  const store: Persistence.AtomicStore = {
    read: (key) => Effect.sync(() => values.get(key)),
    modify: (key, f) =>
      Effect.try({
        try: () => {
          const [result, value] = f(values.get(key));

          if (value === undefined) values.delete(key);
          else values.set(key, value);

          return result;
        },
        catch: Persistence.storageError,
      }),
  };

  return { values, store };
};

const pause = Effect.gen(function* () {
  const started = yield* Deferred.make<void>();
  const release = yield* Deferred.make<void>();

  return {
    started: Deferred.await(started),
    release: Deferred.succeed(release, undefined),
    driver: (store: Persistence.AtomicStore): Persistence.AtomicStore => ({
      read: store.read,
      modify: (key, f) =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.andThen(store.modify(key, f)),
        ),
    }),
  };
});

const reason = <A>(effect: Effect.Effect<A, PersistenceError>) =>
  effect.pipe(Effect.match({ onFailure: (error) => error.reason, onSuccess: () => "Success" }));

it.effect("bounds cache storage when retired writers resume after repeated wipes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const journal = records();
      const cache = records();

      const open = (driver = cache.store) =>
        Persistence.make(options, {
          journal: Effect.succeed(journal.store),
          cache: Effect.succeed(driver),
        });

      for (let generation = 0; generation < 4; generation += 1) {
        const delayed = yield* pause;
        const stale = yield* open(delayed.driver(cache.store));
        const current = yield* open();

        const write = yield* reason(stale.snapshotCache.put(address, { generation })).pipe(
          Effect.forkChild,
        );

        yield* delayed.started;
        yield* current.wipe;
        yield* delayed.release;
        expect(yield* Fiber.join(write)).toBe("StaleGeneration");
        expect(cache.values.size).toBeLessThanOrEqual(1);
        const fresh = yield* open();

        expect(yield* fresh.snapshotCache.get(address)).toBeUndefined();
        yield* fresh.snapshotCache.put(address, { generation: generation + 1 });
        expect(yield* fresh.snapshotCache.get(address)).toEqual({ generation: generation + 1 });
        expect(cache.values.size).toBe(1);
      }
    }),
  ),
);

it.effect.each(["write", "wipe"] as const)(
  "preserves fresh snapshots when an old cache %s completes late",
  (operation) =>
    Effect.scoped(
      Effect.gen(function* () {
        const journal = records();
        const cache = records();
        const delayed = yield* pause;

        const open = (driver = cache.store) =>
          Persistence.make(options, {
            journal: Effect.succeed(journal.store),
            cache: Effect.succeed(driver),
          });

        const stale = yield* open(delayed.driver(cache.store));
        const current = yield* open();

        const completion = yield* reason(
          operation === "write" ? stale.snapshotCache.put(address, "retired") : stale.wipe,
        ).pipe(Effect.forkChild);

        yield* delayed.started;
        if (operation === "write") yield* current.wipe;
        const fresh = yield* open();

        yield* fresh.snapshotCache.put(address, "fresh");
        yield* delayed.release;
        expect(yield* Fiber.join(completion)).toBe(
          operation === "write" ? "StaleGeneration" : "Success",
        );
        expect(yield* fresh.snapshotCache.get(address)).toBe("fresh");
        expect(cache.values.size).toBe(1);
      }),
    ),
);

it.effect("closes failed cache acquisitions while keeping the journal usable", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const journal = records();
      let cacheClosed = false;

      const storage = yield* Persistence.make(options, {
        journal: Effect.succeed(journal.store),
        cache: Effect.gen(function* () {
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              cacheClosed = true;
            }),
          );

          return yield* Persistence.failure("Unavailable", "Unsupported cache format");
        }),
      });

      expect(cacheClosed).toBe(true);
      expect(yield* storage.snapshotCache.get(address)).toBeUndefined();
      expect(yield* reason(storage.snapshotCache.put(address, "snapshot"))).toBe("Unavailable");
      const evidence = { address, commandId: "keep-me", value: { payload: "exact" } };

      yield* storage.intentJournal.transaction((tx) => tx.put(evidence));
      expect(yield* storage.intentJournal.transaction((tx) => tx.get(address, "keep-me"))).toEqual(
        evidence,
      );
    }),
  ),
);
