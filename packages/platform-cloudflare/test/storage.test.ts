import { SqliteStorage } from "@yielded/sync-platform-cloudflare";
import { SourceStorage, type OutboxRecord, type StoredHead } from "@yielded/sync/server";
import { env, reset, runInDurableObject, evictDurableObject } from "cloudflare:test";
import { Deferred, Effect, Fiber, Layer } from "effect";
import { DurableObjectState } from "effect-cf";
import { afterEach, expect, it } from "vite-plus/test";

import type { CounterObject } from "../../../examples/cloudflare/src/worker.ts";

const bindings = env as { COUNTERS: DurableObjectNamespace<CounterObject> };
const stub = () => bindings.COUNTERS.getByName("storage-proof");

const initial: StoredHead = {
  address: { kind: "counter", id: "storage-proof" },
  schemaVersion: 1,
  position: { sourceAuthorityGeneration: "6d5f7444-dba4-4a4c-a343-ce7e7dac6e88", cursor: 0 },
  state: { value: 0 },
};

const usingStorage = <A, E>(body: (storage: SourceStorage["Service"]) => Effect.Effect<A, E>) =>
  runInDurableObject(stub(), (_instance, state) =>
    Effect.runPromise(
      Effect.flatMap(SourceStorage, body).pipe(
        Effect.provide(
          SqliteStorage.layer({
            namespace: "counter-v1",
            replayWindow: 4,
            maxPendingOutbox: 100,
          }).pipe(
            Layer.provide(
              Layer.succeed(
                DurableObjectState.DurableObjectState,
                DurableObjectState.fromDurableObjectState(state),
              ),
            ),
          ),
        ),
        Effect.scoped,
      ),
    ),
  );

const plan = (outbox: ReadonlyArray<OutboxRecord>) => ({
  expected: initial.position,
  head: { ...initial, position: { ...initial.position, cursor: 1 }, state: { value: 1 } },
  events: [
    {
      position: { ...initial.position, cursor: 1 },
      command: { actorId: "alice", commandId: "work" },
      event: { value: 1 },
    },
  ],
  receipt: {
    ...initial.position,
    actorId: "alice",
    commandId: "work",
    fingerprint: "a".repeat(64),
    command: { value: 1 },
    outcome: { _tag: "Succeeded", result: 1 },
  },
  outbox,
});

afterEach(() => reset());

it("rolls back a transaction interrupted after all four writes", async () => {
  await usingStorage(
    Effect.fn(function* (storage) {
      yield* storage.transaction((tx) => tx.initialize(initial));
      const written = yield* Deferred.make<void>();

      const fiber = yield* storage
        .transaction(
          Effect.fn(function* (tx) {
            yield* tx.commit(
              plan([
                {
                  id: "work",
                  position: initial.position,
                  payload: { value: 1 },
                  attempts: 0,
                  availableAtMillis: 4_000_000_000_000,
                },
              ]),
            );
            yield* Deferred.succeed(written, undefined);

            return yield* Effect.never;
          }),
        )
        .pipe(Effect.forkChild);

      yield* Deferred.await(written);
      yield* Fiber.interrupt(fiber);
      expect(yield* storage.transaction((tx) => tx.readHead)).toEqual(initial);
      expect(yield* storage.nextOutboxTime).toBeUndefined();
      expect(yield* storage.transaction((tx) => tx.readEvents(initial.position, 4))).toEqual([]);
      expect(
        yield* storage.transaction((tx) =>
          tx.readReceipt({ ...initial.position, actorId: "alice", commandId: "work" }),
        ),
      ).toBeUndefined();
    }),
  );
});

it("recovers leased outbox evidence after eviction and rejects a stale acknowledgement", async () => {
  const due = 4_000_000_000_000;

  const original = await usingStorage(
    Effect.fn(function* (storage) {
      yield* storage.transaction((tx) => tx.initialize(initial));
      yield* storage.transaction((tx) =>
        tx.commit(
          plan([
            {
              id: "stable-obligation",
              position: initial.position,
              payload: { value: 1 },
              attempts: 0,
              availableAtMillis: due,
            },
          ]),
        ),
      );
      const claimed = yield* storage.claimOutbox(due, 100, 1);

      expect(yield* storage.claimOutbox(due + 1, 100, 1)).toEqual([]);

      return claimed[0];
    }),
  );

  await evictDurableObject(stub());
  await usingStorage(
    Effect.fn(function* (storage) {
      const [recovered] = yield* storage.claimOutbox(due + 101, 100, 1);

      expect(recovered).toEqual({ ...original, attempts: 2, availableAtMillis: due + 201 });
      yield* storage.settleOutbox(original, { _tag: "Delivered" });
      expect(yield* storage.nextOutboxTime).toBe(due + 201);
      yield* storage.settleOutbox(recovered, { _tag: "Delivered" });
      expect(yield* storage.nextOutboxTime).toBeUndefined();
    }),
  );
});
