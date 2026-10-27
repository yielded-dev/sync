import { Action, ProtocolError, Source } from "@yielded/sync";
import { SourceAtom } from "@yielded/sync/atom";
import { Client, ReplicaPersistence, type PersistenceError } from "@yielded/sync/client";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Atom from "effect/reactivity/Atom";
import * as AtomRegistry from "effect/reactivity/AtomRegistry";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import type { Persistence } from "../../../persistence/src/index.ts";

export type Open = (
  options: Persistence.Options,
) => Effect.Effect<Persistence.DurableHandle, PersistenceError, Client.CurrentActor | Scope.Scope>;

const address = { kind: "counter", id: "room/one" } as const;

export const evidence = {
  address,
  commandId: "reserved-id",
  value: { format: 999, exact: ["🙂", 42, null], phase: "ConfirmedAwaitingResult" },
} as const;

const options = (namespace: string) => ({
  namespace,
  maxSnapshots: 1,
  maxJournalRows: 2,
});

const assert = (condition: boolean, message: string) => {
  if (!condition) throw new Error(message);
};

const equal = (actual: unknown, expected: unknown, message: string) =>
  assert(
    actual === undefined || expected === undefined
      ? actual === expected
      : Schema.toEquivalence(Schema.Json)(
          Schema.decodeUnknownSync(Schema.Json)(actual),
          Schema.decodeUnknownSync(Schema.Json)(expected),
        ),
    message,
  );

const failureReason = <A>(effect: Effect.Effect<A, PersistenceError>) =>
  effect.pipe(Effect.match({ onFailure: (error) => error.reason, onSuccess: () => "Success" }));

export const seed = (open: Open, namespace: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* open(options(namespace));

      yield* storage.intentJournal.commit(address, { insert: [evidence], replace: [], remove: [] });
      yield* storage.snapshotCache.put(address, { value: 7 });
    }),
  );

export const restore = (open: Open, namespace: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* open(options(namespace));

      equal(
        (yield* storage.intentJournal.load(address, 10)).find(
          (row) => row.commandId === evidence.commandId,
        ),
        evidence,
        "exact evidence did not survive reopen",
      );
      equal(
        yield* storage.snapshotCache.get(address),
        { value: 7 },
        "snapshot did not survive reopen",
      );

      return "restored";
    }),
  );

export const concurrent = (open: Open, namespace: string, concurrency: 1 | 2 = 2) =>
  Effect.scoped(
    Effect.gen(function* () {
      const a = yield* open(options(namespace));
      const b = yield* open(options(namespace));

      const results = yield* Effect.forEach(
        [a, b],
        (storage, index) =>
          failureReason(
            storage.intentJournal.commit(address, {
              insert: [{ ...evidence, commandId: `writer-${index}` }, evidence],
              replace: [],
              remove: [],
            }),
          ),
        { concurrency },
      );

      equal(
        [...results].sort(),
        ["Conflict", "Success"],
        `concurrent commit results: ${results.join(", ")}`,
      );
      equal(
        (yield* b.intentJournal.load(address, 10)).map((row) => row.commandId).sort(),
        [evidence.commandId, `writer-${results.indexOf("Success")}`],
        "conflicting batch partially committed",
      );

      return "atomic conflict rollback";
    }),
  );

const Counter = Source.make({
  kind: "counter",
  schemaVersion: 1,
  snapshot: Schema.Struct({ value: Schema.Int }),
  event: Schema.Struct({ value: Schema.Int }),
  actions: [Action.make("set", { payload: Schema.Int, success: Schema.Int, error: Schema.Never })],
});

const definition = Client.definition(Counter, {
  applyEvent: (_state, event) => event,
  optimistic: { set: (_state, value) => ({ value }) },
});

const position = { sourceAuthorityGeneration: "00000000-0000-4000-8000-000000000001", cursor: 0 };

const snapshot = {
  _tag: "Snapshot",
  address,
  protocolVersion: 1,
  schemaVersion: 1,
  position,
  snapshot: { source: { value: 0 }, plugins: {} },
} as const;

const unavailable = () => ProtocolError.make({ reason: "Unavailable", message: "offline" });

// Requested shared-store seam: two runtimes must not own the same persisted source concurrently.
export const ownedRuntime = (open: Open, namespace: string) =>
  Effect.gen(function* () {
    const storage = yield* open(options(namespace));

    const client = yield* Client.make(definition, { persistence: { mode: "persistent" } }).pipe(
      Effect.provideService(ReplicaPersistence, storage),
      Effect.provideService(Client.Transport, {
        snapshot: () => Effect.succeed(snapshot),
        subscribe: () => Stream.concat(Stream.succeed(snapshot), Stream.never),
        execute: () => Effect.never,
        result: () => Effect.never,
        publishMessage: () => Effect.void,
      }),
    );

    const source = yield* client.open(address);

    yield* source.ready;

    return { source, storage, admit: source.enqueue(Counter.actions.set, 8) };
  });

export const runtimeSeed = (open: Open, namespace: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* open(options(namespace));
      const saved = yield* Deferred.make<void>();
      let command: Client.EncodedCommand | undefined;

      const transport: Client.Transport = {
        snapshot: () => Effect.succeed(snapshot),
        subscribe: () => Stream.concat(Stream.succeed(snapshot), Stream.never),
        execute: (value) =>
          Effect.sync(() => {
            command = value;
          }).pipe(Effect.andThen(Effect.fail(unavailable()))),
        result: () => Effect.fail(unavailable()),
        publishMessage: () => Effect.void,
      };

      const client = yield* Client.make(definition, {
        persistence: { mode: "persistent" },
      }).pipe(
        Effect.provideService(Client.Transport, transport),
        Effect.provideService(ReplicaPersistence, {
          ...storage,
          snapshotCache: {
            ...storage.snapshotCache,
            put: (target, value) =>
              storage.snapshotCache
                .put(target, value)
                .pipe(Effect.tap(() => Deferred.succeed(saved, undefined))),
          },
        }),
      );

      const source = yield* client.open(address);

      yield* source.ready;
      assert(
        Exit.isFailure(yield* Effect.exit(source.execute(Counter.actions.set, 8))),
        "response loss did not retain uncertainty",
      );
      yield* TestClock.adjust("100 millis");
      yield* Deferred.await(saved);
      equal(
        yield* storage.snapshotCache.get(address),
        snapshot,
        "background flush did not persist authority",
      );
      const rows = yield* storage.intentJournal.load(address, 10);

      equal(rows.length, 1, "lost response did not retain journal");

      return { command, rows };
    }),
  ).pipe(Effect.provide(TestClock.layer()));

export const runtimeRestore = (open: Open, namespace: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* open(options(namespace));
      const rows = yield* storage.intentJournal.load(address, 10);
      let resent: Client.EncodedCommand | undefined;

      const transport: Client.Transport = {
        snapshot: () => Effect.succeed(snapshot),
        subscribe: () => Stream.concat(Stream.succeed(snapshot), Stream.never),
        execute: (command) =>
          Effect.sync(() => {
            resent = command;

            return { _tag: "Succeeded", result: 8, position };
          }),
        result: () => Effect.succeed({ _tag: "Unknown" }),
        publishMessage: () => Effect.void,
      };

      const client = yield* Client.make(definition, {
        persistence: { mode: "persistent" },
      }).pipe(
        Effect.provideService(Client.Transport, transport),
        Effect.provideService(ReplicaPersistence, storage),
      );

      const source = yield* client.open(address);

      yield* source.ready;
      yield* source.retry(rows[0].commandId);

      return {
        resent,
        remaining: yield* storage.intentJournal.load(address, 10),
      };
    }),
  );

// https://github.com/yielded-dev/sync/issues/60: Atom admission must work from an offline cache.
export const atomOffline = (open: Open, namespace: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* open(options(namespace));

      yield* storage.snapshotCache.put(address, snapshot);

      const runtime = Atom.runtime(
        Layer.mergeAll(
          Layer.succeed(Client.CurrentActor, yield* Client.CurrentActor),
          Layer.succeed(ReplicaPersistence, storage),
          Layer.succeed(Client.Transport, {
            snapshot: () => Effect.fail(unavailable()),
            subscribe: () => Stream.fail(unavailable()),
            execute: () => Effect.fail(unavailable()),
            result: () => Effect.fail(unavailable()),
            publishMessage: () => Effect.void,
          }),
        ),
      );

      const binding = SourceAtom.bind(definition, {
        runtime,
        address,
        persistence: { mode: "persistent" },
        retry: { maxAttempts: 1, initialDelay: 1, maxDelay: 1 },
      });

      const registry = yield* Effect.acquireRelease(
        Effect.sync(() => AtomRegistry.make()),
        (value) => Effect.sync(() => value.dispose()),
      );

      yield* AtomRegistry.mount(registry, binding.replica);
      yield* AtomRegistry.toStreamResult(registry, binding.replica).pipe(
        Stream.filter((state) => state.provisional !== undefined && state.connection === "parked"),
        Stream.take(1),
        Stream.runDrain,
      );

      const id = yield* binding.enqueue
        .set(8)
        .pipe(Effect.provideService(AtomRegistry.AtomRegistry, registry));

      const state = yield* AtomRegistry.getResult(registry, binding.replica);

      equal(state.value, { value: 8 }, "offline Atom admission did not apply optimism");
      equal(
        state.pending.map((pending) => pending.commandId),
        [id],
        "admitted command was not observable",
      );
      const rows = yield* storage.intentJournal.load(address, 10);

      equal(rows.length, 1, "admitted command was not durable");

      return { command: rows[0].value };
    }),
  );
