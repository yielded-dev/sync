import { Action, ProtocolError, Source } from "@yielded/sync";
import { Client, type Persistence, type PersistenceError } from "@yielded/sync/client";
import { Deferred, Effect, Exit, Fiber, Schema, type Scope, Stream } from "effect";
import * as TestClock from "effect/testing/TestClock";

export type Open = (
  options: Persistence.Options,
) => Effect.Effect<Persistence.DurableHandle, PersistenceError, Scope.Scope>;

const address = { kind: "counter", id: "room/one" } as const;

export const evidence = {
  address,
  commandId: "reserved-id",
  value: { format: 999, exact: ["🙂", 42, null], phase: "ConfirmedAwaitingResult" },
} as const;

const options = (namespace: string) => ({
  namespace,
  actorId: "alice",
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

      yield* storage.intentJournal.transaction((tx) => tx.put(evidence));
      yield* storage.snapshotCache.put(address, { value: 7 });
    }),
  );

export const restore = (open: Open, namespace: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* open(options(namespace));

      equal(
        yield* storage.intentJournal.transaction((tx) => tx.get(address, evidence.commandId)),
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

export const exercise = (open: Open, namespace: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const config = options(namespace);
      const first = yield* open(config);
      const second = yield* open(config);

      yield* first.intentJournal.transaction((tx) => tx.put(evidence));

      const entered = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();

      const writer = yield* first.intentJournal
        .transaction((tx) =>
          Effect.gen(function* () {
            yield* tx.replace({ ...evidence, value: "late" });
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(resume);
          }),
        )
        .pipe(Effect.forkScoped);

      yield* Deferred.await(entered);
      yield* second.wipe;
      const fresh = yield* open(config);

      yield* fresh.intentJournal.transaction((tx) =>
        tx.put({ ...evidence, value: "new generation" }),
      );
      yield* Deferred.succeed(resume, undefined);
      equal(
        yield* failureReason(Fiber.join(writer)),
        "StaleGeneration",
        "delayed writer crossed a wipe",
      );
      equal(
        yield* fresh.intentJournal.transaction((tx) => tx.get(address, evidence.commandId)),
        { ...evidence, value: "new generation" },
        "late writer destroyed new evidence",
      );

      return "generation fencing";
    }),
  );

export const concurrent = (open: Open, namespace: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const a = yield* open(options(namespace));
      const b = yield* open(options(namespace));
      const entered = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();

      const pending = yield* a.intentJournal
        .transaction((tx) =>
          Effect.gen(function* () {
            yield* tx.put(evidence);
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(resume);
          }),
        )
        .pipe(Effect.forkScoped);

      yield* Deferred.await(entered);
      yield* b.intentJournal.transaction((tx) => tx.put({ ...evidence, commandId: "winner" }));
      yield* Deferred.succeed(resume, undefined);
      equal(
        yield* failureReason(Fiber.join(pending)),
        "Conflict",
        "concurrent commit lost a writer",
      );
      equal(
        (yield* b.intentJournal.transaction((tx) => tx.scan(address, 10))).map(
          (row) => row.commandId,
        ),
        ["winner"],
        "conflicting transaction partially committed",
      );

      const cancelled = yield* a.intentJournal
        .transaction((tx) =>
          Effect.gen(function* () {
            yield* tx.put(evidence);

            return yield* Effect.never;
          }),
        )
        .pipe(Effect.forkScoped);

      yield* Fiber.interrupt(cancelled);
      equal(
        yield* b.intentJournal.transaction((tx) => tx.get(address, evidence.commandId)),
        undefined,
        "interruption committed a staged write",
      );

      return "conflict and interruption rollback";
    }),
  );

const Counter = Source.make({
  kind: "counter",
  plugins: [],
  schemaVersion: 1,
  snapshot: Schema.Struct({ value: Schema.Int }),
  event: Schema.Struct({ value: Schema.Int }),
  message: Schema.Never,
  actions: [Action.make("set", { payload: Schema.Int, success: Schema.Int, error: Schema.Never })],
});

const definition = Client.definition(Counter, {
  plugins: [],
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
        actorId: "alice",
        transport,
        persistence: {
          mode: "persistent",
          storage: {
            ...storage,
            snapshotCache: {
              ...storage.snapshotCache,
              put: (target, value) =>
                storage.snapshotCache
                  .put(target, value)
                  .pipe(Effect.tap(() => Deferred.succeed(saved, undefined))),
            },
          },
        },
      });

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
      const rows = yield* storage.intentJournal.transaction((tx) => tx.scan(address, 10));

      equal(rows.length, 1, "lost response did not retain journal");

      return { command, rows };
    }),
  ).pipe(Effect.provide(TestClock.layer()));

export const runtimeRestore = (open: Open, namespace: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const storage = yield* open(options(namespace));
      const rows = yield* storage.intentJournal.transaction((tx) => tx.scan(address, 10));
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
        actorId: "alice",
        transport,
        persistence: { mode: "persistent", storage },
      });

      const source = yield* client.open(address);

      yield* source.ready;
      yield* source.retry(rows[0].commandId);

      return {
        resent,
        remaining: yield* storage.intentJournal.transaction((tx) => tx.scan(address, 10)),
      };
    }),
  );
