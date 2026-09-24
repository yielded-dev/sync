import { it } from "@effect/vitest";
import { Action, Plugin, ProtocolError, Source } from "@yielded/sync";
import { Client, memory, type JournalRow, type PersistenceHandle } from "@yielded/sync/client";
import { DateTime, Deferred, Effect, Exit, Fiber, Queue, Schema, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { expect } from "vite-plus/test";

const Label = Plugin.make("label", {
  snapshot: Schema.Struct({ text: Schema.String }),
  event: Schema.Struct({ text: Schema.String }),
  message: Schema.Never,
  actions: [
    Action.make("rename", {
      payload: Schema.Struct({ text: Schema.String }),
      success: Schema.String,
      error: Schema.Never,
    }),
  ],
});

const Counter = Source.make({
  kind: "counter",
  schemaVersion: 1,
  snapshot: Schema.Struct({ value: Schema.Finite }),
  event: Schema.Struct({ delta: Schema.Finite }),
  message: Schema.Struct({ editing: Schema.Boolean }),
  actions: [
    Action.make("add", {
      payload: Schema.Struct({ amount: Schema.Finite }),
      success: Schema.Struct({ before: Schema.Finite, after: Schema.Finite }),
      error: Schema.Never,
    }),
  ],
  plugins: [Label],
});

const label = Client.plugin(Label, {
  applyEvent: (_state, event) => event,
  optimistic: { rename: (_state, payload) => payload },
});

const definition = Client.definition(Counter, {
  applyEvent: (state, event) => ({ value: state.value + event.delta }),
  optimistic: { add: (state, payload) => ({ value: state.value + payload.amount }) },
  plugins: [label],
});

const address = Counter.address("demo");
const generation = "00000000-0000-4000-8000-000000000001";
const replacement = "00000000-0000-4000-8000-000000000002";

const position = (cursor: number, sourceAuthorityGeneration = generation) => ({
  cursor,
  sourceAuthorityGeneration,
});

const header = { protocolVersion: 1 as const, address, schemaVersion: 1 as const };

const snapshot = (cursor: number, value: number, authority = generation) => ({
  ...header,
  _tag: "Snapshot" as const,
  position: position(cursor, authority),
  snapshot: { source: { value }, plugins: { label: { text: "original" } } },
});

const event = (
  cursor: number,
  delta: number,
  command?: { actorId: string; commandId: string },
) => ({
  ...header,
  _tag: "Event" as const,
  event: {
    position: position(cursor),
    event: { namespace: "$source", payload: { delta } },
    ...(command === undefined ? {} : { command }),
  },
});

const unavailable = () => ProtocolError.make({ reason: "Unavailable", message: "Response lost" });
const tick = Effect.yieldNow;

const server = Effect.sync(() => {
  const queues = new Set<Queue.Queue<Schema.Json, ProtocolError>>();
  const ledger = new Map<string, Schema.Json>();
  const calls: Array<Client.EncodedCommand> = [];
  const lookups: Array<Client.EncodedCommand> = [];
  let cursor = 0;
  let value = 0;
  let dropResponse = false;
  let lookupMode: "normal" | "Expired" = "normal";
  let beforeExecute: Effect.Effect<void> = Effect.void;

  const publish = (frame: Schema.Json) =>
    Effect.forEach(queues, (queue) => Queue.offer(queue, frame)).pipe(Effect.asVoid);

  const transport: Client.Transport = {
    snapshot: () => Effect.sync(() => snapshot(cursor, value)),
    subscribe: () =>
      Stream.unwrap(
        Effect.acquireRelease(
          Effect.gen(function* () {
            const queue = yield* Queue.make<Schema.Json, ProtocolError>();

            queues.add(queue);
            yield* Queue.offer(queue, snapshot(cursor, value));

            return queue;
          }),
          (queue) =>
            Effect.sync(() => {
              queues.delete(queue);
            }),
        ).pipe(Effect.map(Stream.fromQueue)),
      ),
    execute: (command) =>
      Effect.gen(function* () {
        calls.push(command);
        yield* beforeExecute;
        const found = ledger.get(command.commandId);

        if (found !== undefined) return found;

        const payload = yield* Schema.decodeEffect(Schema.toCodecJson(Counter.actions.add.command))(
          command,
        );

        const before = value;

        value += payload.payload.amount;
        cursor += 1;

        const outcome: Schema.Json = {
          _tag: "Succeeded",
          result: { before, after: value },
          position: position(cursor),
        };

        yield* publish(
          event(cursor, payload.payload.amount, {
            actorId: "alice",
            commandId: command.commandId,
          }),
        );
        ledger.set(command.commandId, outcome);
        if (dropResponse) return yield* unavailable();

        return outcome;
      }).pipe(
        Effect.catchTag("SchemaError", () =>
          Effect.fail(ProtocolError.make({ reason: "UnsupportedVersion", message: "bad command" })),
        ),
      ),
    result: (command) =>
      Effect.sync((): Schema.Json => {
        lookups.push(command);
        const outcome = ledger.get(command.commandId);

        return lookupMode !== "normal"
          ? { _tag: lookupMode }
          : outcome === undefined
            ? { _tag: "Unknown" }
            : { _tag: "Found", outcome };
      }),
    publishMessage: () => Effect.void,
  };

  return {
    transport,
    publish,
    calls,
    lookups,
    drop: (value: boolean) => {
      dropResponse = value;
    },
    lookup: (mode: typeof lookupMode) => {
      lookupMode = mode;
    },
    pause: (effect: Effect.Effect<void>) => {
      beforeExecute = effect;
    },
    head: (nextCursor: number, nextValue: number) => {
      cursor = nextCursor;
      value = nextValue;
    },
  };
});

const options = (transport: Client.Transport, storage?: PersistenceHandle): Client.Options => ({
  actorId: "alice",
  transport,
  persistence: storage === undefined ? { mode: "volatile" } : { mode: "persistent", storage },
  retry: { maxAttempts: 3, initialDelay: "10 millis", maxDelay: "20 millis" },
});

const stateWhere = (
  lease: Client.Lease<typeof Counter.spec>,
  predicate: (state: Client.State<typeof Counter.spec>) => boolean,
) => lease.changes.pipe(Stream.filter(predicate), Stream.take(1), Stream.runCollect);

it.effect("rejects a whole gapped batch before recovering authoritative state", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* server;
      const client = yield* Client.make(definition, options(remote.transport));
      const lease = yield* client.open(address);

      yield* lease.ready;
      yield* remote.publish(event(1, 3));
      yield* stateWhere(lease, (s) => s.authoritative?.position.cursor === 1);
      remote.head(4, 20);
      yield* remote.publish({
        ...header,
        _tag: "Events",
        position: position(4),
        events: [event(2, 4).event, event(4, 13).event],
      });
      yield* stateWhere(lease, (s) => s.connection === "recovering");
      expect((yield* lease.read).authoritative?.snapshot.source.value).toBe(3);
      yield* TestClock.adjust("10 millis");
      yield* stateWhere(lease, (s) => s.authoritative?.position.cursor === 4);
      expect((yield* lease.read).value?.source.value).toBe(20);
    }),
  ),
);

it.effect(
  "cancels a disposed lease's queued command while another lease keeps the source alive",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;
        const storage = yield* memory().open("alice");
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();

        remote.pause(
          Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))),
        );
        const client = yield* Client.make(definition, options(remote.transport, storage));
        const survivor = yield* client.open(address);
        const leaseScope = yield* Scope.fork(yield* Effect.scope);
        const disposed = yield* client.open(address).pipe(Scope.provide(leaseScope));

        yield* survivor.ready;

        const first = yield* survivor
          .execute(Counter.actions.add, { amount: 1 })
          .pipe(Effect.forkChild);

        yield* Deferred.await(started);

        const queued = yield* disposed
          .execute(Counter.actions.add, { amount: 5 })
          .pipe(Effect.forkChild);

        yield* stateWhere(survivor, (state) => state.pending.length === 2);
        const id = (yield* survivor.read).pending[1].commandId;

        yield* Scope.close(leaseScope, Exit.void);
        yield* Deferred.succeed(release, undefined);
        expect(yield* Fiber.join(first)).toEqual({ before: 0, after: 1 });
        expect((yield* Fiber.await(queued))._tag).toBe("Failure");
        expect(remote.calls).toHaveLength(1);
        expect(yield* storage.intentJournal.transaction((tx) => tx.get(address, id))).toBeDefined();
        expect(yield* survivor.retry(id)).toEqual({ before: 1, after: 6 });
        expect(remote.calls[1]?.commandId).toBe(id);
      }),
    ),
);

it.effect(
  "fences an in-flight execute when its lease closes but the shared source remains open",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;
        const storage = yield* memory().open("alice");
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();

        const delayed = Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
        );

        const client = yield* Client.make(
          definition,
          options(
            {
              ...remote.transport,
              execute: () =>
                delayed.pipe(
                  Effect.as({
                    _tag: "Succeeded",
                    result: { before: 0, after: 3 },
                    position: position(1),
                  }),
                  Effect.uninterruptible,
                ),
            },
            storage,
          ),
        );

        const survivor = yield* client.open(address);
        const leaseScope = yield* Scope.fork(yield* Effect.scope);
        const lease = yield* client.open(address).pipe(Scope.provide(leaseScope));

        yield* lease.ready;

        const response = yield* lease
          .execute(Counter.actions.add, { amount: 3 })
          .pipe(Effect.forkChild);

        yield* Deferred.await(started);
        const closing = yield* Scope.close(leaseScope, Exit.void).pipe(Effect.forkChild);

        yield* tick;
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(closing);
        expect((yield* Fiber.await(response))._tag).toBe("Failure");
        const pending = (yield* survivor.read).pending;

        expect(pending).toHaveLength(1);
        expect(pending[0]?.phase).toBe("Pending");
        expect(
          yield* storage.intentJournal.transaction((tx) => tx.get(address, pending[0].commandId)),
        ).toMatchObject({ value: { phase: "Pending", outcome: null } });
      }),
    ),
);

it.effect("cancels settlement waiting on another lease's journal transaction", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* server;
      const storage = yield* memory().open("alice");
      const responding = yield* Deferred.make<void>();
      const finishResponse = yield* Deferred.make<void>();
      const writing = yield* Deferred.make<void>();
      const finishWrite = yield* Deferred.make<void>();
      let delayAdmission = false;

      remote.pause(
        Deferred.succeed(responding, undefined).pipe(
          Effect.andThen(Deferred.await(finishResponse)),
        ),
      );

      const client = yield* Client.make(
        definition,
        options(remote.transport, {
          ...storage,
          intentJournal: {
            transaction: (f) =>
              storage.intentJournal.transaction((tx) =>
                f({
                  ...tx,
                  put: (row) =>
                    tx
                      .put(row)
                      .pipe(
                        Effect.andThen(() =>
                          delayAdmission
                            ? Deferred.succeed(writing, undefined).pipe(
                                Effect.andThen(Deferred.await(finishWrite)),
                              )
                            : Effect.void,
                        ),
                      ),
                }),
              ),
          },
        }),
      );

      const survivor = yield* client.open(address);
      const leaseScope = yield* Scope.fork(yield* Effect.scope);
      const lease = yield* client.open(address).pipe(Scope.provide(leaseScope));

      yield* lease.ready;

      const response = yield* lease
        .execute(Counter.actions.add, { amount: 1 })
        .pipe(Effect.forkChild);

      yield* Deferred.await(responding);
      delayAdmission = true;

      const other = yield* survivor
        .execute(Counter.actions.add, { amount: 2 })
        .pipe(Effect.forkChild);

      yield* Deferred.await(writing);
      yield* Deferred.succeed(finishResponse, undefined);
      yield* tick;
      const closing = yield* Scope.close(leaseScope, Exit.void).pipe(Effect.forkChild);

      yield* tick;
      const closedBeforeWrite = closing.pollUnsafe()?._tag === "Success";

      yield* Deferred.succeed(finishWrite, undefined);
      yield* Fiber.join(closing);
      yield* Fiber.join(other);
      expect(closedBeforeWrite).toBe(true);
      expect((yield* Fiber.await(response))._tag).toBe("Failure");
    }),
  ),
);

it.effect(
  "refuses journal failures before I/O and never silently falls back to volatile mode",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;
        const storage = yield* memory({ maxJournalRows: 0 }).open("alice");
        const client = yield* Client.make(definition, options(remote.transport, storage));
        const lease = yield* client.open(address);

        yield* lease.ready;
        const result = yield* Effect.result(lease.execute(Counter.actions.add, { amount: 1 }));

        expect(result).toMatchObject({ _tag: "Failure", failure: { reason: "Storage" } });
        expect(remote.calls).toEqual([]);
        expect((yield* lease.read).pending).toEqual([]);
      }),
    ),
);

it.effect("preserves rich command and result codecs through lookup, journaling, and remount", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const value = Schema.Struct({ at: Schema.DateTimeUtc });

      const Reminder = Source.make({
        kind: "reminder",
        schemaVersion: 1,
        snapshot: value,
        event: value,
        message: Schema.Never,
        actions: [
          Action.make("schedule", {
            payload: value,
            success: Schema.DateTimeUtc,
            error: Schema.Never,
          }),
        ],
        plugins: [],
      });

      const reducers = Client.definition(Reminder, {
        applyEvent: (_state, event) => event,
        optimistic: { schedule: (_state, payload) => payload },
        plugins: [],
      });

      const at = "2026-09-23T12:00:00.000Z";

      const snapshot = {
        protocolVersion: 1,
        schemaVersion: 1,
        address: Reminder.address("demo"),
        _tag: "Snapshot",
        position: position(0),
        snapshot: { source: { at: "2026-09-22T12:00:00.000Z" }, plugins: {} },
      };

      const calls: Array<Client.EncodedCommand> = [];

      const transport: Client.Transport = {
        snapshot: () => Effect.succeed(snapshot),
        subscribe: () => Stream.make(snapshot).pipe(Stream.concat(Stream.never)),
        execute: (command) =>
          Effect.sync(() => calls.push(command)).pipe(Effect.andThen(Effect.fail(unavailable()))),
        result: () =>
          Effect.sync(() => {
            return {
              _tag: "Found",
              outcome: { _tag: "Succeeded", result: at, position: position(1) },
            };
          }),
        publishMessage: () => Effect.void,
      };

      const storage = yield* memory().open("alice");
      const settings = options(transport, storage);

      const id = yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* Client.make(reducers, settings);
          const lease = yield* client.open(Reminder.address("demo"));

          yield* lease.ready;

          const result = yield* Effect.result(
            lease.execute(Reminder.actions.schedule, { at: DateTime.makeUnsafe(at) }),
          );

          if (result._tag !== "Failure" || result.failure.commandId === undefined)
            return yield* Effect.die("Expected an ambiguous command result");
          const id = result.failure.commandId;

          expect(DateTime.formatIso(yield* lease.retry(id))).toBe(at);
          expect(
            yield* storage.intentJournal.transaction((tx) => tx.get(Reminder.address("demo"), id)),
          ).toMatchObject({
            value: {
              phase: "Accepted",
              outcome: { _tag: "Succeeded", result: at, position: position(1) },
            },
          });

          return id;
        }),
      );

      const client = yield* Client.make(reducers, settings);
      const lease = yield* client.open(Reminder.address("demo"));

      yield* lease.ready;
      expect(DateTime.formatIso(yield* lease.retry(id))).toBe(at);
      expect(calls).toHaveLength(1);
    }),
  ),
);

it.effect("does not confirm another actor's command with the same identity", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* server;

      const client = yield* Client.make(
        definition,
        options({ ...remote.transport, execute: () => Effect.fail(unavailable()) }),
      );

      const lease = yield* client.open(address);

      yield* lease.ready;
      yield* lease.execute(Counter.actions.add, { amount: 2 }).pipe(Effect.result);
      const id = (yield* lease.read).pending[0].commandId;

      yield* remote.publish(event(1, 5, { actorId: "bob", commandId: id }));
      yield* stateWhere(lease, (state) => state.authoritative?.position.cursor === 1);
      expect((yield* lease.read).value?.source.value).toBe(7);
      expect((yield* lease.read).pending).toEqual([{ commandId: id, phase: "Pending" }]);
    }),
  ),
);

it.effect("keeps expired confirmed results unresolved without re-executing them", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* server;

      remote.drop(true);
      remote.lookup("Expired");
      const client = yield* Client.make(definition, options(remote.transport));
      const lease = yield* client.open(address);

      yield* lease.ready;
      yield* lease.execute(Counter.actions.add, { amount: 4 }).pipe(Effect.result);
      yield* stateWhere(lease, (state) => state.pending[0]?.phase === "ConfirmedAwaitingResult");
      const id = (yield* lease.read).pending[0].commandId;

      expect(yield* Effect.result(lease.retry(id))).toMatchObject({
        _tag: "Failure",
        failure: { reason: "OutcomeUnknown", commandId: id },
      });
      expect((yield* lease.read).pending).toEqual([
        { commandId: id, phase: "ConfirmedAwaitingResult" },
      ]);
      expect(remote.calls).toHaveLength(1);
    }),
  ),
);

it.effect("never rebinds an ambiguous unbound command to a newly observed authority", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* server;
      let calls = 0;

      const client = yield* Client.make(
        Client.definition(Counter, {
          applyEvent: (state, event) => ({ value: state.value + event.delta }),
          optimistic: { add: (state, payload) => ({ value: state.value + payload.amount }) },
          allowUnboundCommands: true,
          plugins: [label],
        }),
        options({
          ...remote.transport,
          snapshot: () => Effect.never,
          subscribe: () => Stream.never,
          execute: () =>
            Effect.suspend(() => {
              calls += 1;

              return Effect.fail(unavailable());
            }),
        }),
      );

      const lease = yield* client.open(address);

      yield* lease.execute(Counter.actions.add, { amount: 2 }).pipe(Effect.result);
      const id = (yield* lease.read).pending[0].commandId;

      yield* lease.hydrate(snapshot(0, 10, replacement));
      expect(yield* Effect.result(lease.retry(id))).toMatchObject({
        _tag: "Failure",
        failure: { reason: "OutcomeUnknown", commandId: id },
      });
      expect(calls).toBe(1);
      expect(remote.lookups.at(-1)?.admittedGeneration).toBeNull();
    }),
  ),
);

it.effect.each([
  { transition: "confirmation", lifetime: "source" },
  { transition: "admission", lifetime: "lease" },
] as const)(
  "reloads journal $transition committed while the $lifetime scope closes",
  ({ transition, lifetime }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;
        const storage = yield* memory().open("alice");
        const writing = yield* Deferred.make<Client.Intent>();
        const finishWrite = yield* Deferred.make<void>();
        const finishLookup = yield* Deferred.make<void>();
        const sourceScope = yield* Scope.fork(yield* Effect.scope);
        const write = transition === "admission" ? "put" : "replace";

        const delayCommit = Effect.fn("delayCommit")(function* (row: JournalRow) {
          yield* Deferred.succeed(
            writing,
            yield* Schema.decodeEffect(Schema.toCodecJson(Client.Intent))(row.value).pipe(
              Effect.orDie,
            ),
          );
          yield* Deferred.await(finishWrite);
        });

        remote.drop(transition === "confirmation");

        const settings = options(
          {
            ...remote.transport,
            result: (command) =>
              Deferred.await(finishLookup).pipe(Effect.andThen(remote.transport.result(command))),
          },
          {
            ...storage,
            intentJournal: {
              transaction: (f) =>
                storage.intentJournal.transaction((tx) =>
                  f({
                    ...tx,
                    [write]: (row: JournalRow) =>
                      tx[write](row).pipe(Effect.andThen(delayCommit(row))),
                  }),
                ),
            },
          },
        );

        const client = yield* Client.make(definition, settings);

        const lease = yield* client.open(address).pipe(Scope.provide(sourceScope));

        yield* lease.ready;
        if (lifetime === "lease") yield* client.open(address);

        const response = yield* lease
          .execute(Counter.actions.add, { amount: 3 })
          .pipe(Effect.forkChild);

        const intent = yield* Deferred.await(writing);

        const closing = yield* Scope.close(sourceScope, Exit.void).pipe(Effect.forkChild);

        yield* tick;
        expect(yield* Effect.result(lease.read)).toMatchObject({
          _tag: "Failure",
          failure: { reason: "Closed" },
        });
        yield* Deferred.succeed(finishWrite, undefined);
        yield* Fiber.join(closing);
        expect((yield* Fiber.await(response))._tag).toBe("Failure");
        expect(remote.calls).toEqual(transition === "admission" ? [] : [intent.command]);
        expect(
          yield* storage.intentJournal.transaction((tx) =>
            tx.get(address, intent.command.commandId),
          ),
        ).toMatchObject({ value: intent });

        const reopened = yield* client.open(address);

        yield* reopened.ready;
        expect((yield* reopened.read).pending).toEqual([
          {
            commandId: intent.command.commandId,
            phase: transition === "admission" ? "Pending" : "ConfirmedAwaitingResult",
          },
        ]);
        expect((yield* reopened.read).value?.source.value).toBe(3);
        yield* Deferred.succeed(finishLookup, undefined);
        expect(yield* reopened.retry(intent.command.commandId)).toEqual({ before: 0, after: 3 });
        yield* stateWhere(reopened, (state) => state.pending.length === 0);
        expect(remote.lookups[0]).toEqual(intent.command);
        expect(remote.calls).toEqual([intent.command]);
        expect(yield* storage.intentJournal.transaction((tx) => tx.scan(address, 10))).toEqual([]);
      }),
    ),
);

it.effect("ignores a late snapshot probe after live progress", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* server;
      const loading = yield* Deferred.make<Schema.Json>();
      const probing = yield* Deferred.make<void>();

      const client = yield* Client.make(
        definition,
        options({
          ...remote.transport,
          snapshot: () =>
            Deferred.succeed(probing, undefined).pipe(Effect.andThen(Deferred.await(loading))),
        }),
      );

      const lease = yield* client.open(address);

      yield* lease.ready;
      yield* TestClock.adjust("30 seconds");
      yield* Deferred.await(probing);
      yield* remote.publish(snapshot(5, 20));
      yield* stateWhere(lease, (state) => state.authoritative?.position.cursor === 5);
      yield* remote.publish(event(6, 3));
      yield* stateWhere(lease, (state) => state.authoritative?.position.cursor === 6);
      yield* Deferred.succeed(loading, snapshot(0, 0));
      yield* tick;
      expect((yield* lease.read).authoritative).toEqual({
        position: position(6),
        snapshot: { source: { value: 23 }, plugins: { label: { text: "original" } } },
      });
    }),
  ),
);
