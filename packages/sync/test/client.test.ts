import { it } from "@effect/vitest";
import { Action, Plugin, ProtocolError, RegistrationError, Source } from "@yielded/sync";
import { SourceAtom } from "@yielded/sync/atom";
import {
  Client,
  ClientError,
  memory,
  type JournalRow,
  type PersistenceHandle,
} from "@yielded/sync/client";
import { Deferred, Effect, Exit, Fiber, Layer, Queue, Schema, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { AtomRegistry } from "effect/unstable/reactivity";
import { expect } from "vite-plus/test";

class Invalid extends Schema.TaggedError<Invalid>()("Invalid", { amount: Schema.Finite }) {}

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
      error: Invalid,
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
  let opened = 0;
  let closed = 0;
  let cursor = 0;
  let value = 0;
  let currentGeneration = generation;
  let dropResponse = false;
  let lookupMode: "normal" | "Unknown" | "Expired" = "normal";
  let beforeExecute: Effect.Effect<void> = Effect.void;
  let load: Effect.Effect<Schema.Json, ProtocolError> | undefined;

  const publish = (frame: Schema.Json) =>
    Effect.forEach(queues, (queue) => Queue.offer(queue, frame)).pipe(Effect.asVoid);

  const transport: Client.Transport = {
    snapshot: () =>
      Effect.suspend(() => load ?? Effect.succeed(snapshot(cursor, value, currentGeneration))),
    subscribe: () =>
      Stream.unwrap(
        Effect.acquireRelease(
          Effect.gen(function* () {
            const queue = yield* Queue.make<Schema.Json, ProtocolError>();

            queues.add(queue);
            opened += 1;

            return queue;
          }),
          (queue) =>
            Effect.sync(() => {
              queues.delete(queue);
              closed += 1;
            }),
        ).pipe(Effect.map(Stream.fromQueue)),
      ),
    execute: (command) =>
      Effect.gen(function* () {
        calls.push(command);
        yield* beforeExecute;
        const found = ledger.get(command.commandId);

        if (found !== undefined) return found;
        if (command.admittedGeneration !== null && command.admittedGeneration !== currentGeneration)
          return yield* ProtocolError.make({
            reason: "AuthorityMismatch",
            message: "Authority changed",
          });

        const payload = yield* Schema.decodeEffect(Schema.toCodecJson(Counter.actions.add.command))(
          command,
        );

        let outcome: Schema.Json;

        if (payload.payload.amount < 0) {
          outcome = {
            _tag: "Rejected",
            error: { _tag: "Invalid", amount: payload.payload.amount },
          };
        } else {
          const before = value;

          value += payload.payload.amount;
          cursor += 1;
          outcome = {
            _tag: "Succeeded",
            result: { before, after: value },
            position: position(cursor, currentGeneration),
          };
          yield* publish(
            event(cursor, payload.payload.amount, {
              actorId: "alice",
              commandId: command.commandId,
            }),
          );
        }
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
    connections: () => ({ opened, closed }),
    drop: (value: boolean) => {
      dropResponse = value;
    },
    lookup: (mode: typeof lookupMode) => {
      lookupMode = mode;
    },
    pause: (effect: Effect.Effect<void>) => {
      beforeExecute = effect;
    },
    load: (effect: Effect.Effect<Schema.Json, ProtocolError>) => {
      load = effect;
    },
    head: (nextCursor: number, nextValue: number, authority = generation) => {
      cursor = nextCursor;
      value = nextValue;
      currentGeneration = authority;
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

it.effect(
  "ignores duplicates and stale hydration, rejects a whole gapped batch, and recovers",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;
        const client = yield* Client.make(definition, options(remote.transport));
        const lease = yield* client.open(address);

        yield* lease.ready;
        yield* remote.publish(event(1, 3));
        yield* stateWhere(lease, (s) => s.authoritative?.position.cursor === 1);
        yield* remote.publish(event(1, 3));
        yield* lease.hydrate(snapshot(0, 99));
        expect((yield* lease.read).value?.source.value).toBe(3);
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
        expect(remote.connections()).toEqual({ opened: 2, closed: 1 });
      }),
    ),
);

it.effect("rolls back a typed rejection without disturbing plugin state", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* server;
      const gate = yield* Deferred.make<void>();

      remote.pause(Deferred.await(gate));
      const client = yield* Client.make(definition, options(remote.transport));
      const lease = yield* client.open(address);

      yield* lease.ready;

      const response = yield* lease
        .execute(Counter.actions.add, { amount: -2 })
        .pipe(Effect.result, Effect.forkChild);

      yield* stateWhere(lease, (s) => s.pending.length === 1);
      expect((yield* lease.read).value?.source.value).toBe(-2);
      yield* Deferred.succeed(gate, undefined);
      const result = yield* Fiber.join(response);

      expect(result).toMatchObject({ _tag: "Failure", failure: { _tag: "Invalid", amount: -2 } });
      const state = yield* lease.read;

      expect(state.value).toEqual({
        source: { value: 0 },
        plugins: { label: { text: "original" } },
      });
      expect(state.pending).toEqual([]);
      expect(state.failures).toHaveLength(1);
      yield* lease.dismissFailure(state.failures[0].commandId);
      expect((yield* lease.read).failures).toEqual([]);
    }),
  ),
);

it.effect("retains confirmation before a lost result and retries the immutable envelope", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* server;
      const storage = yield* memory().open("alice");
      const client = yield* Client.make(definition, options(remote.transport, storage));
      const lease = yield* client.open(address);

      yield* lease.ready;
      remote.drop(true);
      const payload = { amount: 5 };
      const result = yield* Effect.result(lease.execute(Counter.actions.add, payload));

      expect(result._tag).toBe("Failure");
      if (result._tag !== "Failure" || !Schema.is(ClientError)(result.failure))
        return yield* Effect.die("Expected operational failure");
      const id = result.failure.commandId!;

      payload.amount = 100;
      yield* stateWhere(lease, (s) => s.pending[0]?.phase === "ConfirmedAwaitingResult");
      expect((yield* lease.read).value?.source.value).toBe(5);
      const retained = yield* storage.intentJournal.transaction((tx) => tx.get(address, id));

      expect(retained?.value).toMatchObject({
        phase: "ConfirmedAwaitingResult",
        command: { payload: { amount: 5 } },
      });
      expect(yield* lease.retry(id)).toEqual({ before: 0, after: 5 });
      expect(remote.lookups.at(-1)).toEqual(remote.calls[0]);
      expect(remote.calls).toHaveLength(1);
      expect((yield* lease.read).pending).toEqual([]);
      expect(yield* storage.intentJournal.transaction((tx) => tx.get(address, id))).toBeUndefined();
    }),
  ),
);

it.effect("resends a pending command only after an unknown lookup in the same authority", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* server;
      let fail = true;
      const submitted: Array<Client.EncodedCommand> = [];

      const transport: Client.Transport = {
        ...remote.transport,
        execute: (command) => {
          submitted.push(command);

          return fail ? Effect.fail(unavailable()) : remote.transport.execute(command);
        },
      };

      const client = yield* Client.make(definition, options(transport));
      const lease = yield* client.open(address);

      yield* lease.ready;
      const result = yield* Effect.result(lease.execute(Counter.actions.add, { amount: 2 }));

      if (result._tag !== "Failure" || !Schema.is(ClientError)(result.failure))
        return yield* Effect.die("Expected lost response");
      fail = false;
      expect(yield* lease.retry(result.failure.commandId!)).toEqual({ before: 0, after: 2 });
      expect(submitted).toHaveLength(2);
      expect(submitted[0]).toEqual(submitted[1]);
    }),
  ),
);

it.effect(
  "parks old work on explicit authority replacement and ignores ordinary foreign snapshots",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;

        const client = yield* Client.make(
          definition,
          options({ ...remote.transport, execute: () => Effect.fail(unavailable()) }),
        );

        const lease = yield* client.open(address);

        yield* lease.ready;
        const result = yield* Effect.result(lease.execute(Counter.actions.add, { amount: 2 }));

        if (result._tag !== "Failure" || !Schema.is(ClientError)(result.failure))
          return yield* Effect.die("Expected lost response");
        const id = result.failure.commandId!;

        yield* lease.hydrate(snapshot(0, 100, replacement));
        expect((yield* lease.read).authoritative?.position.sourceAuthorityGeneration).toBe(
          generation,
        );
        yield* remote.publish({ ...snapshot(0, 100, replacement), _tag: "Reset" });
        yield* stateWhere(
          lease,
          (s) => s.authoritative?.position.sourceAuthorityGeneration === replacement,
        );
        expect((yield* lease.read).value?.source.value).toBe(100);
        expect((yield* lease.read).pending).toEqual([{ commandId: id, phase: "AuthorityChanged" }]);
        expect(yield* Effect.result(lease.retry(id))).toMatchObject({
          _tag: "Failure",
          failure: { reason: "AuthorityChanged", commandId: id },
        });
        expect(remote.lookups).toHaveLength(0);
      }),
    ),
);

it.effect("shares source leases and fences stale handles after source and actor disposal", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* server;
      const actor = yield* Scope.make();
      const firstScope = yield* Scope.make();
      const secondScope = yield* Scope.make();

      const client = yield* Client.make(definition, options(remote.transport)).pipe(
        Scope.provide(actor),
      );

      const first = yield* client.open(address).pipe(Scope.provide(firstScope));
      const second = yield* client.open(address).pipe(Scope.provide(secondScope));

      yield* first.ready;
      expect(remote.connections()).toEqual({ opened: 1, closed: 0 });
      yield* Scope.close(firstScope, Exit.void);
      expect(remote.connections()).toEqual({ opened: 1, closed: 0 });
      expect(yield* Effect.result(first.execute(Counter.actions.add, { amount: 1 }))).toMatchObject(
        { _tag: "Failure", failure: { reason: "Closed" } },
      );
      yield* second.execute(Counter.actions.add, { amount: 1 });
      yield* Scope.close(secondScope, Exit.void);
      expect(remote.connections()).toEqual({ opened: 1, closed: 1 });
      yield* Scope.close(actor, Exit.void);
      expect((yield* client.read(address)).connection).toBe("closed");
      expect(yield* Effect.result(client.open(address))).toMatchObject({
        _tag: "Failure",
        failure: { reason: "Closed" },
      });
    }),
  ),
);

it.effect(
  "passive atoms never connect and active atoms share the headless coordinator across registries",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;
        const client = yield* Client.make(definition, options(remote.transport));
        const atoms = SourceAtom.make(client);
        const registry = AtomRegistry.make();
        const other = AtomRegistry.make();

        registry.mount(atoms.passiveReplica(address));
        registry.mount(atoms.status(address));
        yield* tick;
        expect(remote.connections()).toEqual({ opened: 0, closed: 0 });
        expect(atoms.replica({ ...address })).toBe(atoms.replica(address));
        registry.mount(atoms.replica(address));
        other.mount(atoms.replica(address));
        yield* client.changes(address).pipe(
          Stream.filter((s) => s.connection === "live"),
          Stream.take(1),
          Stream.runDrain,
        );
        expect(remote.connections()).toEqual({ opened: 1, closed: 0 });
        registry.dispose();
        yield* tick;
        expect(remote.connections()).toEqual({ opened: 1, closed: 0 });
        other.dispose();
        yield* tick;
        expect(remote.connections()).toEqual({ opened: 1, closed: 1 });
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

it.effect(
  "restores exact evidence across actor scopes while delayed cache hydration stays provisional",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;
        const database = memory();
        const storage = yield* database.open("alice");
        let id = "";

        remote.drop(true);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const client = yield* Client.make(definition, options(remote.transport, storage));
            const lease = yield* client.open(address);

            yield* lease.ready;
            const result = yield* Effect.result(lease.execute(Counter.actions.add, { amount: 8 }));

            if (result._tag !== "Failure" || !Schema.is(ClientError)(result.failure))
              return yield* Effect.die("Expected lost response");
            id = result.failure.commandId!;
          }),
        );
        const cache = yield* Deferred.make<Schema.Json>();

        const restored = yield* Client.make(
          definition,
          options(remote.transport, {
            ...storage,
            snapshotCache: { ...storage.snapshotCache, get: () => Deferred.await(cache) },
          }),
        );

        const lease = yield* restored.open(address);

        yield* lease.ready;
        yield* stateWhere(lease, (s) => s.pending.length === 0);
        yield* Deferred.succeed(cache, snapshot(0, 99));
        yield* tick;
        expect((yield* lease.read).value?.source.value).toBe(8);
        expect(remote.lookups.some((command) => command.commandId === id)).toBe(true);
        expect(remote.calls).toHaveLength(1);
      }),
    ),
);

it.effect(
  "wipes fence stale memory handles and preserve records written by the new generation",
  () =>
    Effect.gen(function* () {
      const database = memory();
      const old = yield* database.open("alice");

      yield* old.wipe;
      const fresh = yield* database.open("alice");

      yield* fresh.intentJournal.transaction((tx) =>
        tx.put({ address, commandId: "new", value: {} }),
      );

      const finished = yield* fresh.intentJournal.transaction((tx) => Effect.succeed(tx));

      expect(
        yield* Effect.result(finished.put({ address, commandId: "late", value: {} })),
      ).toMatchObject({
        _tag: "Failure",
        failure: { reason: "Conflict" },
      });
      expect(yield* Effect.result(old.snapshotCache.put(address, {}))).toMatchObject({
        _tag: "Failure",
        failure: { reason: "StaleGeneration" },
      });
      expect(yield* Effect.result(old.wipe)).toMatchObject({
        _tag: "Failure",
        failure: { reason: "StaleGeneration" },
      });
      expect(yield* fresh.intentJournal.transaction((tx) => tx.get(address, "new"))).toBeDefined();
    }),
);

it("validates dynamic reducer registrations before acquiring resources", () => {
  const reducers = { applyEvent: () => ({ text: "" }), optimistic: {} } as unknown as Parameters<
    typeof Client.plugin<
      typeof Label.id,
      typeof Label.spec,
      { rename: (snapshot: { text: string }, payload: { text: string }) => { text: string } }
    >
  >[1];

  expect(() => Client.plugin(Label, reducers)).toThrow(RegistrationError);
});

it.effect("does not confirm commands from another actor or from unversioned observations", () =>
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
      yield* lease.mergeObservation((state) => ({
        source: { value: 50 },
        plugins: state?.plugins ?? { label: { text: "original" } },
      }));
      expect((yield* lease.read).value?.source.value).toBe(52);
      expect((yield* lease.read).authoritative?.position.cursor).toBe(1);
      expect((yield* lease.read).pending[0].phase).toBe("Pending");
      yield* remote.publish({
        ...header,
        _tag: "Message",
        actorId: "bob",
        connectionId: "peer",
        message: { namespace: "$source", payload: { editing: true } },
      });
      yield* tick;
      expect((yield* lease.read).authoritative?.position.cursor).toBe(1);
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
      yield* lease.dismissFailure(id);
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

it.effect("quarantines corrupt journal rows and reserves their capacity", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const remote = yield* server;
      const storage = yield* memory().open("alice");

      yield* storage.intentJournal.transaction((tx) =>
        tx.put({ address, commandId: "reserved", value: { version: "old" } }),
      );
      yield* storage.snapshotCache.put(address, { broken: true });

      const client = yield* Client.make(definition, {
        ...options(remote.transport, storage),
        limits: { maxSources: 1, maxPendingPerSource: 1, frameBuffer: 10 },
      });

      const lease = yield* client.open(address);

      yield* lease.ready;
      expect((yield* lease.read).pending).toEqual([
        { commandId: "reserved", phase: "Quarantined" },
      ]);
      expect(yield* Effect.result(lease.execute(Counter.actions.add, { amount: 1 }))).toMatchObject(
        { _tag: "Failure", failure: { reason: "Capacity" } },
      );
      expect(yield* Effect.result(lease.retry("reserved"))).toMatchObject({
        _tag: "Failure",
        failure: { reason: "Quarantined" },
      });
      expect(
        yield* storage.intentJournal.transaction((tx) => tx.get(address, "reserved")),
      ).toMatchObject({ commandId: "reserved", value: { version: "old" } });
      expect(remote.calls).toEqual([]);
    }),
  ),
);

it.effect(
  "source disposal interrupts submissions while retaining evidence and isolating the next actor",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;
        const database = memory();
        const storage = yield* database.open("alice");
        const sourceScope = yield* Scope.make();
        const client = yield* Client.make(definition, options(remote.transport, storage));
        const lease = yield* client.open(address).pipe(Scope.provide(sourceScope));

        yield* lease.ready;
        remote.pause(Effect.never);

        const response = yield* lease
          .execute(Counter.actions.add, { amount: 3 })
          .pipe(Effect.forkChild);

        yield* stateWhere(lease, (state) => state.pending.length === 1);
        const id = (yield* lease.read).pending[0].commandId;

        yield* Scope.close(sourceScope, Exit.void);
        expect((yield* Fiber.await(response))._tag).toBe("Failure");
        expect(remote.connections()).toEqual({ opened: 1, closed: 1 });
        expect(yield* storage.intentJournal.transaction((tx) => tx.get(address, id))).toBeDefined();
        const bobStorage = yield* database.open("bob");

        const bob = yield* Client.make(definition, {
          ...options(remote.transport, bobStorage),
          actorId: "bob",
        });

        const fresh = yield* bob.open(address);

        yield* fresh.ready;
        expect((yield* fresh.read).pending).toEqual([]);
        expect((yield* fresh.read).value?.source.value).toBe(0);
      }),
    ),
);

it.effect.each(["admission", "confirmation"] as const)(
  "reloads journal %s committed while the last source lease is closing",
  (transition) =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;
        const storage = yield* memory().open("alice");
        const writing = yield* Deferred.make<Client.Intent>();
        const finishWrite = yield* Deferred.make<void>();
        const finishLookup = yield* Deferred.make<void>();
        const sourceScope = yield* Scope.make();
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

        const client = yield* Client.make(
          definition,
          options(
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
          ),
        );

        const lease = yield* client.open(address).pipe(Scope.provide(sourceScope));

        yield* lease.ready;

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

it.effect(
  "bounds reconnect attempts and parks authentication failures until explicit recovery",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;
        let attempts = 0;
        let forbidden = false;

        const transport: Client.Transport = {
          ...remote.transport,
          subscribe: () =>
            Stream.suspend(() => {
              attempts += 1;

              return Stream.fail(
                ProtocolError.make({
                  reason: forbidden ? "Forbidden" : "Unavailable",
                  message: "disconnected",
                }),
              );
            }),
        };

        const client = yield* Client.make(definition, options(transport));
        const lease = yield* client.open(address);

        yield* TestClock.adjust("100 millis");
        yield* stateWhere(lease, (state) => state.connection === "parked");
        expect(attempts).toBe(3);
        yield* TestClock.adjust("1 minute");
        expect(attempts).toBe(3);
        forbidden = true;
        yield* lease.recover;
        yield* stateWhere(lease, (state) => state.error?.reason === "Unauthorized");
        yield* TestClock.adjust("1 minute");
        expect(attempts).toBe(4);
        expect(
          yield* Effect.result(lease.execute(Counter.actions.add, { amount: 1 })),
        ).toMatchObject({ _tag: "Failure", failure: { reason: "Unauthorized" } });
      }),
    ),
);

it.effect(
  "disconnects an overflowing frame buffer and resumes from an authoritative snapshot",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;
        let attempts = 0;

        const client = yield* Client.make(definition, {
          ...options({
            ...remote.transport,
            subscribe: () =>
              Stream.suspend(() => {
                attempts += 1;

                return attempts === 1
                  ? Stream.fromIterable([snapshot(0, 0), event(1, 2), event(2, 3)])
                  : Stream.never;
              }),
          }),
          limits: { maxSources: 1, maxPendingPerSource: 2, frameBuffer: 1 },
        });

        const lease = yield* client.open(address);

        yield* stateWhere(lease, (state) => state.error?.reason === "Overflow");
        remote.head(2, 5);
        yield* TestClock.adjust("10 millis");
        yield* stateWhere(lease, (state) => state.authoritative?.position.cursor === 2);
        expect((yield* lease.read).value?.source.value).toBe(5);
        expect(attempts).toBe(2);
      }),
    ),
);

it.effect(
  "late HTTP hydration cannot rewind snapshots or events received through the live stream",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;
        const loading = yield* Deferred.make<Schema.Json>();

        remote.load(Deferred.await(loading));
        const client = yield* Client.make(definition, options(remote.transport));
        const lease = yield* client.open(address);

        yield* tick;
        yield* remote.publish(snapshot(5, 20));
        yield* lease.ready;
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

it.effect(
  "releases client plugin layers with the final source lease rather than the actor scope",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const remote = yield* server;
        const lifecycle: Array<string> = [];

        const layer = (name: string) =>
          Layer.effectDiscard(
            Effect.acquireRelease(
              Effect.sync(() => {
                lifecycle.push(`open ${name}`);
              }),
              () =>
                Effect.sync(() => {
                  lifecycle.push(`close ${name}`);
                }),
            ),
          );

        const scopedDefinition = Client.provide(
          Client.definition(Counter, {
            applyEvent: (state, event) => ({ value: state.value + event.delta }),
            optimistic: { add: (state, payload) => ({ value: state.value + payload.amount }) },
            plugins: [Client.providePlugin(label, layer("label"))],
          }),
          layer("root"),
        );

        const client = yield* Client.make(scopedDefinition, options(remote.transport));
        const sourceScope = yield* Scope.make();
        const lease = yield* client.open(address).pipe(Scope.provide(sourceScope));

        yield* lease.ready;
        expect(lifecycle).toEqual(["open root", "open label"]);
        yield* Scope.close(sourceScope, Exit.void);
        expect(lifecycle).toEqual(["open root", "open label", "close label", "close root"]);
        expect((yield* client.read(address)).connection).toBe("idle");
      }),
    ),
);
