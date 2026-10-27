import { ProtocolError, type SourcePosition } from "@yielded/sync";
import * as Cloudflare from "@yielded/sync-platform-cloudflare/Cloudflare";
import * as SqliteStorage from "@yielded/sync-platform-cloudflare/SqliteStorage";
import {
  SourceStorage,
  type OutboxRecord,
  type OutboxStorage,
  type StoredHead,
} from "@yielded/sync/server";
import { SELF, env, reset, runInDurableObject, evictDurableObject } from "cloudflare:test";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import { afterEach, describe, expect, it } from "vite-plus/test";

import { Counter, CounterId } from "./fixtures/counter/contract.ts";
import type { CounterObject } from "./fixtures/counter/worker.ts";
import { Publication, type PublicationObject } from "./fixtures/worker.ts";

describe("EffectCfServices", () => {
  const bindings = env as { COUNTERS: DurableObjectNamespace<InstanceType<typeof CounterObject>> };
  const address = Counter.address(CounterId.make("test"));
  const header = { protocolVersion: 1, schemaVersion: 1, address };
  const stub = () => bindings.COUNTERS.getByName("counter:test");
  const Json = Schema.decodeUnknownSync(Schema.Json);

  const RpcExit = Schema.Array(
    Schema.TaggedStruct("Exit", {
      requestId: Schema.Union([Schema.String, Schema.Int]),
      exit: Schema.Union([
        Schema.TaggedStruct("Success", { value: Schema.optionalKey(Schema.Json) }),
        Schema.TaggedStruct("Failure", {
          cause: Schema.Array(Schema.TaggedStruct("Fail", { error: ProtocolError })),
        }),
      ]),
    }),
  );

  const invoke = async (tag: string, payload: Schema.Json, actor = "alice") => {
    const response = await SELF.fetch("https://example.test/sync/counters/test", {
      method: "POST",
      headers: { authorization: `Bearer ${actor}-local`, "content-type": "application/json" },
      body: JSON.stringify([{ _tag: "Request", id: "1", tag, payload, headers: [] }]),
    });

    expect(response.status).toBe(200);
    const [reply] = Schema.decodeUnknownSync(RpcExit)(await response.json());

    return reply.exit;
  };

  const snapshot = async () => {
    const result = await invoke("snapshot", header);

    expect(result._tag).toBe("Success");
    if (result._tag !== "Success") throw new Error("Snapshot failed");

    return Schema.decodeUnknownSync(Schema.toCodecJson(Counter.envelope.snapshotFrame))(
      result.value,
    );
  };

  const command = (generation: string, commandId: string, payload: number) => ({
    ...header,
    namespace: "$source",
    action: "set",
    admittedGeneration: generation,
    commandId,
    payload,
  });

  afterEach(() => reset());

  describe("public source on a SQLite Durable Object", () => {
    it("uses authenticated Worker identity instead of a supplied session header", async () => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const client = yield* Cloudflare.client(Counter, {
            address,
            identity: {
              actorId: "bob",
              principal: { actorId: "bob" },
              expiresAtMillis: 4_000_000_000_000,
            },
            fetch: (request) =>
              Effect.tryPromise(() => {
                const forwarded = new Request("https://example.test/sync/counters/test", request);

                forwarded.headers.set("authorization", "Bearer alice-local");

                return SELF.fetch(forwarded);
              }),
          });

          const initial = yield* client.snapshot;

          yield* client.execute(Counter.actions.set, {
            commandId: "native",
            admittedGeneration: initial.position.sourceAuthorityGeneration,
            payload: 12,
          });
        }).pipe(Effect.scoped),
      );

      const stored = await runInDurableObject(stub(), (_instance, state) =>
        state.storage.sql.exec("SELECT actor FROM sync_receipts").one(),
      );

      expect(stored).toEqual({ actor: "alice" });
    });

    it("rejects conflicting commands while keeping receipt identities scoped to actors", async () => {
      const initial = await snapshot();
      const first = command(initial.position.sourceAuthorityGeneration, "first", 7);

      await invoke("execute/$source/set", first);

      await invoke(
        "execute/$source/set",
        command(initial.position.sourceAuthorityGeneration, "second", 9),
      );
      await evictDurableObject(stub());
      expect(await invoke("execute/$source/set", { ...first, payload: 8 })).toMatchObject({
        _tag: "Failure",
        cause: [{ error: { reason: "CommandIdConflict" } }],
      });
      expect((await snapshot()).snapshot.source.value).toBe(9);
      expect(await invoke("execute/$source/set", first, "bob")).toMatchObject({
        _tag: "Success",
        value: { result: { previous: 9, current: 7 } },
      });
      expect((await snapshot()).position.cursor).toBe(3);
    });

    it("serializes concurrent retries without advancing twice", async () => {
      const initial = await snapshot();
      const request = command(initial.position.sourceAuthorityGeneration, "parallel", 4);

      const results = await Promise.all([
        invoke("execute/$source/set", request),
        invoke("execute/$source/set", request),
      ]);

      expect(results[0]).toEqual(results[1]);
      expect((await snapshot()).position.cursor).toBe(1);
    });

    it("rolls back state, events and receipt when the outbox insert fails", async () => {
      const initial = await snapshot();

      await runInDurableObject(stub(), (_instance, state) => {
        state.storage.sql.exec(
          "CREATE TRIGGER reject_outbox BEFORE INSERT ON sync_outbox BEGIN SELECT RAISE(ABORT, 'injected failure'); END",
        );
      });
      const request = command(initial.position.sourceAuthorityGeneration, "atomic", 7);

      expect(await invoke("execute/$source/set", request)).toMatchObject({
        _tag: "Failure",
        cause: [{ error: { reason: "Unavailable" } }],
      });
      expect(await snapshot()).toEqual(initial);

      const counts = await runInDurableObject(stub(), (_instance, state) =>
        state.storage.sql
          .exec(
            "SELECT (SELECT COUNT(*) FROM sync_events) AS events, (SELECT COUNT(*) FROM sync_receipts) AS receipts, (SELECT COUNT(*) FROM sync_outbox) AS outbox",
          )
          .one(),
      );

      expect(counts).toEqual({ events: 0, receipts: 0, outbox: 0 });
      await runInDurableObject(stub(), (_instance, state) => {
        state.storage.sql.exec("DROP TRIGGER reject_outbox");
      });
      expect(await invoke("execute/$source/set", request)).toMatchObject({
        _tag: "Success",
        value: { result: { previous: 0, current: 7 } },
      });
    });

    it("retains exact receipts after replay events are pruned", async () => {
      const initial = await snapshot();
      const generation = initial.position.sourceAuthorityGeneration;

      const rename = {
        ...header,
        namespace: "label",
        action: "rename",
        admittedGeneration: generation,
        commandId: "rename",
        payload: "Example",
      };

      expect(await invoke("execute/label/rename", rename)).toMatchObject({
        _tag: "Success",
        value: { result: "Example", position: { cursor: 1 } },
      });
      for (let value = 1; value <= 6; value++)
        await invoke("execute/$source/set", command(generation, `set-${value}`, value));
      expect(await invoke("execute/label/rename", rename)).toMatchObject({
        _tag: "Success",
        value: { result: "Example", position: { cursor: 1 } },
      });
    });
  });

  const connect = async () => {
    const response = await SELF.fetch("https://example.test/sync/counters/test", {
      headers: { upgrade: "websocket", authorization: "Bearer alice-local" },
    });

    const socket = response.webSocket;

    if (socket === null) throw new Error(`Upgrade failed: ${response.status}`);
    socket.accept();
    const pending: Array<(value: Schema.Json) => void> = [];
    const received: Array<Schema.Json> = [];

    socket.addEventListener("message", (event) => {
      const value = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Json))(event.data);
      const resolve = pending.shift();

      if (resolve === undefined) received.push(value);
      else resolve(value);
    });

    return {
      socket,
      next: () =>
        received.length > 0
          ? Promise.resolve(received.shift())
          : new Promise<Schema.Json>((resolve) => {
              pending.push(resolve);
            }),
      send: (tag: string, payload: Schema.Json, id = "stream") =>
        socket.send(JSON.stringify({ _tag: "Request", id, tag, payload, headers: [] })),
    };
  };

  describe("hibernating subscriptions", () => {
    it("disconnects a consumer that stops acknowledging without losing durable progress", async () => {
      const initial = await snapshot();
      const client = await connect();

      client.send("subscribe", header);
      await client.next();

      const closed = new Promise<number>((resolve) =>
        client.socket.addEventListener("close", (event) => resolve(event.code), { once: true }),
      );

      for (let value = 1; value <= 16; value++)
        await invoke(
          "execute/$source/set",
          command(initial.position.sourceAuthorityGeneration, `overflow-${value}`, value),
        );
      expect(await closed).toBe(1013);
      expect((await snapshot()).position.cursor).toBe(16);
    });

    it("requires resynchronization when a requested cursor falls outside replay retention", async () => {
      const initial = await snapshot();

      for (let value = 1; value <= 6; value++)
        await invoke(
          "execute/$source/set",
          command(initial.position.sourceAuthorityGeneration, `gap-${value}`, value),
        );
      const stale = await connect();

      stale.send("subscribe", { ...header, after: initial.position });
      expect(await stale.next()).toMatchObject({
        _tag: "Chunk",
        values: [{ _tag: "ResyncRequired", position: { cursor: 6 } }],
      });
      stale.socket.close();
    });

    it("fences an expired identity when a hibernated connection wakes", async () => {
      await snapshot();
      const client = await connect();

      client.send("subscribe", header);
      await client.next();
      await runInDurableObject(stub(), (_instance, state) => {
        for (const socket of state.getWebSockets()) {
          const attachment = Schema.decodeUnknownSync(
            Schema.Struct({ session: Schema.Record(Schema.String, Schema.Json) }),
          )(socket.deserializeAttachment());

          const original = Json(socket.deserializeAttachment());

          socket.serializeAttachment({
            ...(original as object),
            session: { ...attachment.session, expiresAtMillis: 1 },
          });
        }
      });
      await evictDurableObject(stub());

      const closed = new Promise<number>((resolve) =>
        client.socket.addEventListener("close", (event) => resolve(event.code), { once: true }),
      );

      client.socket.send(JSON.stringify({ _tag: "Ping" }));
      expect(await closed).toBe(4403);
    });
  });
});

describe("EffectCfServices publication", () => {
  const bindings = env as {
    PUBLICATION: DurableObjectNamespace<InstanceType<typeof PublicationObject>>;
  };

  const address = Publication.address("test");
  const header = { protocolVersion: 1, schemaVersion: 1, address };
  const stub = () => bindings.PUBLICATION.getByName("test");

  const headers = (failure = "none") => ({
    "content-type": "application/json",
    "x-yielded-address": JSON.stringify(address),
    "x-yielded-session": JSON.stringify({
      principal: { failure },
      actorId: "alice",
      connectionId: crypto.randomUUID(),
      expiresAtMillis: 4_000_000_000_000,
    }),
  });

  const request = (tag: string, payload: Schema.Json) =>
    stub().fetch("https://example.test/", {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({ _tag: "Request", id: "1", tag, payload, headers: [] }),
    });

  const rpc = async (tag: string, payload: Schema.Json) => {
    const response = await request(tag, payload);

    expect(response.status).toBe(200);

    const [reply] = Schema.decodeUnknownSync(
      Schema.Array(
        Schema.TaggedStruct("Exit", {
          exit: Schema.TaggedStruct("Success", { value: Schema.Json }),
        }),
      ),
    )(await response.json());

    return reply.exit.value;
  };

  const snapshot = async () =>
    Schema.decodeSync(Schema.toCodecJson(Publication.envelope.snapshotFrame))(
      await rpc("snapshot", header),
    );

  const connect = async (failure = "none", after?: SourcePosition) => {
    const response = await stub().fetch("https://example.test/", {
      headers: { ...headers(failure), upgrade: "websocket" },
    });

    const socket = response.webSocket;

    if (socket === null) throw new Error(`Upgrade failed: ${response.status}`);
    socket.accept();

    const closed = new Promise<number>((resolve) =>
      socket.addEventListener("close", (event) => resolve(event.code), { once: true }),
    );

    const first = new Promise<Schema.Json>((resolve) =>
      socket.addEventListener(
        "message",
        (event) =>
          resolve(Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Json))(event.data)),
        { once: true },
      ),
    );

    socket.send(
      JSON.stringify({
        _tag: "Request",
        id: "stream",
        tag: "subscribe",
        payload: { ...header, after },
        headers: [],
      }),
    );

    return { socket, closed, first };
  };

  afterEach(() => reset());

  it("disconnects all subscribers after a committed command's publication is interrupted", async () => {
    const initial = await snapshot();
    const affected = await connect("interrupt");
    const peer = await connect();

    await Promise.all([affected.first, peer.first]);

    const command = {
      ...header,
      namespace: "$source",
      action: "set",
      admittedGeneration: initial.position.sourceAuthorityGeneration,
      commandId: "committed",
      payload: 1,
    };

    const response = await request("execute/$source/set", command);

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject([
      { _tag: "Exit", exit: { _tag: "Failure", cause: [{ _tag: "Interrupt" }] } },
    ]);
    expect(await snapshot()).toMatchObject({ position: { cursor: 1 }, snapshot: { source: 1 } });
    expect(await Promise.all([affected.closed, peer.closed])).toEqual([1013, 1013]);
    expect(await rpc("execute/$source/set", command)).toMatchObject({
      _tag: "Succeeded",
      position: { ...initial.position, cursor: 1 },
      result: 1,
    });

    const recovered = await connect("none", initial.position);

    expect(await recovered.first).toMatchObject({
      _tag: "Chunk",
      values: [{ _tag: "Events", position: { cursor: 1 }, events: [{ event: { payload: 1 } }] }],
    });
    recovered.socket.close();
  });
});

describe("EffectCfServices SQLite storage", () => {
  const bindings = env as { COUNTERS: DurableObjectNamespace<CounterObject> };
  const stub = () => bindings.COUNTERS.getByName("storage-proof");

  const initial: StoredHead = {
    address: { kind: "counter", id: "storage-proof" },
    schemaVersion: 1,
    position: { sourceAuthorityGeneration: "6d5f7444-dba4-4a4c-a343-ce7e7dac6e88", cursor: 0 },
    state: { value: 0 },
  };

  const usingStorage = <A, E>(
    body: (storage: SourceStorage["Service"], outbox: OutboxStorage) => Effect.Effect<A, E>,
  ) =>
    runInDurableObject(stub(), (_instance, state) =>
      Effect.runPromise(
        Effect.flatMap(SourceStorage, (storage) =>
          storage.outbox === undefined
            ? Effect.die("Configured SQLite outbox is unavailable")
            : body(storage, storage.outbox),
        ).pipe(
          Effect.provide(
            SqliteStorage.layer(state.storage, {
              namespace: "counter-v1",
              replayWindow: 4,
              maxPendingOutbox: 100,
            }),
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
      Effect.fn(function* (storage, outbox) {
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
        expect(yield* outbox.nextTime).toBeUndefined();
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
      Effect.fn(function* (storage, outbox) {
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
        const claimed = yield* outbox.claim(due, 100, 1);

        expect(yield* outbox.claim(due + 1, 100, 1)).toEqual([]);

        return claimed[0];
      }),
    );

    await evictDurableObject(stub());
    await usingStorage(
      Effect.fn(function* (_storage, outbox) {
        const [recovered] = yield* outbox.claim(due + 101, 100, 1);

        expect(recovered).toEqual({ ...original, attempts: 2, availableAtMillis: due + 201 });
        yield* outbox.settle(original, { _tag: "Delivered" });
        expect(yield* outbox.nextTime).toBe(due + 201);
        yield* outbox.settle(recovered, { _tag: "Delivered" });
        expect(yield* outbox.nextTime).toBeUndefined();
      }),
    );
  });
});
