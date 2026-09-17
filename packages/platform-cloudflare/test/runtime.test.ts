import { ProtocolError } from "@yielded/sync";
import { Client, ClientError } from "@yielded/sync/client";
import {
  SELF,
  env,
  reset,
  runInDurableObject,
  evictDurableObject,
  runDurableObjectAlarm,
} from "cloudflare:test";
import { Deferred, Effect, Fiber, Layer, Schema, Stream } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import { Socket } from "effect/unstable/socket";
import { afterEach, describe, expect, it } from "vite-plus/test";

import { Counter, Label } from "../../../examples/cloudflare/src/contract.ts";
import type { CounterObject } from "../../../examples/cloudflare/src/worker.ts";

const bindings = env as { COUNTERS: DurableObjectNamespace<InstanceType<typeof CounterObject>> };
const address = Counter.address("test");
const header = { protocolVersion: 1, schemaVersion: 1, address };
const stub = () => bindings.COUNTERS.getByName("counter:test");
const Json = Schema.decodeUnknownSync(Schema.Json);

const RpcExit = Schema.Array(
  Schema.Struct({
    _tag: Schema.Literal("Exit"),
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

  return Schema.decodeUnknownSync(Schema.toCodecJson(Counter.envelope.snapshotFrame))(result.value);
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
  it("converges headless clients and recovers an exact result lost after a durable commit", async () => {
    const definition = Client.definition(Counter, {
      applyEvent: (_snapshot, event) => event,
      optimistic: { set: (_snapshot, value) => ({ value }) },
      plugins: [
        Client.plugin(Label, {
          applyEvent: (_snapshot, event) => event,
          optimistic: { rename: (_snapshot, text) => ({ text }) },
        }),
      ],
    });

    const open = Effect.fn("test.openClient")(function* (actorId: string, loseResponse: boolean) {
      const upgraded = yield* Effect.promise(() =>
        SELF.fetch("https://example.test/sync/counters/test", {
          headers: { upgrade: "websocket", authorization: `Bearer ${actorId}-local` },
        }),
      );

      const websocket = upgraded.webSocket;

      if (websocket === null) return yield* Effect.die("Upgrade failed");
      websocket.accept();

      const protocol = RpcClient.layerProtocolSocket({ retryTransientErrors: false }).pipe(
        Layer.provide(
          Layer.effect(
            Socket.Socket,
            Socket.fromWebSocket(
              Effect.acquireRelease(Effect.succeed(websocket), (socket) =>
                Effect.sync(() => socket.close(1000)),
              ),
            ),
          ),
        ),
        Layer.provide(RpcSerialization.layerJson),
      );

      const services = yield* Layer.build(protocol);
      const transport = yield* Client.rpcTransport(Counter).pipe(Effect.provideContext(services));

      const client = yield* Client.make(definition, {
        actorId,
        persistence: { mode: "volatile" },
        transport: {
          ...transport,
          execute: (command) =>
            transport.execute(command).pipe(
              Effect.filterOrFail(
                () => !loseResponse,
                () =>
                  ProtocolError.make({
                    reason: "Unavailable",
                    message: "Response lost after commit",
                  }),
              ),
            ),
        },
      });

      return yield* client.open(address);
    });

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const alice = yield* open("alice", true);
          const bob = yield* open("bob", false);

          yield* alice.ready;
          yield* bob.ready;
          const result = yield* Effect.result(alice.execute(Counter.actions.set, 7));

          if (
            result._tag !== "Failure" ||
            !Schema.is(ClientError)(result.failure) ||
            result.failure.commandId === undefined
          ) {
            return yield* Effect.die("Expected an ambiguous command with retained identity");
          }
          yield* bob.changes.pipe(
            Stream.filter((state) => state.value?.source.value === 7),
            Stream.take(1),
            Stream.runDrain,
          );
          yield* Effect.promise(() => evictDurableObject(stub()));
          expect(yield* alice.retry(result.failure.commandId)).toEqual({ previous: 0, current: 7 });
          expect(yield* bob.execute(Counter.plugins.label.actions.rename, "shared")).toBe("shared");
          yield* alice.changes.pipe(
            Stream.filter((state) => state.value?.plugins.label.text === "shared"),
            Stream.take(1),
            Stream.runDrain,
          );
          expect((yield* alice.read).value).toEqual((yield* bob.read).value);
          expect((yield* alice.read).authoritative?.position.cursor).toBe(2);
        }),
      ),
    );
  });

  it("serves the source's generated Effect RPC client through the authenticated Worker", async () => {
    const transport = RpcClient.layerProtocolHttp({
      url: "https://example.test/sync/counters/test",
    }).pipe(Layer.provide(FetchHttpClient.layer), Layer.provide(RpcSerialization.layerJson));

    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* RpcClient.make(Counter.rpc, { flatten: true });

        const initial = yield* client("snapshot", {
          ...header,
          protocolVersion: 1,
          schemaVersion: 1,
        });

        const outcome = yield* client("execute/$source/set", {
          ...header,
          protocolVersion: 1,
          schemaVersion: 1,
          namespace: "$source",
          action: "set",
          commandId: "native",
          admittedGeneration: initial.position.sourceAuthorityGeneration,
          payload: 12,
        });

        expect(outcome).toEqual({
          _tag: "Succeeded",
          result: { previous: 0, current: 12 },
          position: { ...initial.position, cursor: 1 },
        });
      }).pipe(
        Effect.provide(transport),
        Effect.provideService(FetchHttpClient.Fetch, (input, init) => {
          const request = new Request(input, init);

          request.headers.set("authorization", "Bearer alice-local");
          request.headers.set("x-yielded-session", JSON.stringify({ actorId: "bob" }));

          return SELF.fetch(request);
        }),
        Effect.scoped,
      ),
    );

    const stored = await runInDurableObject(stub(), (_instance, state) =>
      state.storage.sql.exec("SELECT actor FROM sync_receipts").one(),
    );

    expect(stored).toEqual({ actor: "alice" });
  });

  it("returns the original result after a lost response, later changes and restart; isolates actors and conflicting ids", async () => {
    const initial = await snapshot();
    const first = command(initial.position.sourceAuthorityGeneration, "first", 7);
    const original = await invoke("execute/$source/set", first);

    expect(original).toEqual({
      _tag: "Success",
      value: {
        _tag: "Succeeded",
        result: { previous: 0, current: 7 },
        position: { ...initial.position, cursor: 1 },
      },
    });
    await invoke(
      "execute/$source/set",
      command(initial.position.sourceAuthorityGeneration, "second", 9),
    );
    await evictDurableObject(stub());
    expect(await invoke("execute/$source/set", first)).toEqual(original);
    expect(await invoke("result/$source/set", first)).toEqual({
      _tag: "Success",
      value: { _tag: "Found", outcome: original._tag === "Success" ? original.value : null },
    });
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

  it("serializes concurrent retries and preserves exact domain rejections", async () => {
    const initial = await snapshot();
    const request = command(initial.position.sourceAuthorityGeneration, "parallel", 4);

    expect(
      await invoke("execute/$source/set", { ...request, admittedGeneration: null }),
    ).toMatchObject({ _tag: "Failure", cause: [{ error: { reason: "AuthorityMismatch" } }] });
    expect(
      await invoke("execute/$source/set", {
        ...request,
        admittedGeneration: "09321600-2e2a-4a46-bb6d-0b7112a7f155",
      }),
    ).toMatchObject({ _tag: "Failure", cause: [{ error: { reason: "AuthorityMismatch" } }] });

    const results = await Promise.all([
      invoke("execute/$source/set", request),
      invoke("execute/$source/set", request),
    ]);

    expect(results[0]).toEqual(results[1]);
    expect((await snapshot()).position.cursor).toBe(1);
    const refused = command(initial.position.sourceAuthorityGeneration, "refused", 101);
    const rejection = await invoke("execute/$source/set", refused);

    expect(rejection).toEqual({
      _tag: "Success",
      value: { _tag: "Rejected", error: { _tag: "InvalidValue", maximum: 100 } },
    });
    await invoke(
      "execute/$source/set",
      command(initial.position.sourceAuthorityGeneration, "later", 8),
    );
    await evictDurableObject(stub());
    expect(await invoke("execute/$source/set", refused)).toEqual(rejection);
    expect((await snapshot()).position.cursor).toBe(2);
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

  it("keeps plugin private state out of snapshots and receipts after replay retention", async () => {
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
    const current = await snapshot();

    expect(current.snapshot).toEqual({
      source: { value: 6 },
      plugins: { label: { text: "Example" } },
    });
    expect(await invoke("execute/label/rename", rename)).toMatchObject({
      _tag: "Success",
      value: { result: "Example", position: { cursor: 1 } },
    });

    const rows = await runInDurableObject(stub(), (_instance, state) =>
      state.storage.sql
        .exec(
          "SELECT (SELECT COUNT(*) FROM sync_events) AS events, (SELECT COUNT(*) FROM sync_receipts) AS receipts",
        )
        .one(),
    );

    expect(rows).toEqual({ events: 4, receipts: 7 });
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

  it("supports generated streaming RPC clients and acknowledgements across hibernation", async () => {
    const upgraded = await SELF.fetch("https://example.test/sync/counters/test", {
      headers: { upgrade: "websocket", authorization: "Bearer alice-local" },
    });

    const websocket = upgraded.webSocket;

    if (websocket === null) throw new Error("Upgrade failed");
    websocket.accept();

    const transport = RpcClient.layerProtocolSocket({ retryTransientErrors: false }).pipe(
      Layer.provide(
        Layer.effect(
          Socket.Socket,
          Socket.fromWebSocket(
            Effect.acquireRelease(Effect.succeed(websocket), (socket) =>
              Effect.sync(() => socket.close(1000)),
            ),
          ),
        ),
      ),
      Layer.provide(RpcSerialization.layerJson),
    );

    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* RpcClient.make(Counter.rpc, { flatten: true });
        const ready = yield* Deferred.make<void>();

        const frames = yield* client("subscribe", {
          ...header,
          protocolVersion: 1,
          schemaVersion: 1,
        }).pipe(
          Stream.tap(() => Deferred.succeed(ready, undefined)),
          Stream.take(3),
          Stream.runCollect,
          Effect.forkScoped,
        );

        yield* Deferred.await(ready);

        const initial = yield* client("snapshot", {
          ...header,
          protocolVersion: 1,
          schemaVersion: 1,
        });

        yield* Effect.promise(() => evictDurableObject(stub()));
        expect(
          yield* client("execute/$source/set", {
            ...header,
            protocolVersion: 1,
            schemaVersion: 1,
            namespace: "$source",
            action: "set",
            admittedGeneration: initial.position.sourceAuthorityGeneration,
            commandId: "streamed",
            payload: 8,
          }),
        ).toMatchObject({ _tag: "Succeeded", result: { previous: 0, current: 8 } });
        yield* client("publishMessage", {
          ...header,
          protocolVersion: 1,
          schemaVersion: 1,
          message: { namespace: "$source", payload: { editing: true } },
        });
        expect((yield* Fiber.join(frames)).map((frame) => frame._tag)).toEqual([
          "Snapshot",
          "Event",
          "Message",
        ]);
      }).pipe(Effect.provide(transport), Effect.scoped),
    );
  });

  it("recovers gaps and hibernated sessions, while messages and departures consume no cursor", async () => {
    const initial = await snapshot();
    const client = await connect();

    client.send("subscribe", { ...header, after: initial.position });
    expect(await client.next()).toMatchObject({
      _tag: "Chunk",
      values: [{ _tag: "Events", events: [], position: initial.position }],
    });
    await evictDurableObject(stub());
    client.send(
      "publishMessage",
      { ...header, message: { namespace: "$source", payload: { editing: true } } },
      "message",
    );
    expect(await client.next()).toMatchObject({
      _tag: "Chunk",
      values: [{ _tag: "Message", actorId: "alice", message: { payload: { editing: true } } }],
    });
    expect(await client.next()).toMatchObject({
      _tag: "Exit",
      requestId: "message",
      exit: { _tag: "Success" },
    });
    expect((await snapshot()).position).toEqual(initial.position);
    const peer = await connect();

    peer.send("subscribe", header);
    await peer.next();
    peer.socket.close();
    expect(await client.next()).toMatchObject({
      _tag: "Chunk",
      values: [{ _tag: "MessageLeave", actorId: "alice" }],
    });
    expect((await snapshot()).position).toEqual(initial.position);
    client.socket.close();
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
    expect((await snapshot()).snapshot.source.value).toBe(6);
    stale.socket.close();
  });

  it("fences an expired hibernated identity before sending or accepting messages", async () => {
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
    expect((await snapshot()).position.cursor).toBe(0);
  });

  it("rearms and recovers durable outbox work after eviction", async () => {
    const initial = await snapshot();

    await invoke(
      "execute/$source/set",
      command(initial.position.sourceAuthorityGeneration, "outbox", 3),
    );
    await evictDurableObject(stub());
    await runDurableObjectAlarm(stub());

    const pending = await runInDurableObject(stub(), (_instance, state) =>
      state.storage.sql.exec("SELECT COUNT(*) AS count FROM sync_outbox").one(),
    );

    expect(pending).toEqual({ count: 0 });
    expect((await snapshot()).snapshot.source.value).toBe(3);
  });
});
