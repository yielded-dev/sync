import { ProtocolError } from "@yielded/sync";
import { SELF, env, reset, runInDurableObject, evictDurableObject } from "cloudflare:test";
import { Effect, Layer, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import { afterEach, describe, expect, it } from "vite-plus/test";

import { Counter } from "../../../examples/cloudflare/src/contract.ts";
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
  it("uses authenticated Worker identity instead of a supplied session header", async () => {
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

        yield* client("execute/$source/set", {
          ...header,
          protocolVersion: 1,
          schemaVersion: 1,
          namespace: "$source",
          action: "set",
          commandId: "native",
          admittedGeneration: initial.position.sourceAuthorityGeneration,
          payload: 12,
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
