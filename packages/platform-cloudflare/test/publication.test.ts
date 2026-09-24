import type { SourcePosition } from "@yielded/sync";
import { env, reset } from "cloudflare:test";
import { Schema } from "effect";
import { afterEach, expect, it } from "vite-plus/test";

import { Publication, type PublicationObject } from "./fixtures/worker.ts";

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
    body: JSON.stringify({ _tag: "Request", id: "1", tag, payload }),
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
      (event) => resolve(Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Json))(event.data)),
      { once: true },
    ),
  );

  socket.send(
    JSON.stringify({
      _tag: "Request",
      id: "stream",
      tag: "subscribe",
      payload: { ...header, after },
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

  const status = await request("execute/$source/set", command).then(
    (response) => response.status,
    () => 500,
  );

  expect(status).toBeGreaterThanOrEqual(500);
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
