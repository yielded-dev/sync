import {
  Action,
  Plugin,
  RegistrationError,
  Source,
  SourceCatalog,
  SourcePosition,
  comparePositions,
} from "@yielded/sync";
import { DateTime, Effect, Schema, Stream } from "effect";
import { RpcTest } from "effect/unstable/rpc";
import { describe, expect, it } from "vite-plus/test";

class Refused extends Schema.TaggedError<Refused>()("Refused", { reason: Schema.String }) {}

const rename = Action.make("rename", {
  payload: Schema.Struct({ text: Schema.String }),
  success: Schema.Struct({ text: Schema.String, at: Schema.DateTimeUtc }),
  error: Refused,
});

const label = Plugin.make("label", {
  snapshot: Schema.Struct({ text: Schema.String }),
  event: Schema.Struct({ text: Schema.String }),
  message: Schema.Never,
  actions: [rename],
});

const counter = Source.make({
  kind: "counter",
  schemaVersion: 1,
  snapshot: Schema.Struct({ value: Schema.Int, at: Schema.DateTimeUtc }),
  event: Schema.Struct({ value: Schema.Int, at: Schema.DateTimeUtc }),
  message: Schema.Struct({ editing: Schema.Boolean }),
  actions: [
    Action.make("rename", { payload: Schema.Int, success: Schema.Int, error: Schema.Never }),
  ],
  plugins: [label],
});

const generation = "6d5f7444-dba4-4a4c-a343-ce7e7dac6e88";
const position = { sourceAuthorityGeneration: generation, cursor: 7 };

const header = {
  protocolVersion: 1 as const,
  address: counter.address("demo"),
  schemaVersion: 1 as const,
};

const instant = DateTime.makeUnsafe("2026-09-16T12:00:00Z");

const renameCommand = {
  ...header,
  commandId: "rename-1",
  admittedGeneration: generation,
  namespace: "label" as const,
  action: "rename" as const,
  payload: { text: "Demo" },
};

describe("public source contracts", () => {
  it("rejects dynamic collisions before a registry can overwrite a registration", () => {
    const duplicateActions: Array<Action.Definition> = [rename, rename];
    const duplicatePlugins: Array<Plugin.Definition> = [label, label];
    const duplicateSources: Array<SourceCatalog.Entry> = [counter, counter];

    expect(() => Plugin.make("dynamic", { ...label.spec, actions: duplicateActions })).toThrow(
      RegistrationError,
    );
    expect(() => Source.make({ ...counter.spec, actions: duplicateActions })).toThrow(
      RegistrationError,
    );
    expect(() => Source.make({ ...counter.spec, plugins: duplicatePlugins })).toThrow(
      RegistrationError,
    );
    expect(() => SourceCatalog.make(duplicateSources)).toThrow(RegistrationError);

    for (const id of ["", "$source"] as Array<string>) {
      expect(() => Plugin.make(id, label.spec)).toThrow(RegistrationError);
    }

    for (const schemaVersion of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => Source.make({ ...counter.spec, schemaVersion })).toThrow(RegistrationError);
    }

    expect(() => Source.make({ ...counter.spec, kind: "" })).toThrow(RegistrationError);
    expect(() => Action.make("", rename)).toThrow(RegistrationError);
    expect(counter.actions.rename.payload).toBe(Schema.Int);
    expect(counter.plugins.label.actions.rename.payload).toBe(rename.payload);
  });

  it("keeps arbitrary registration names distinct in registries and RPC tags", () => {
    const first = Plugin.make("a/b", { ...label.spec, actions: [Action.make("c", rename)] });
    const second = Plugin.make("a", { ...label.spec, actions: [Action.make("b/c", rename)] });
    const third = Plugin.make("a%2Fb", { ...label.spec, actions: [Action.make("c", rename)] });

    const prototype = Plugin.make("__proto__", {
      ...label.spec,
      actions: [Action.make("constructor", rename)],
    });

    const source = Source.make({
      ...counter.spec,
      actions: [],
      plugins: [first, second, third, prototype],
    });

    expect([...source.rpc.requests.keys()]).toEqual([
      "snapshot",
      "subscribe",
      "publishMessage",
      "execute/a%2Fb/c",
      "result/a%2Fb/c",
      "execute/a/b%2Fc",
      "result/a/b%2Fc",
      "execute/a%252Fb/c",
      "result/a%252Fb/c",
      "execute/__proto__/constructor",
      "result/__proto__/constructor",
    ]);
    expect(source.plugins.__proto__.actions.constructor.name).toBe("constructor");
    expect(
      Schema.decodeUnknownSync(source.snapshot)({
        source: { value: 1, at: instant },
        plugins: Object.fromEntries(
          ["a/b", "a", "a%2Fb", "__proto__"].map((id) => [id, { text: id }]),
        ),
      }).plugins.__proto__,
    ).toEqual({ text: "__proto__" });
    expect(SourceCatalog.make([source]).get("missing")).toBeUndefined();
  });

  it("round-trips composed snapshots and frames through canonical JSON codecs", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const codec = Schema.toCodecJson(counter.envelope.outbound);

        const snapshot = {
          _tag: "Snapshot" as const,
          ...header,
          position,
          snapshot: { source: { value: 7, at: instant }, plugins: { label: { text: "Demo" } } },
        };

        const encoded = yield* Schema.encodeEffect(codec)(snapshot);

        expect(encoded).toEqual({
          ...snapshot,
          snapshot: {
            source: { value: 7, at: "2026-09-16T12:00:00.000Z" },
            plugins: { label: { text: "Demo" } },
          },
        });
        expect(yield* Schema.decodeEffect(codec)(encoded)).toEqual(snapshot);

        const event = {
          _tag: "Event" as const,
          ...header,
          event: {
            position,
            command: { actorId: "actor", commandId: "rename-1" },
            event: { namespace: "label" as const, payload: { text: "Demo" } },
          },
        };

        expect(yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(event))).toEqual(
          event,
        );

        const message = {
          _tag: "Message" as const,
          ...header,
          actorId: "actor",
          connectionId: "connection",
          message: { namespace: "$source" as const, payload: { editing: true } },
        };

        expect(yield* Schema.encodeEffect(codec)(message)).toEqual(message);
        expect(
          Schema.decodeResult(codec)({
            ...event,
            event: { ...event.event, event: { namespace: "label", payload: { value: 7 } } },
          })._tag,
        ).toBe("Failure");
        expect(
          Schema.decodeResult(codec)({
            ...message,
            message: { namespace: "label", payload: { editing: true } },
          })._tag,
        ).toBe("Failure");
        expect(Schema.decodeUnknownResult(codec)({ ...snapshot, schemaVersion: 2 })._tag).toBe(
          "Failure",
        );
      }),
    );
  });

  it("retains exact typed success, rejection, and unresolved lookup states", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const action = counter.plugins.label.actions.rename;
        const receiptCodec = Schema.toCodecJson(action.receipt);

        const receipt = {
          command: renameCommand,
          actorId: "actor",
          sourceAuthorityGeneration: generation,
          fingerprint: "a".repeat(64),
          outcome: { _tag: "Succeeded" as const, result: { text: "Demo", at: instant }, position },
        };

        const encoded = yield* Schema.encodeEffect(receiptCodec)(receipt);

        expect(encoded).toEqual({
          ...receipt,
          outcome: {
            _tag: "Succeeded",
            result: { text: "Demo", at: "2026-09-16T12:00:00.000Z" },
            position,
          },
        });
        expect(yield* Schema.decodeEffect(receiptCodec)(encoded)).toEqual(receipt);

        const lookupCodec = Schema.toCodecJson(action.lookup);

        const rejected = {
          _tag: "Found" as const,
          outcome: { _tag: "Rejected" as const, error: Refused.make({ reason: "locked" }) },
        };

        expect(yield* Schema.encodeEffect(lookupCodec)(rejected)).toEqual({
          _tag: "Found",
          outcome: { _tag: "Rejected", error: { _tag: "Refused", reason: "locked" } },
        });
        expect(
          yield* Schema.decodeEffect(lookupCodec)(
            yield* Schema.encodeEffect(lookupCodec)(rejected),
          ),
        ).toEqual(rejected);
        expect(yield* Schema.decodeEffect(lookupCodec)({ _tag: "Unknown" })).toEqual({
          _tag: "Unknown",
        });
        expect(yield* Schema.decodeEffect(lookupCodec)({ _tag: "Expired" })).toEqual({
          _tag: "Expired",
        });
        expect(
          Schema.decodeResult(action.execute.payloadSchema)({
            ...renameCommand,
            namespace: "$source",
          })._tag,
        ).toBe("Failure");
        expect(
          Schema.decodeResult(action.execute.payloadSchema)({
            ...renameCommand,
            address: { kind: "other", id: "demo" },
          })._tag,
        ).toBe("Failure");
        expect(
          Schema.decodeResult(action.result.successSchema)({
            _tag: "Found",
            outcome: { _tag: "Succeeded", position },
          })._tag,
        ).toBe("Failure");
      }),
    );
  });

  it("derives native RPC clients and handlers without mixing same-named actions", async () => {
    const layer = counter.rpc.toLayer({
      snapshot: () =>
        Effect.succeed({
          _tag: "Snapshot" as const,
          ...header,
          position,
          snapshot: { source: { value: 7, at: instant }, plugins: { label: { text: "Demo" } } },
        }),
      subscribe: () => Stream.empty,
      publishMessage: () => Effect.void,
      "execute/$source/rename": (input) =>
        Effect.succeed({ _tag: "Succeeded" as const, result: input.payload + 1, position }),
      "result/$source/rename": () => Effect.succeed({ _tag: "Unknown" as const }),
      "execute/label/rename": (input) =>
        Effect.succeed({
          _tag: "Succeeded" as const,
          result: { text: input.payload.text.toUpperCase(), at: instant },
          position,
        }),
      "result/label/rename": () =>
        Effect.succeed({
          _tag: "Found" as const,
          outcome: { _tag: "Rejected" as const, error: Refused.make({ reason: "locked" }) },
        }),
    });

    await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* RpcTest.makeClient(counter.rpc, { flatten: true });

        expect(
          yield* client("execute/$source/rename", {
            ...renameCommand,
            namespace: "$source",
            payload: 10,
          }),
        ).toEqual({ _tag: "Succeeded", result: 11, position });
        expect(yield* client("execute/label/rename", renameCommand)).toEqual({
          _tag: "Succeeded",
          result: { text: "DEMO", at: instant },
          position,
        });
        expect(yield* client("result/label/rename", renameCommand)).toEqual({
          _tag: "Found",
          outcome: { _tag: "Rejected", error: Refused.make({ reason: "locked" }) },
        });
      }).pipe(Effect.provide(layer), Effect.scoped),
    );
  });

  it("only orders safe positions within the same authority generation", () => {
    expect(comparePositions(position, { ...position, cursor: 8 })).toBe(-1);
    expect(comparePositions(position, position)).toBe(0);
    expect(comparePositions(position, { ...position, cursor: 6 })).toBe(1);
    expect(
      comparePositions(position, {
        sourceAuthorityGeneration: "09321600-2e2a-4a46-bb6d-0b7112a7f155",
        cursor: 0,
      }),
    ).toBeUndefined();

    for (const cursor of [-1, 0.5, Number.MAX_SAFE_INTEGER + 1, Infinity]) {
      expect(Schema.decodeResult(SourcePosition)({ ...position, cursor })._tag).toBe("Failure");
    }

    expect(
      Schema.decodeResult(SourcePosition)({
        ...position,
        sourceAuthorityGeneration: "not-a-uuid",
      })._tag,
    ).toBe("Failure");
  });
});
