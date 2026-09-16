import { Plugin, RegistrationError, Source } from "@yielded/sync";
import { Server } from "@yielded/sync/server";
import { Effect, Layer, Schema } from "effect";
import { expect, it } from "vite-plus/test";

const slot = {
  snapshot: Schema.Struct({}),
  event: Schema.Never,
  message: Schema.Never,
  actions: [],
};

const first = Plugin.make("first", slot);
const second = Plugin.make("second", slot);

const contract = Source.make({
  ...slot,
  kind: "lifecycle",
  schemaVersion: 1,
  plugins: [first, second],
});

const definition = {
  principal: Schema.Struct({}),
  state: Schema.Struct({}),
  initialize: Effect.succeed({}),
  snapshot: () => ({}),
  actions: {},
};

it("acquires plugin services in contract order and releases them in reverse when the source scope closes", async () => {
  const lifecycle: Array<string> = [];

  const services = (name: string) =>
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

  const firstServer = Server.providePlugin(Server.plugin(first, definition), services("first"));
  const secondServer = Server.providePlugin(Server.plugin(second, definition), services("second"));

  const server = Server.provide(
    Server.make(contract, {
      ...definition,
      authorize: () => Effect.void,
      plugins: [secondServer, firstServer],
    }),
    services("root"),
  );

  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        yield* server.acquire;
        expect(lifecycle).toEqual(["open root", "open first", "open second"]);
      }),
    ),
  );
  expect(lifecycle).toEqual([
    "open root",
    "open first",
    "open second",
    "close second",
    "close first",
    "close root",
  ]);
});

it("rejects dynamic extra handlers and mismatched plugin definitions during construction", () => {
  const extra = { ...definition, actions: { unexpected: () => Effect.void } } as typeof definition;

  expect(() => Server.plugin(first, extra)).toThrow(RegistrationError);
  const wrong = Plugin.make("first", slot);
  const implementations = [Server.plugin(wrong, definition), Server.plugin(second, definition)];

  expect(() =>
    Server.make(contract, {
      ...definition,
      authorize: () => Effect.void,
      plugins: implementations,
    }),
  ).toThrow(RegistrationError);
});
