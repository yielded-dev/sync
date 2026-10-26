/// <reference lib="dom" />

import { Action, Source } from "@yielded/sync";
import { Server, ServerCrypto, SourceStorage, StorageError } from "@yielded/sync/server";
import { Crypto, DateTime, Deferred, Effect, Fiber, PlatformError, Schema, Stream } from "effect";
import { Hex } from "effect/encoding";

import { makeStorage } from "./storage.ts";

const Counter = Source.make({
  kind: "generic-counter",
  schemaVersion: 1,
  snapshot: Schema.Int,
  event: Schema.Int,
  actions: [Action.make("add", { payload: Schema.Int, success: Schema.Int, error: Schema.Never })],
});

const definition = Server.make(Counter, {
  principal: Schema.Struct({ actorId: Schema.String }),
  state: Schema.Int,
  initialize: Effect.succeed(0),
  snapshot: (state) => state,
  authorize: () => Effect.void,
  actions: {
    add: ({ state, payload }) => {
      const next = state + payload;

      return Effect.succeed(Server.commit({ state: next, events: [next], result: next }));
    },
  },
});

const webCrypto = Crypto.make({
  randomBytes: (size) => globalThis.crypto.getRandomValues(new Uint8Array(size)),
  digest: (algorithm, data) =>
    Effect.tryPromise({
      try: async () =>
        new Uint8Array(await globalThis.crypto.subtle.digest(algorithm, new Uint8Array(data))),
      catch: (cause) =>
        PlatformError.systemError({ _tag: "Unknown", module: "Crypto", method: "digest", cause }),
    }),
});

const cryptoService = ServerCrypto.of({
  generation: webCrypto
    .randomUUIDv4()
    .pipe(
      Effect.mapError((cause) =>
        StorageError.make({ message: "Could not generate authority", cause }),
      ),
    ),
  sha256: (value) =>
    webCrypto.digest("SHA-256", new TextEncoder().encode(value)).pipe(
      Effect.map(Hex.encode),
      Effect.mapError((cause) =>
        StorageError.make({ message: "Could not fingerprint command", cause }),
      ),
    ),
});

const program = Effect.scoped(
  Effect.gen(function* () {
    const storage = yield* makeStorage;

    const host = yield* Server.host(definition).pipe(
      Effect.provideService(SourceStorage, storage),
      Effect.provideService(ServerCrypto, cryptoService),
    );

    const address = Counter.address("demo");
    const now = DateTime.toEpochMillis(yield* DateTime.now);

    // A real application authenticates before constructing this trusted session.
    const connection = yield* host.connect({
      address,
      session: {
        principal: { actorId: "alice" },
        actorId: "alice",
        connectionId: "local",
        expiresAtMillis: now + 60_000,
      },
    });

    const snapshot = yield* host.snapshot(connection, address);
    const subscribed = yield* Deferred.make<void>();

    const frames = yield* host.subscribe(connection, address).pipe(
      Stream.tap((frame) =>
        frame._tag === "Snapshot" ? Deferred.succeed(subscribed, undefined) : Effect.void,
      ),
      Stream.take(2),
      Stream.runCollect,
      Effect.forkScoped,
    );

    yield* Deferred.await(subscribed);

    const command = yield* Schema.encodeEffect(Schema.toCodecJson(Counter.actions.add.command))({
      protocolVersion: 1,
      schemaVersion: 1,
      address,
      commandId: "first-add",
      admittedGeneration: snapshot.position.sourceAuthorityGeneration,
      namespace: "$source",
      action: "add",
      payload: 3,
    });

    const first = yield* host.dispatch(connection, address, command);
    const retry = yield* host.dispatch(connection, address, command);
    const lookup = yield* host.lookup(connection, address, command);
    const delivered = yield* Fiber.join(frames);
    const current = yield* host.snapshot(connection, address);

    yield* Effect.log({
      initial: snapshot.snapshot.source,
      frames: delivered.map((frame) => frame._tag),
      first,
      retry,
      lookup,
      current: current.snapshot.source,
      cursor: current.position.cursor,
    });
  }),
);

await Effect.runPromise(program);
