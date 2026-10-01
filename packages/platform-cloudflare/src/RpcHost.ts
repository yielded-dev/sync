import { ProtocolError, type Source, type SourceAddress, type SourcePosition } from "@yielded/sync";
import { Context, Deferred, Effect, Schema, Stream } from "effect";
import { type Rpc, type RpcGroup, RpcSerialization, type RpcServer } from "effect/unstable/rpc";

import { unavailable, type Connection, type Hub } from "./SourceHub.ts";

// Only the authenticated native event supplies this context; RPC headers cannot set it.
export const CurrentConnection = Context.Reference<Connection | undefined>(
  "@yielded/sync-platform-cloudflare/Connection",
  { defaultValue: () => undefined },
);

const connection = CurrentConnection.pipe(
  Effect.filterOrFail(
    (value): value is Connection => value !== undefined,
    () => ProtocolError.make({ reason: "Unauthenticated", message: "Missing source session" }),
  ),
);

/** Bound the transport input while retaining Effect RPC's serializer and envelope semantics. */
export const serialization = (maxBytes: number): RpcSerialization.RpcSerialization["Service"] => ({
  ...RpcSerialization.json,
  makeUnsafe: () => {
    const parser = RpcSerialization.json.makeUnsafe();

    return {
      encode: parser.encode,
      decode: (data) => {
        if (
          (typeof data === "string" ? new TextEncoder().encode(data).byteLength : data.byteLength) >
          maxBytes
        )
          throw new RpcSerialization.MaxBufferSizeExceeded({ maxBufferSize: maxBytes });
        const messages = parser.decode(data);

        if (messages.length > 32) throw new RangeError("RPC batch exceeds 32 messages");

        return messages;
      },
    };
  },
});

type Rpcs = RpcGroup.Rpcs<Source.Definition["rpc"]>;
type Handlers = { [R in Rpcs as R["_tag"]]: Rpc.ToHandlerFn<R, never> };
type Payload = {
  readonly address: SourceAddress;
  readonly after?: SourcePosition;
  readonly message?: unknown;
};

/** Domain handlers are registered against the same schemas that built the public RPC group. */
export const handlers = (hub: Hub) => {
  const contract = hub.contract;

  const entries: Record<
    string,
    (
      payload: Payload,
    ) => Effect.Effect<unknown, ProtocolError> | Stream.Stream<unknown, ProtocolError>
  > = {
    snapshot: (input) =>
      Effect.flatMap(connection, (current) =>
        hub.run(current, input.address, (runtime) => runtime.snapshot(current.session)),
      ),
    subscribe: (input) =>
      Stream.unwrap(
        Effect.map(connection, (current) => hub.subscribe(current, input.address, input.after)),
      ),
    publishMessage: (input) =>
      Effect.gen(function* () {
        const current = yield* connection;

        const message = yield* Schema.encodeUnknownEffect(Schema.toCodecJson(contract.message))(
          input.message,
        ).pipe(Effect.mapError(unavailable));

        yield* hub.publishMessage(current, input.address, message);
      }),
  };

  const actions = [
    ...Object.values(contract.actions),
    ...Object.values(contract.plugins).flatMap((plugin) => Object.values(plugin.actions)),
  ];

  for (const action of actions) {
    entries[action.execute._tag] = (input) =>
      Effect.gen(function* () {
        const current = yield* connection;

        const command = yield* Schema.encodeUnknownEffect(Schema.toCodecJson(action.command))(
          input,
        ).pipe(Effect.mapError(unavailable));

        const outcome = yield* hub.dispatch(current, input.address, command);

        return yield* Schema.decodeEffect(Schema.toCodecJson(action.outcome))(outcome).pipe(
          Effect.mapError(unavailable),
        );
      });
    entries[action.result._tag] = (input) =>
      Effect.gen(function* () {
        const current = yield* connection;

        const command = yield* Schema.encodeUnknownEffect(Schema.toCodecJson(action.command))(
          input,
        ).pipe(Effect.mapError(unavailable));

        const result = yield* hub.run(current, input.address, (runtime) =>
          runtime.lookup(current.session, command),
        );

        return yield* Schema.decodeEffect(Schema.toCodecJson(action.lookup))(result).pipe(
          Effect.mapError(unavailable),
        );
      });
  }

  // Iteration erases the tag/schema relationship. Each entry above uses its registered action's codecs.
  return { group: contract.rpc, services: contract.rpc.toHandlers(entries as unknown as Handlers) };
};

/** Keep a native message event alive until its request produces a response or first stream batch. */
export const withEventLifetime = (
  base: RpcServer.Protocol["Service"],
): RpcServer.Protocol["Service"] => {
  const pending = new Map<number, Map<string | number, Set<Deferred.Deferred<void>>>>();

  const complete = (clientId: number, requestId?: string | number) =>
    Effect.gen(function* () {
      const requests = pending.get(clientId);

      if (requests === undefined) return;

      const signals =
        requestId === undefined
          ? [...requests.values()].flatMap((values) => [...values])
          : [...(requests.get(requestId) ?? [])];

      for (const signal of signals) yield* Deferred.succeed(signal, undefined);
    });

  return {
    ...base,
    run: (write) =>
      base.run((clientId, request) => {
        if (request._tag !== "Request" || request.isNotification === true)
          return write(clientId, request);

        return Effect.gen(function* () {
          const signal = yield* Deferred.make<void>();
          let requests = pending.get(clientId);

          if (requests === undefined) pending.set(clientId, (requests = new Map()));
          const id = request.id;
          let signals = requests.get(id);

          if (signals === undefined) requests.set(id, (signals = new Set()));
          signals.add(signal);
          yield* write(clientId, request).pipe(
            Effect.andThen(Deferred.await(signal)),
            Effect.ensuring(
              Effect.sync(() => {
                signals.delete(signal);
                if (signals.size === 0) requests.delete(id);
                if (requests.size === 0) pending.delete(clientId);
              }),
            ),
          );
        });
      }),
    send: (clientId, response, transferables) =>
      base
        .send(clientId, response, transferables)
        .pipe(
          Effect.andThen(
            response._tag === "Chunk" || response._tag === "Exit"
              ? complete(clientId, response.requestId)
              : response._tag === "Defect" || response._tag === "ClientProtocolError"
                ? complete(clientId)
                : Effect.void,
          ),
        ),
    end: (clientId) => base.end(clientId).pipe(Effect.ensuring(complete(clientId))),
  };
};
