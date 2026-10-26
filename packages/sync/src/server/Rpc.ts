import { Context, Effect, Schema, Stream } from "effect";
import { type Rpc, type RpcGroup, RpcSerialization } from "effect/rpc";

import { ProtocolError, type SourceAddress, type SourcePosition } from "../Model.ts";
import type * as Source from "../Source.ts";
import type { Connection, Host } from "./Host.ts";

const unavailable = () =>
  ProtocolError.make({ reason: "Unavailable", message: "Source temporarily unavailable" });

// Only an authenticated host event supplies this context; RPC headers cannot set it.
export const CurrentConnection = Context.Reference<Connection | undefined>(
  "@yielded/sync/server/Rpc/CurrentConnection",
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

type Rpcs<S extends Source.Spec> = RpcGroup.Rpcs<Source.Definition<S>["rpc"]>;
type Handlers<S extends Source.Spec> = {
  [R in Rpcs<S> as R["_tag"]]: Rpc.ToHandlerFn<R, never>;
};
type Payload = {
  readonly address: SourceAddress;
  readonly after?: SourcePosition;
  readonly message?: unknown;
};

/** Domain handlers are registered against the same schemas that built the public RPC group. */
export const handlers = <S extends Source.Spec>(
  host: Host<S>,
): {
  readonly group: Source.Definition<S>["rpc"];
  readonly services: Effect.Effect<Context.Context<Rpc.ToHandler<Rpcs<S>>>>;
} => {
  // Reflection uses the same contract's schemas; the public group retains S.
  const contract = host.contract as unknown as Source.Definition;

  const entries: Record<
    string,
    (
      payload: Payload,
    ) => Effect.Effect<unknown, ProtocolError> | Stream.Stream<unknown, ProtocolError>
  > = {
    snapshot: (input) =>
      Effect.flatMap(connection, (current) => host.snapshot(current, input.address)),
    subscribe: (input) =>
      Stream.unwrap(
        Effect.map(connection, (current) => host.subscribe(current, input.address, input.after)),
      ),
    publishMessage: (input) =>
      Effect.gen(function* () {
        const current = yield* connection;

        const message = yield* Schema.encodeUnknownEffect(Schema.toCodecJson(contract.message))(
          input.message,
        ).pipe(Effect.mapError(unavailable));

        yield* host.publishMessage(current, input.address, message);
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

        const outcome = yield* host.dispatch(current, input.address, command);

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

        const result = yield* host.lookup(current, input.address, command);

        return yield* Schema.decodeEffect(Schema.toCodecJson(action.lookup))(result).pipe(
          Effect.mapError(unavailable),
        );
      });
  }

  // Iteration erases the tag/schema relationship. Each entry above uses its registered action's codecs.
  const services = contract.rpc.toHandlers(entries as unknown as Handlers<Source.Spec>);

  return {
    group: host.contract.rpc,
    services: Effect.map(
      services,
      (context) => context as unknown as Context.Context<Rpc.ToHandler<Rpcs<S>>>,
    ),
  };
};
