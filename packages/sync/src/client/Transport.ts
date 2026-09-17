import { Effect, Schema, Stream, type Scope } from "effect";
import { RpcClient } from "effect/unstable/rpc";

import { ProtocolError, type SourceAddress, type SourcePosition } from "../Model.ts";
import type * as Source from "../Source.ts";
import type { EncodedCommand } from "./Model.ts";

export interface Request {
  readonly protocolVersion: 1;
  readonly address: SourceAddress;
  readonly schemaVersion: number;
}

/** JSON codecs are checked on both sides of this portable transport port. */
export interface Transport<R = never> {
  readonly snapshot: (request: Request) => Effect.Effect<Schema.Json, ProtocolError, R>;
  readonly subscribe: (
    request: Request & { readonly after?: SourcePosition },
  ) => Stream.Stream<Schema.Json, ProtocolError, R>;
  readonly execute: (command: EncodedCommand) => Effect.Effect<Schema.Json, ProtocolError, R>;
  readonly result: (command: EncodedCommand) => Effect.Effect<Schema.Json, ProtocolError, R>;
  readonly publishMessage: (
    request: Request & { readonly message: Schema.Json },
  ) => Effect.Effect<void, ProtocolError, R>;
}

/** The application provides RpcClient.Protocol and its HTTP/socket services. */
export const rpcTransport = <S extends Source.Spec>(
  contract: Source.Definition<S>,
): Effect.Effect<Transport, never, Scope.Scope | RpcClient.Protocol> =>
  Effect.gen(function* () {
    const wire = contract as unknown as Source.Definition;
    const client = yield* RpcClient.make(wire.rpc, { flatten: true });

    // Tags and codecs come from the same validated RpcGroup. This single erasure
    // is private to dispatch; user-facing execute retains the bound action type.
    const call = client as unknown as (
      tag: string,
      payload: unknown,
    ) => Effect.Effect<unknown, ProtocolError>;

    const subscribe = client as unknown as (
      tag: "subscribe",
      payload: unknown,
    ) => Stream.Stream<unknown, ProtocolError>;

    const failed = () =>
      ProtocolError.make({ reason: "Unavailable", message: "RPC transport failed" });

    const actions = [
      ...Object.values(wire.actions),
      ...Object.values(wire.plugins).flatMap((plugin) => Object.values(plugin.actions)),
    ];

    const actionCall = Effect.fn("Client.rpcTransport.action")(function* (
      command: EncodedCommand,
      operation: "execute" | "result",
    ) {
      const action = actions.find(
        (a) => a.namespace === command.namespace && a.name === command.action,
      );

      if (action === undefined)
        return yield* ProtocolError.make({
          reason: "UnsupportedVersion",
          message: "Unknown action",
        });

      const payload = yield* Schema.decodeEffect(Schema.toCodecJson(action.command))(command).pipe(
        Effect.mapError(failed),
      );

      const value = yield* call(action[operation]._tag, payload).pipe(
        Effect.mapError((e) => (Schema.is(ProtocolError)(e) ? e : failed())),
      );

      return yield* Schema.encodeUnknownEffect(
        Schema.toCodecJson(operation === "execute" ? action.outcome : action.lookup),
      )(value).pipe(Effect.mapError(failed));
    });

    return {
      snapshot: (request) =>
        call("snapshot", request).pipe(
          Effect.flatMap(
            Schema.encodeUnknownEffect(Schema.toCodecJson(wire.envelope.snapshotFrame)),
          ),
          Effect.mapError((e) => (Schema.is(ProtocolError)(e) ? e : failed())),
        ),
      subscribe: (request) =>
        subscribe("subscribe", request).pipe(
          Stream.mapEffect((frame) =>
            Schema.encodeUnknownEffect(Schema.toCodecJson(wire.envelope.outbound))(frame),
          ),
          Stream.mapError((e) => (Schema.is(ProtocolError)(e) ? e : failed())),
        ),
      execute: (command) => actionCall(command, "execute"),
      result: (command) => actionCall(command, "result"),
      publishMessage: (request) =>
        Schema.decodeEffect(Schema.toCodecJson(wire.message))(request.message).pipe(
          Effect.flatMap((message) => call("publishMessage", { ...request, message })),
          Effect.asVoid,
          Effect.mapError((e) => (Schema.is(ProtocolError)(e) ? e : failed())),
        ),
    } satisfies Transport;
  });
