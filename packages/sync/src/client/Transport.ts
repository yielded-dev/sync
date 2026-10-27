/**
 * The portable JSON transport service and its Effect RPC implementation.
 *
 * @since 0.1.0-beta.0
 */

import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Url from "effect/http/Url";
import * as UrlParams from "effect/http/UrlParams";
import * as Layer from "effect/Layer";
import * as RpcClient from "effect/rpc/RpcClient";
import * as RpcSerialization from "effect/rpc/RpcSerialization";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Socket from "effect/socket/Socket";
import * as Stream from "effect/Stream";

import { ProtocolError, type SourceAddress, type SourcePosition } from "../Model.ts";
import * as Source from "../Source.ts";
import type { EncodedCommand } from "./Model.ts";

/**
 * The protocol version, source address, and application schema version
 * attached to source operations.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Request {
  readonly protocolVersion: 1;
  readonly address: SourceAddress;
  readonly schemaVersion: number;
}

/**
 * JSON codecs are checked on both sides of this portable transport port.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Transport {
  readonly snapshot: (request: Request) => Effect.Effect<Schema.Json, ProtocolError>;
  readonly subscribe: (
    request: Request & { readonly after?: SourcePosition },
  ) => Stream.Stream<Schema.Json, ProtocolError>;
  readonly execute: (command: EncodedCommand) => Effect.Effect<Schema.Json, ProtocolError>;
  readonly result: (command: EncodedCommand) => Effect.Effect<Schema.Json, ProtocolError>;
  readonly publishMessage: (
    request: Request & { readonly message: Schema.Json },
  ) => Effect.Effect<void, ProtocolError>;
}

/**
 * Transport implementations capture their dependencies in their construction Layer.
 *
 * @category services
 * @since 0.1.0-beta.0
 */
export const Transport: Context.Service<Transport, Transport> = Context.Service(
  "@yielded/sync/client/Transport",
);

/**
 * The application provides RpcClient.Protocol and its HTTP/socket services.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
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

/**
 * Requires the application's scoped RPC protocol, route, and credentials.
 *
 * @category layers
 * @since 0.1.0-beta.0
 */
export const layerRpcTransport = <S extends Source.Spec>(contract: Source.Definition<S>) =>
  Layer.effect(Transport, rpcTransport(contract));

/**
 * A WebSocket URL or an Effect that resolves it for a transport connection.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface WebSocketTransportOptions {
  readonly url: string | Effect.Effect<string>;
}

/**
 * A source origin, schema-validated identity, and optional query parameters.
 * The transport derives the WebSocket URL from the shared source route.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface SourceWebSocketOptions<S extends Source.Spec = Source.Spec> {
  readonly origin: string;
  readonly id: Source.Id<S>;
  /** Application query parameters, such as short-lived connection credentials. */
  readonly query?: UrlParams.Input;
}

/**
 * The application supplies the socket constructor, credentials, and session lifetime.
 *
 * @category layers
 * @since 0.1.0-beta.0
 */
export function layerWebSocketTransport<S extends Source.Spec>(
  contract: Source.Definition<S>,
  options: WebSocketTransportOptions,
): Layer.Layer<Transport, never, Socket.WebSocketConstructor>;

export function layerWebSocketTransport<S extends Source.Spec>(
  contract: Source.Definition<S>,
  options: WebSocketTransportOptions | SourceWebSocketOptions<NoInfer<S>>,
): Layer.Layer<Transport, ProtocolError, Socket.WebSocketConstructor>;

export function layerWebSocketTransport<S extends Source.Spec>(
  contract: Source.Definition<S>,
  options: WebSocketTransportOptions | SourceWebSocketOptions<S>,
): Layer.Layer<Transport, ProtocolError, Socket.WebSocketConstructor> {
  const transport = (url: WebSocketTransportOptions["url"]) =>
    layerRpcTransport(contract).pipe(
      Layer.provide(RpcClient.layerProtocolSocket({ retryTransientErrors: false })),
      Layer.provide(RpcSerialization.layerJson),
      Layer.provide(Socket.layerWebSocket(url)),
    );

  if ("url" in options) return transport(options.url);

  const url = Effect.gen(function* () {
    const path = yield* Source.pathFor(contract, options.id);
    const url = yield* Effect.fromResult(Url.fromString(options.origin));

    if (!["http:", "https:", "ws:", "wss:"].includes(url.protocol))
      return yield* ProtocolError.make({
        reason: "UnsupportedVersion",
        message: "Invalid source origin",
      });

    url.protocol = url.protocol === "https:" || url.protocol === "wss:" ? "wss:" : "ws:";
    url.pathname = path;
    url.search = "";
    url.hash = "";

    return Url.setUrlParams(url, UrlParams.fromInput(options.query ?? {})).href;
  }).pipe(
    Effect.mapError(() =>
      ProtocolError.make({ reason: "UnsupportedVersion", message: "Invalid source URL" }),
    ),
  );

  return Layer.unwrap(Effect.map(url, transport));
}
