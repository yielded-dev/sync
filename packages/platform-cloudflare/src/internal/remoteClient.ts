import {
  type Action,
  ProtocolError,
  type Source,
} from "@yielded/sync";
import type { Client } from "@yielded/sync/client";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientError from "effect/http/HttpClientError";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as RpcClient from "effect/rpc/RpcClient";
import type * as RpcClientError from "effect/rpc/RpcClientError";
import * as RpcSerialization from "effect/rpc/RpcSerialization";

import type { prepareRequest } from "../Cloudflare.ts";

import type { ClientOptions, Command, RemoteClient } from "../Cloudflare.ts";

type RequestError = ProtocolError | RpcClientError.RpcClientError;

export type ClientConstructor = <S extends Source.Spec, E, R>(
  contract: Source.Definition<S>,
  options: ClientOptions<S["kind"], E, R>,
) => Effect.Effect<RemoteClient<S>, never, Scope.Scope | R>;

/** Internal composition avoids coupling the HTTP client back to the Cloudflare host. */
export const makeClient = (prepare: typeof prepareRequest): ClientConstructor =>
  Effect.fnUntraced(function* <S extends Source.Spec, E, R>(
    contract: Source.Definition<S>,
    options: ClientOptions<S["kind"], E, R>,
  ): Effect.fn.Return<RemoteClient<S>, never, Scope.Scope | R> {
    const context = yield* Effect.context<R>();

    const http = HttpClient.make((request, _url, signal) =>
      Effect.gen(function* () {
        const prepared = yield* prepare(contract, {
          request: yield* HttpClientRequest.toWeb(request, { signal }),
          address: options.address,
          identity: options.identity,
        });

        return HttpClientResponse.fromWeb(
          request,
          yield* options.fetch(prepared).pipe(Effect.provideContext(context)),
        );
      }).pipe(
        Effect.mapError(
          (cause) =>
            new HttpClientError.HttpClientError({
              reason: new HttpClientError.TransportError({ request, cause }),
            }),
        ),
      ),
    );

    const protocol = yield* Layer.build(
      RpcClient.layerProtocolHttp({ url: "https://sync.internal" }).pipe(
        Layer.provide(RpcSerialization.layerJson),
        Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
      ),
    );

    const wire = contract as unknown as Source.Definition;

    const rpc = yield* RpcClient.make(wire.rpc, { flatten: true }).pipe(
      Effect.provideContext(protocol),
    );

    // Dispatch uses only tags from this contract; its RPC codecs retain each
    // action's payload, outcome, and lookup relationship across this erasure.
    const call = rpc as unknown as <Value>(
      tag: string,
      payload: unknown,
    ) => Effect.Effect<Value, RequestError>;

    const header = {
      protocolVersion: 1,
      schemaVersion: contract.schemaVersion,
      address: options.address,
    };

    const actions = [
      ...Object.values(wire.actions),
      ...Object.values(wire.plugins).flatMap((plugin) => Object.values(plugin.actions)),
    ];

    const actionCall = <Value>(
      action: Client.Actions<S>,
      command: Command<Action.Definition>,
      operation: "execute" | "result",
    ): Effect.Effect<Value, RequestError> => {
      const bound = actions.find((candidate): boolean => candidate === action);

      if (bound === undefined)
        return Effect.fail(
          ProtocolError.make({ reason: "UnsupportedVersion", message: "Unknown action" }),
        );

      return call(bound[operation]._tag, {
        ...header,
        commandId: command.commandId,
        admittedGeneration: command.admittedGeneration,
        payload: command.payload,
        namespace: bound.namespace,
        action: bound.name,
      });
    };

    return {
      snapshot: call("snapshot", header),
      execute: (action, command) => actionCall(action, command, "execute"),
      result: (action, command) => actionCall(action, command, "result"),
    };
  });
