import {
  type Action,
  type ActionOutcome,
  ProtocolError,
  type ResultLookup,
  type Source,
} from "@yielded/sync";
import type { Client } from "@yielded/sync/client";
import { Effect, Layer, type Scope } from "effect";
import { HttpClient, HttpClientError, HttpClientRequest, HttpClientResponse } from "effect/http";
import { RpcClient, type RpcClientError, RpcSerialization } from "effect/rpc";

import type { Identity, prepareRequest } from "./Cloudflare.ts";

export interface ClientOptions<Kind extends string, E = never, R = never> {
  readonly address: { readonly kind: Kind; readonly id: string };
  readonly identity: Identity;
  readonly fetch: (request: Request) => Effect.Effect<Response, E, R>;
}

/** Retain this exact request for retries and result lookup, including a null generation. */
export type Command<A extends Action.Definition> = Pick<
  Client.EncodedCommand,
  "commandId" | "admittedGeneration"
> & { readonly payload: Action.Payload<A> };

type RequestError = ProtocolError | RpcClientError.RpcClientError;

export interface RemoteClient<S extends Source.Spec> {
  readonly snapshot: Effect.Effect<
    Source.Definition<S>["envelope"]["snapshotFrame"]["Type"],
    RequestError
  >;
  readonly execute: <A extends Client.Actions<S>>(
    action: A,
    command: Command<NoInfer<A>>,
  ) => Effect.Effect<ActionOutcome<Action.Success<A>, Action.Error<A>>, RequestError>;
  readonly result: <A extends Client.Actions<S>>(
    action: A,
    command: Command<NoInfer<A>>,
  ) => Effect.Effect<ResultLookup<Action.Success<A>, Action.Error<A>>, RequestError>;
}

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
