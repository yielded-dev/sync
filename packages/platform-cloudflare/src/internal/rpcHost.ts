import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import type { RpcServer } from "effect/rpc";

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
