import { ProtocolError } from "@yielded/sync";
import { EffectCf } from "@yielded/sync-platform-effect-cf";
import { Context, Effect } from "effect";
import { DurableObject, DurableObjectNamespace, Worker } from "effect-cf";

import { authenticate } from "./auth.ts";
import { Counter } from "./contract.ts";
import { CounterServer } from "./server.ts";

const sync = EffectCf.make(CounterServer, {
  storageNamespace: "counter-v1",
  replay: { maxEvents: 4, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 32, bufferSize: 16 },
  outbox: {
    maxPending: 100,
    batchSize: 10,
    retryMillis: 60_000,
    deliver: () => Effect.succeed({ _tag: "Delivered" as const }),
  },
});

export const CounterObject = DurableObject.make(sync.layer, sync.handlers);
export type CounterObject = InstanceType<typeof CounterObject>;

class Counters extends Context.Service<
  Counters,
  DurableObjectNamespace.DurableObjectNamespaceEffectClient<CounterObject>
>()("Counters") {
  static readonly layer = DurableObjectNamespace.layer(this, { binding: "COUNTERS" });
}

const fetch = Effect.gen(function* () {
  const request = yield* Worker.NativeRequest;
  const match = /^\/sync\/counters\/([^/]+)\/?$/.exec(new URL(request.url).pathname);

  if (match === null) return new Response("Not found", { status: 404 });

  const id = yield* Effect.try({
    try: () => decodeURIComponent(match[1]),
    catch: () => ProtocolError.make({ reason: "UnsupportedVersion", message: "Invalid source id" }),
  });

  const identity = yield* authenticate(request.headers.get("authorization"));
  const counters = yield* Counters;
  const stub = yield* counters.getByName(`counter:${id}`);

  const prepared = yield* EffectCf.prepareRequest(Counter, {
    request,
    address: Counter.address(id),
    identity,
  });

  return yield* counters.fetch(stub, prepared);
}).pipe(
  Effect.catchTag("DurableObjectFetchError", () =>
    Effect.fail(ProtocolError.make({ reason: "Unavailable", message: "Source unavailable" })),
  ),
  Effect.catchTag("ProtocolError", (error) => Effect.succeed(EffectCf.errorResponse(error))),
);

export default Worker.makeFetchHandler(Counters.layer, { fetch });
