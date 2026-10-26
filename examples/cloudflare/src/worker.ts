import { ProtocolError } from "@yielded/sync";
import { EffectCf } from "@yielded/sync-platform-effect-cf";
import { Effect, Schema } from "effect";
import { Worker } from "effect-cf";

import { authenticate } from "./auth.ts";
import { Counter } from "./contract.ts";
import { CounterServer } from "./server.ts";

export const CounterObject = EffectCf.make(CounterServer, {
  storage: { namespace: "counter-v1", maxPendingOutbox: 100 },
  replay: { maxEvents: 4, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 32, bufferSize: 16 },
  outbox: {
    batchSize: 10,
    retryMillis: 60_000,
    deliver: () => Effect.succeed({ _tag: "Delivered" as const }),
  },
});

export type CounterObject = InstanceType<typeof CounterObject>;

const Counters = CounterObject.binding("COUNTERS");

const fetch = Effect.gen(function* () {
  const request = yield* Worker.NativeRequest;
  const match = /^\/sync\/counters\/([^/]+)\/?$/.exec(new URL(request.url).pathname);

  if (match === null) return new Response("Not found", { status: 404 });

  const id = yield* Schema.decodeEffect(Schema.StringFromUriComponent)(match[1]).pipe(
    Effect.mapError(() =>
      ProtocolError.make({ reason: "UnsupportedVersion", message: "Invalid source id" }),
    ),
  );

  const identity = yield* authenticate(request.headers.get("authorization"));
  const stub = yield* Counters.getByName(`counter:${id}`);

  const prepared = yield* EffectCf.prepareRequest(Counter, {
    request,
    address: Counter.address(id),
    identity,
  });

  return yield* Counters.fetch(stub, prepared);
}).pipe(
  Effect.catchTag("DurableObjectFetchError", () =>
    Effect.fail(ProtocolError.make({ reason: "Unavailable", message: "Source unavailable" })),
  ),
  Effect.catchTag("ProtocolError", (error) => Effect.succeed(EffectCf.errorResponse(error))),
);

export default Worker.makeFetchHandler(Counters.layer, { fetch });
