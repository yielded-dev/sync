import { ProtocolError } from "@yielded/sync";
import { AlchemyCf } from "@yielded/sync-platform-alchemy-cf";
import * as Cloudflare from "alchemy/Cloudflare";
import { Effect } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import { authenticate } from "../../cloudflare/src/auth.ts";
import { Counter } from "../../cloudflare/src/contract.ts";
import { CounterServer } from "../../cloudflare/src/server.ts";

export class CounterObject extends Cloudflare.DurableObject<CounterObject>()(
  "CounterObject",
  AlchemyCf.make(CounterServer, {
    storageNamespace: "counter-v1",
    replay: { maxEvents: 4, maxBatchBytes: 600_000 },
    sockets: { maxConnections: 32, bufferSize: 16 },
    outbox: {
      maxPending: 100,
      batchSize: 10,
      retryMillis: 60_000,
      deliver: () => Effect.succeed({ _tag: "Delivered" as const }),
    },
  }),
) {}

export default class CounterWorker extends Cloudflare.Worker<CounterWorker>()(
  "CounterWorker",
  { main: import.meta.url },
  Effect.gen(function* () {
    const counters = yield* CounterObject;

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;

        const match = /^\/sync\/counters\/([^/]+)\/?$/.exec(
          new URL(request.url, "https://sync.local").pathname,
        );

        if (match === null) return HttpServerResponse.text("Not found", { status: 404 });

        const id = yield* Effect.try({
          try: () => decodeURIComponent(match[1]),
          catch: () =>
            ProtocolError.make({ reason: "UnsupportedVersion", message: "Invalid source id" }),
        });

        const identity = yield* authenticate(request.headers.authorization);
        const stub = counters.getByName(`counter:${id}`);

        const prepared = yield* AlchemyCf.prepareRequest(Counter, {
          request,
          address: Counter.address(id),
          identity,
        });

        return yield* stub.fetch(prepared);
      }).pipe(
        Effect.catchTag("ProtocolError", (error) => Effect.succeed(AlchemyCf.errorResponse(error))),
      ),
    };
  }),
) {}
