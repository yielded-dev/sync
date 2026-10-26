import { EffectCf } from "@yielded/sync-platform-effect-cf";
import { Effect } from "effect";
import { Worker } from "effect-cf";

import { authenticate } from "./auth.ts";
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

const fetch = Counters.handler({ authenticate });

export default Worker.makeFetchHandler(Counters.layer, { fetch });
