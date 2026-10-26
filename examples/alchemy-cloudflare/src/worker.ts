import { AlchemyCf } from "@yielded/sync-platform-alchemy-cf";
import * as Cloudflare from "alchemy/Cloudflare";
import { Effect } from "effect";

import { authenticate } from "../../cloudflare/src/auth.ts";
import { CounterServer } from "../../cloudflare/src/server.ts";

export const CounterObject = AlchemyCf.make("CounterObject", CounterServer, {
  storage: { namespace: "counter-v1", maxPendingOutbox: 100 },
  replay: { maxEvents: 4, maxBatchBytes: 600_000 },
  sockets: { maxConnections: 32, bufferSize: 16 },
  outbox: {
    deliver: () => Effect.succeed({ _tag: "Delivered" as const }),
  },
});

export default class CounterWorker extends Cloudflare.Worker<CounterWorker>()(
  "CounterWorker",
  { main: import.meta.url },
  Effect.map(CounterObject.handler({ authenticate }), (fetch) => ({ fetch })),
) {}
