import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import { Effect } from "effect";

import CounterWorker from "./src/worker.ts";

export default Alchemy.Stack(
  "YieldedSyncCounter",
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const worker = yield* CounterWorker;

    return { url: worker.url };
  }),
);
