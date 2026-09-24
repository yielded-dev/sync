import { DateTime, Effect, Schema } from "effect";

import { Counter } from "./counter.ts";

const roundTrip = Effect.gen(function* () {
  const codec = Schema.toCodecJson(Counter.snapshot);

  const snapshot: typeof Counter.snapshot.Type = {
    source: { value: 7, updatedAt: DateTime.makeUnsafe("2026-09-16T12:00:00Z") },
    plugins: { label: { text: "Demo" } },
  };

  const encoded = yield* Schema.encodeEffect(codec)(snapshot);
  const decoded = yield* Schema.decodeEffect(codec)(encoded);

  yield* Effect.log({ encoded, decoded });
});

await Effect.runPromise(roundTrip);
