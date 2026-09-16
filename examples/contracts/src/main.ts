import assert from "node:assert/strict";

import { DateTime, Effect, Schema } from "effect";

import { Catalog, Counter } from "./counter.ts";

const roundTrip = Effect.gen(function* () {
  const codec = Schema.toCodecJson(Counter.snapshot);

  const snapshot: typeof Counter.snapshot.Type = {
    source: { value: 7, updatedAt: DateTime.makeUnsafe("2026-09-16T12:00:00Z") },
    plugins: { label: { text: "Demo" } },
  };

  const encoded = yield* Schema.encodeEffect(codec)(snapshot);
  const decoded = yield* Schema.decodeEffect(codec)(encoded);

  assert.deepEqual(encoded, {
    source: { value: 7, updatedAt: "2026-09-16T12:00:00.000Z" },
    plugins: { label: { text: "Demo" } },
  });
  assert.deepEqual(decoded, snapshot);
  assert.equal(Catalog.get("counter"), Counter);
  assert.equal(Counter.rpc.requests.size, 7);
});

await Effect.runPromise(roundTrip);
