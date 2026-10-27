import { expectTypeOf } from "@effect/vitest";
import { Action, type ProtocolError } from "@yielded/sync";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Rpc from "effect/rpc/Rpc";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { Counter } from "../test/fixtures/counter.ts";

class Access extends Context.Service<
  Access,
  { readonly check: Effect.Effect<void, ProtocolError> }
>()("example/Access") {}

class Audit extends Context.Service<Audit, { readonly record: Effect.Effect<void> }>()(
  "example/Audit",
) {}

declare const serviceCodec: Schema.Top & Schema.Codec<string, string, Access>;

const position = { sourceAuthorityGeneration: "6d5f7444-dba4-4a4c-a343-ce7e7dac6e88", cursor: 1 };

const execute = Effect.fn("example.execute")(function* (
  input: Rpc.Payload<typeof Counter.actions.set.execute>,
) {
  const access = yield* Access;

  yield* access.check;

  return {
    _tag: "Succeeded" as const,
    result: { previous: 0, current: input.payload.value },
    position,
  };
});

const executeLayer = Counter.rpc.toLayerHandler("execute/$source/set", execute);

expectTypeOf<Layer.Services<typeof executeLayer>>().toEqualTypeOf<Access>();

const supplied = executeLayer.pipe(Layer.provide(Layer.succeed(Access, { check: Effect.void })));

expectTypeOf<Layer.Services<typeof supplied>>().toEqualTypeOf<never>();

const handlers = Counter.rpc.of({
  snapshot: () => Effect.die("contract example only"),
  subscribe: () => Stream.empty,
  publishMessage: () => Effect.void,
  "execute/$source/set": execute,
  "result/$source/set": () => Effect.succeed({ _tag: "Unknown" as const }),
  "execute/label/rename": Effect.fn("example.rename")(function* (
    input: Rpc.Payload<typeof Counter.plugins.label.actions.rename.execute>,
  ) {
    const audit = yield* Audit;

    yield* audit.record;

    return { _tag: "Succeeded" as const, result: { text: input.payload.text }, position };
  }),
  "result/label/rename": () => Effect.succeed({ _tag: "Expired" as const }),
});

const combinedLayer = Counter.rpc.toLayer(handlers);

expectTypeOf<Layer.Services<typeof combinedLayer>>().toEqualTypeOf<Access | Audit>();

// This file is checked by the compiler and is never executed.
export const invalidRegistrations = () => {
  // @ts-expect-error Transported codecs cannot require runtime services.
  Action.make("service", { payload: serviceCodec, success: Schema.Void, error: Schema.Never });
};
