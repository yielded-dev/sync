import { Action, type ProtocolError } from "@yielded/sync";
import { Context, Effect, Layer, Schema, Stream } from "effect";
import { type Rpc } from "effect/unstable/rpc";

import { Counter } from "../src/counter.ts";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Assert<T extends true> = T;

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

export type RequirementsRemainVisible = Assert<Equal<Layer.Services<typeof executeLayer>, Access>>;

const supplied = executeLayer.pipe(Layer.provide(Layer.succeed(Access, { check: Effect.void })));

export type ProvisionSubtractsRequirement = Assert<Equal<Layer.Services<typeof supplied>, never>>;

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

export type CompositionUnionsRequirements = Assert<
  Equal<Layer.Services<typeof combinedLayer>, Access | Audit>
>;

// Type-only function: invalid definitions must never execute during the example.
export const invalidRegistrations = () => {
  // @ts-expect-error Transported codecs cannot require runtime services.
  Action.make("service", { payload: serviceCodec, success: Schema.Void, error: Schema.Never });
};
