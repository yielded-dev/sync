import { Action, Plugin, Source, SourceCatalog, type ProtocolError } from "@yielded/sync";
import { Context, Effect, Layer, Schema, Stream } from "effect";
import { type Rpc, type RpcGroup } from "effect/unstable/rpc";

import { Counter, type InvalidValue, Label } from "../src/counter.ts";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Assert<T extends true> = T;

export type PayloadIsExact = Assert<
  Equal<Action.Payload<typeof Counter.actions.set>, { readonly value: number }>
>;

export type ResultIsExact = Assert<
  Equal<
    Action.Success<typeof Counter.actions.set>,
    { readonly previous: number; readonly current: number }
  >
>;

export type ErrorIsExact = Assert<Equal<Action.Error<typeof Counter.actions.set>, InvalidValue>>;

export type PluginResultIsExact = Assert<
  Equal<Action.Success<typeof Counter.plugins.label.actions.rename>, { readonly text: string }>
>;

export type PluginErrorIsNever = Assert<
  Equal<Action.Error<typeof Counter.plugins.label.actions.rename>, never>
>;

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

export type OperationalErrorRemainsTyped = Assert<
  Equal<Effect.Error<ReturnType<typeof execute>>, ProtocolError>
>;

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

export type RootEventIsExact = Assert<
  Equal<
    Extract<typeof Counter.event.Type, { readonly namespace: "$source" }>["payload"],
    typeof Counter.spec.event.Type
  >
>;

export type PluginEventIsExact = Assert<
  Equal<
    Extract<typeof Counter.event.Type, { readonly namespace: "label" }>["payload"],
    { readonly text: string }
  >
>;

export type MessagesHaveNoPosition = Assert<
  Equal<Extract<keyof typeof Counter.envelope.messageFrame.Type, "position">, never>
>;

export type CommandPayloadIsExact = Assert<
  Equal<Rpc.Payload<typeof Counter.actions.set.execute>["payload"], { readonly value: number }>
>;

declare const dynamicEntries: Array<SourceCatalog.Entry>;
const dynamicCatalog = () => SourceCatalog.make(dynamicEntries).get("counter");

export type DynamicCatalogMayMiss = Assert<
  Equal<ReturnType<typeof dynamicCatalog>, SourceCatalog.Entry | undefined>
>;

export type HandlerPayloadIsExact = Assert<
  Equal<
    Parameters<(typeof handlers)["execute/label/rename"]>[0]["payload"],
    { readonly text: string }
  >
>;

export type RpcTagsAreExact = Assert<
  Equal<
    RpcGroup.Rpcs<typeof Counter.rpc>["_tag"],
    | "snapshot"
    | "subscribe"
    | "publishMessage"
    | "execute/$source/set"
    | "result/$source/set"
    | "execute/label/rename"
    | "result/label/rename"
  >
>;

// Type-only function: invalid definitions must never execute during the example.
export const invalidRegistrations = () => {
  // @ts-expect-error Transported codecs cannot require runtime services.
  Action.make("service", { payload: serviceCodec, success: Schema.Void, error: Schema.Never });
  // @ts-expect-error Duplicate plugin ids cannot replace an existing slot.
  Source.make({ ...Counter.spec, plugins: [Label, Label] });
  Plugin.make("duplicate", {
    ...Label.spec,
    // @ts-expect-error Duplicate action names cannot silently replace schemas.
    actions: [Label.actions.rename, Label.actions.rename],
  });
  // @ts-expect-error Root actions also reject duplicate names.
  Source.make({ ...Counter.spec, actions: [Counter.actions.set, Counter.actions.set] });
  // @ts-expect-error Source kinds in a catalog are unique.
  SourceCatalog.make([Counter, Counter]);
  // @ts-expect-error The source namespace is reserved.
  Plugin.make("$source", Label.spec);
  // @ts-expect-error Every declared RPC requires a handler.
  Counter.rpc.of({ snapshot: handlers.snapshot });
  // @ts-expect-error An action result cannot be replaced by a sibling result.
  Counter.rpc.toLayerHandler("execute/$source/set", () =>
    Effect.succeed({ _tag: "Succeeded" as const, result: { text: "wrong" }, position }),
  );
  // @ts-expect-error Plugin payloads remain action-specific.
  const payload: Action.Payload<typeof Counter.plugins.label.actions.rename> = { value: 1 };

  return payload;
};
