import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/unstable/rpc";

import type * as Action from "./Action.ts";
import { bind, registry } from "./internal/action.ts";
import {
  assertUnique,
  validate,
  type Ids,
  type Names,
  type Unique,
} from "./internal/registration.ts";
import {
  ActorId,
  CommandId,
  ConnectionId,
  ProtocolError,
  ProtocolVersion,
  SchemaVersion,
  SourcePosition,
} from "./Model.ts";
import type * as Plugin from "./Plugin.ts";

export interface Spec extends Plugin.Spec {
  readonly kind: string;
  readonly schemaVersion: number;
  readonly plugins: ReadonlyArray<Plugin.Definition>;
}

type BoundActions<
  S extends Spec,
  Namespace extends string,
  Actions extends ReadonlyArray<Action.Definition>,
> = {
  readonly [Index in keyof Actions]: Action.Bound<
    S["kind"],
    S["schemaVersion"],
    Namespace,
    Actions[Index]
  >;
};

const bindActions = <
  S extends Spec,
  const Namespace extends string,
  const Actions extends ReadonlyArray<Action.Definition>,
>(
  spec: S,
  namespace: Namespace,
  actions: Actions,
): BoundActions<S, Namespace, Actions> => {
  // map preserves the validated registration tuple's order and each action's schemas.
  return actions.map((action) =>
    bind(spec.kind, spec.schemaVersion, namespace, action),
  ) as BoundActions<S, Namespace, Actions>;
};

type BoundPlugins<S extends Spec> = {
  readonly [P in S["plugins"][number] as P["id"]]: {
    readonly definition: P;
    readonly actions: Action.Registry<BoundActions<S, P["id"], P["spec"]["actions"]>>;
  };
};

type SnapshotFields<S extends Spec> = {
  readonly [P in S["plugins"][number] as P["id"]]: P["snapshot"];
};

type SlotFrame<Namespace extends string, Payload extends Schema.Top> = Schema.Struct<{
  readonly namespace: Schema.Literal<Namespace>;
  readonly payload: Payload;
}>;

type PluginFrames<
  Plugins extends ReadonlyArray<Plugin.Definition>,
  Key extends "event" | "message",
> = {
  readonly [Index in keyof Plugins]: SlotFrame<Plugins[Index]["id"], Plugins[Index][Key]>;
};

const frames = <S extends Spec, Key extends "event" | "message">(spec: S, key: Key) => {
  const root = Schema.Struct({
    namespace: Schema.Literal("$source"),
    payload: spec[key] as S[Key],
  });

  const plugins = spec.plugins.map((plugin) =>
    Schema.Struct({ namespace: Schema.Literal(plugin.id), payload: plugin[key] }),
  );

  // Each tuple slot is constructed from the same plugin id and payload schema.
  return Schema.Union([root, ...plugins] as unknown as readonly [
    typeof root,
    ...PluginFrames<S["plugins"], Key>,
  ]);
};

type PluginRpcs<S extends Spec, Plugins extends ReadonlyArray<Plugin.Definition>> = {
  [Index in keyof Plugins]: BoundActions<
    S,
    Plugins[Index]["id"],
    Plugins[Index]["spec"]["actions"]
  >[number]["execute" | "result"];
}[number];

type ActionRpcs<S extends Spec> =
  | BoundActions<S, "$source", S["actions"]>[number]["execute" | "result"]
  | PluginRpcs<S, S["plugins"]>;

export const make = <const S extends Spec>(
  input: S & {
    readonly actions: S["actions"] & Unique<Names<S["actions"]>>;
    readonly plugins: S["plugins"] & Unique<Ids<S["plugins"]>>;
  },
) => {
  const spec: S = input;

  validate(Schema.NonEmptyString, spec.kind, "source kind");
  validate(SchemaVersion, spec.schemaVersion, "schema version");
  assertUnique(
    "plugin id",
    spec.plugins.map((plugin) => plugin.id),
  );

  const actionDefinitions = bindActions<S, "$source", S["actions"]>(spec, "$source", spec.actions);
  const actions = registry(actionDefinitions);

  const pluginEntries = spec.plugins.map((plugin) => {
    const bound = bindActions(spec, plugin.id, plugin.spec.actions);

    return [plugin.id, Object.freeze({ definition: plugin, actions: registry(bound) })] as const;
  });

  const plugins = Object.freeze(Object.fromEntries(pluginEntries)) as BoundPlugins<S>;

  const snapshotFields = Object.fromEntries(
    spec.plugins.map((plugin) => [plugin.id, plugin.snapshot]),
  ) as SnapshotFields<S>;

  const snapshot = Schema.Struct({
    source: spec.snapshot as S["snapshot"],
    plugins: Schema.Struct(snapshotFields),
  });

  const event = frames(spec, "event");
  const message = frames(spec, "message");
  const kind = spec.kind as S["kind"];

  const addressSchema = Schema.Struct({
    kind: Schema.Literal(kind),
    id: Schema.NonEmptyString,
  });

  const version = Schema.Literal(spec.schemaVersion as S["schemaVersion"]);

  const header = {
    protocolVersion: ProtocolVersion,
    address: addressSchema,
    schemaVersion: version,
  };

  const sequenced = Schema.Struct({
    position: SourcePosition,
    command: Schema.optionalKey(Schema.Struct({ actorId: ActorId, commandId: CommandId })),
    event,
  });

  const snapshotFrame = Schema.TaggedStruct("Snapshot", {
    ...header,
    position: SourcePosition,
    snapshot,
  });

  const eventFrame = Schema.TaggedStruct("Event", { ...header, event: sequenced });

  const eventsFrame = Schema.TaggedStruct("Events", {
    ...header,
    position: SourcePosition,
    events: Schema.Array(sequenced),
  });

  const resetFrame = Schema.TaggedStruct("Reset", {
    ...header,
    position: SourcePosition,
    snapshot,
  });

  const gapFrame = Schema.TaggedStruct("ResyncRequired", {
    ...header,
    position: SourcePosition,
    reason: Schema.NonEmptyString,
  });

  const messageFrame = Schema.TaggedStruct("Message", {
    ...header,
    actorId: ActorId,
    connectionId: ConnectionId,
    message,
  });

  const leaveFrame = Schema.TaggedStruct("MessageLeave", {
    ...header,
    actorId: ActorId,
    connectionId: ConnectionId,
  });

  const outbound = Schema.Union([
    snapshotFrame,
    eventFrame,
    eventsFrame,
    resetFrame,
    gapFrame,
    messageFrame,
    leaveFrame,
  ]);

  const snapshotRpc = Rpc.make("snapshot", {
    payload: Schema.toCodecJson(Schema.Struct(header)),
    success: Schema.toCodecJson(snapshotFrame),
    error: ProtocolError,
  });

  const subscribeRpc = Rpc.make("subscribe", {
    payload: Schema.toCodecJson(
      Schema.Struct({ ...header, after: Schema.optionalKey(SourcePosition) }),
    ),
    success: Schema.toCodecJson(outbound),
    error: ProtocolError,
    stream: true,
  });

  const publishMessageRpc = Rpc.make("publishMessage", {
    payload: Schema.toCodecJson(Schema.Struct({ ...header, message })),
    success: Schema.Void,
    error: ProtocolError,
  });

  // flatMap erases the tuple relationship; every RPC comes from its corresponding
  // bound action above. Retain that union for native RPC handler/client inference.
  const actionRpcs = [
    ...actionDefinitions.flatMap((action) => [action.execute, action.result]),
    ...pluginEntries.flatMap(([, plugin]) =>
      Object.values(plugin.actions).flatMap((action) => [action.execute, action.result]),
    ),
  ] as unknown as ReadonlyArray<ActionRpcs<S>>;

  const rpc = RpcGroup.make(snapshotRpc, subscribeRpc, publishMessageRpc, ...actionRpcs);

  const frozenSpec = Object.freeze({
    ...spec,
    actions: Object.freeze([...spec.actions]),
    plugins: Object.freeze([...spec.plugins]),
  });

  return Object.freeze({
    kind,
    schemaVersion: spec.schemaVersion as S["schemaVersion"],
    spec: frozenSpec as S,
    address: (id: string) => Schema.decodeSync(addressSchema)({ kind, id }),
    addressSchema,
    snapshot,
    event,
    message,
    actions,
    plugins,
    rpc,
    envelope: Object.freeze({
      sequenced,
      snapshotFrame,
      eventFrame,
      eventsFrame,
      resetFrame,
      gapFrame,
      messageFrame,
      leaveFrame,
      outbound,
    }),
  });
};

export type Definition<S extends Spec = Spec> = ReturnType<typeof make<S>>;
