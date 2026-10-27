/**
 * Shared source contracts and their derived wire schemas and RPCs.
 *
 * A source combines its own snapshot and actions with named plugin slots.
 * The same definition is used by server handlers, clients, and transports.
 *
 * @since 0.1.0-beta.0
 */

import * as Effect from "effect/Effect";
import * as Url from "effect/http/Url";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Rpc from "effect/rpc/Rpc";
import * as RpcGroup from "effect/rpc/RpcGroup";
import type * as RpcSchema from "effect/rpc/RpcSchema";
import * as Schema from "effect/Schema";

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

/**
 * A source kind, positive schema version, and its public capabilities.
 *
 * The version describes the application schema, independently of the wire
 * protocol version and authority generation.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Spec extends Plugin.Spec {
  readonly kind: string;
  /** Identity validation, including brands. Defaults to Schema.NonEmptyString in make. */
  readonly id: Schema.Schema<string>;
  /** Canonical absolute base path. Defaults to /sync/<encoded kind>. */
  readonly path?: string;
  readonly schemaVersion: number;
  readonly plugins: ReadonlyArray<Plugin.Definition>;
}

interface Input extends Omit<Spec, "id" | "message" | "plugins"> {
  readonly id?: Spec["id"];
  readonly message?: Spec["message"];
  readonly plugins?: Spec["plugins"];
}

type Normalized<S extends Input> = S extends Spec
  ? S
  : Omit<S, "id" | "message" | "plugins"> & {
      readonly id: "id" extends keyof S
        ?
            | Exclude<S["id"], undefined>
            | (undefined extends S["id"] ? typeof Schema.NonEmptyString : never)
        : typeof Schema.NonEmptyString;
      readonly message: "message" extends keyof S
        ?
            | Exclude<S["message"], undefined>
            | (undefined extends S["message"] ? typeof Schema.Never : never)
        : typeof Schema.Never;
      readonly plugins: "plugins" extends keyof S
        ? Exclude<S["plugins"], undefined> | (undefined extends S["plugins"] ? readonly [] : never)
        : readonly [];
    };

/**
 * The decoded identity type of a source, including application brands.
 *
 * @category type-level
 * @since 0.1.0-beta.0
 */
export type Id<S extends Spec> = S["id"]["Type"];

/**
 * A source kind and its schema-validated identity.
 *
 * @category type-level
 * @since 0.1.0-beta.0
 */
export type Address<S extends Spec = Spec> = {
  readonly kind: S["kind"];
  readonly id: Id<S>;
};

const Path = Schema.String.check(
  Schema.makeFilter(
    (path) => {
      const parsed = Url.fromString(path, "https://sync.invalid");

      return (
        Result.isSuccess(parsed) &&
        parsed.success.origin === "https://sync.invalid" &&
        parsed.success.pathname === path &&
        parsed.success.search === "" &&
        parsed.success.hash === "" &&
        !path.endsWith("/")
      );
    },
    { expected: "a canonical absolute base path without a query, fragment, or trailing slash" },
  ),
);

const invalidId = () =>
  ProtocolError.make({ reason: "UnsupportedVersion", message: "Invalid source id" });

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

const define = <const S extends Spec>(spec: S): Definition<S> => {
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

  // Identity values stay strings on the wire; brands and checks validate their type side.
  const id: Schema.Codec<Id<S>, string> = Schema.NonEmptyString.pipe(
    Schema.decodeTo(Schema.toType<S["id"]>(spec.id)),
  );

  const path = spec.path ?? `/sync/${Schema.encodeSync(Schema.StringFromUriComponent)(kind)}`;

  validate(Path, path, "source path");

  const addressSchema = Schema.Struct({
    kind: Schema.Literal(kind),
    id,
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
    id,
    path,
    schemaVersion: spec.schemaVersion as S["schemaVersion"],
    spec: frozenSpec as S,
    address: (id: Id<S>): Address<S> => Schema.decodeSync(addressSchema)({ kind, id }),
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

/**
 * Defines a source and derives its addresses, envelopes, and RPC group.
 *
 * Snapshots have `{ source, plugins }` slots. Root actions use `$source`;
 * plugin actions use the plugin id. Invalid names, versions, or duplicate
 * registrations throw `RegistrationError` before resources are acquired.
 *
 * @example
 * ```ts
 * import * as Action from "@yielded/sync/Action"
 * import * as Source from "@yielded/sync/Source"
 * import * as Schema from "effect/Schema"
 *
 * const Counter = Source.make({
 *   kind: "counter",
 *   schemaVersion: 1,
 *   snapshot: Schema.Number,
 *   event: Schema.Number,
 *   message: Schema.Never,
 *   actions: [Action.make("set", {
 *     payload: Schema.Number,
 *     success: Schema.Void,
 *     error: Schema.Never
 *   })],
 *   plugins: []
 * })
 *
 * const address = Counter.address("shared")
 * ```
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export function make<const S extends Input>(
  input: S & {
    readonly actions: S["actions"] & Unique<Names<S["actions"]>>;
    readonly plugins?: S["plugins"] & Unique<Ids<NonNullable<S["plugins"]>>>;
  } & ([S] extends [Spec] ? never : unknown),
): Definition<Normalized<S>>;

/**
 * Defines a source and derives its addresses, envelopes, and RPC group.
 *
 * Snapshots have `{ source, plugins }` slots. Root actions use `$source`;
 * plugin actions use the plugin id. Invalid names, versions, or duplicate
 * registrations throw `RegistrationError` before resources are acquired.
 *
 * @example
 * ```ts
 * import * as Action from "@yielded/sync/Action"
 * import * as Source from "@yielded/sync/Source"
 * import * as Schema from "effect/Schema"
 *
 * const Counter = Source.make({
 *   kind: "counter",
 *   schemaVersion: 1,
 *   snapshot: Schema.Number,
 *   event: Schema.Number,
 *   message: Schema.Never,
 *   actions: [Action.make("set", {
 *     payload: Schema.Number,
 *     success: Schema.Void,
 *     error: Schema.Never
 *   })],
 *   plugins: []
 * })
 *
 * const address = Counter.address("shared")
 * ```
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export function make<const S extends Spec>(
  input: S & {
    readonly actions: S["actions"] & Unique<Names<S["actions"]>>;
    readonly plugins: S["plugins"] & Unique<Ids<S["plugins"]>>;
  },
): Definition<S>;

/**
 * Defines a source and derives its addresses, envelopes, and RPC group.
 *
 * Snapshots have `{ source, plugins }` slots. Root actions use `$source`;
 * plugin actions use the plugin id. Invalid names, versions, or duplicate
 * registrations throw `RegistrationError` before resources are acquired.
 *
 * @example
 * ```ts
 * import * as Action from "@yielded/sync/Action"
 * import * as Source from "@yielded/sync/Source"
 * import * as Schema from "effect/Schema"
 *
 * const Counter = Source.make({
 *   kind: "counter",
 *   schemaVersion: 1,
 *   snapshot: Schema.Number,
 *   event: Schema.Number,
 *   message: Schema.Never,
 *   actions: [Action.make("set", {
 *     payload: Schema.Number,
 *     success: Schema.Void,
 *     error: Schema.Never
 *   })],
 *   plugins: []
 * })
 *
 * const address = Counter.address("shared")
 * ```
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export function make(input: Input): unknown {
  return define({
    ...input,
    id: input.id ?? Schema.NonEmptyString,
    message: input.message ?? Schema.Never,
    plugins: input.plugins ?? [],
  });
}

/**
 * Encode one source ID using its shared path and identity schema.
 *
 * @category http
 * @since 0.1.0-beta.0
 */
export const pathFor = <S extends Spec>(
  source: Definition<S>,
  id: NoInfer<Id<S>>,
): Effect.Effect<string, ProtocolError> =>
  Schema.encodeEffect(source.id)(id).pipe(
    // URI encoding requires valid Unicode; URL parsers also normalize dot-only segments.
    Effect.filterOrFail(
      (id) => !/[\uD800-\uDFFF]/u.test(id) && id !== "." && id !== "..",
      invalidId,
    ),
    Effect.flatMap(Schema.encodeEffect(Schema.StringFromUriComponent)),
    Effect.map((segment) => `${source.path}/${segment}`),
    Effect.mapError(invalidId),
  );

/**
 * Unmatched paths are absent; malformed IDs on a matched route are typed failures.
 *
 * @category http
 * @since 0.1.0-beta.0
 */
export const matchPath = <S extends Spec>(
  source: Definition<S>,
  pathname: string,
): Effect.Effect<Option.Option<Address<S>>, ProtocolError> => {
  const prefix = `${source.path}/`;

  if (!pathname.startsWith(prefix)) return Effect.succeedNone;
  const suffix = pathname.slice(prefix.length);
  const segment = suffix.endsWith("/") ? suffix.slice(0, -1) : suffix;

  if (segment.length === 0 || segment.includes("/")) return Effect.succeedNone;

  return Schema.decodeEffect(Schema.StringFromUriComponent)(segment).pipe(
    Effect.flatMap(Schema.decodeEffect(source.id)),
    Effect.map((id) => Option.some({ kind: source.kind, id })),
    Effect.mapError(invalidId),
  );
};

type Header<S extends Spec> = {
  readonly protocolVersion: typeof ProtocolVersion;
  readonly address: Definition<S>["addressSchema"];
  readonly schemaVersion: Schema.Literal<S["schemaVersion"]>;
};

type Frames<S extends Spec, Key extends "event" | "message"> = Schema.Union<
  readonly [SlotFrame<"$source", S[Key]>, ...PluginFrames<S["plugins"], Key>]
>;

interface Envelope<S extends Spec> {
  readonly sequenced: Schema.Struct<{
    readonly position: typeof SourcePosition;
    readonly command: Schema.optionalKey<
      Schema.Struct<{
        readonly actorId: typeof ActorId;
        readonly commandId: typeof CommandId;
      }>
    >;
    readonly event: Frames<S, "event">;
  }>;
  readonly snapshotFrame: Schema.TaggedStruct<
    "Snapshot",
    Header<S> & {
      readonly position: typeof SourcePosition;
      readonly snapshot: Definition<S>["snapshot"];
    }
  >;
  readonly eventFrame: Schema.TaggedStruct<
    "Event",
    Header<S> & {
      readonly event: Envelope<S>["sequenced"];
    }
  >;
  readonly eventsFrame: Schema.TaggedStruct<
    "Events",
    Header<S> & {
      readonly position: typeof SourcePosition;
      readonly events: Schema.$Array<Envelope<S>["sequenced"]>;
    }
  >;
  readonly resetFrame: Schema.TaggedStruct<
    "Reset",
    Header<S> & {
      readonly position: typeof SourcePosition;
      readonly snapshot: Definition<S>["snapshot"];
    }
  >;
  readonly gapFrame: Schema.TaggedStruct<
    "ResyncRequired",
    Header<S> & {
      readonly position: typeof SourcePosition;
      readonly reason: Schema.NonEmptyString;
    }
  >;
  readonly messageFrame: Schema.TaggedStruct<
    "Message",
    Header<S> & {
      readonly actorId: typeof ActorId;
      readonly connectionId: typeof ConnectionId;
      readonly message: Frames<S, "message">;
    }
  >;
  readonly leaveFrame: Schema.TaggedStruct<
    "MessageLeave",
    Header<S> & {
      readonly actorId: typeof ActorId;
      readonly connectionId: typeof ConnectionId;
    }
  >;
  readonly outbound: Schema.Union<
    readonly [
      Envelope<S>["snapshotFrame"],
      Envelope<S>["eventFrame"],
      Envelope<S>["eventsFrame"],
      Envelope<S>["resetFrame"],
      Envelope<S>["gapFrame"],
      Envelope<S>["messageFrame"],
      Envelope<S>["leaveFrame"],
    ]
  >;
}

/**
 * A validated source contract, preserving literal kinds, plugin ids,
 * action names, and every codec's decoded type.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Definition<S extends Spec = Spec> {
  readonly kind: S["kind"];
  readonly id: Schema.Codec<Id<S>, string>;
  readonly path: string;
  readonly schemaVersion: S["schemaVersion"];
  readonly spec: S;
  /**
   * Constructs an address, validating the source identity schema. Invalid ids throw SchemaError.
   */
  readonly address: (id: Id<S>) => Address<S>;
  readonly addressSchema: Schema.Struct<{
    readonly kind: Schema.Literal<S["kind"]>;
    readonly id: Schema.Codec<Id<S>, string>;
  }>;
  /**
   * The public composite snapshot: { source, plugins }.
   */
  readonly snapshot: Schema.Struct<{
    readonly source: S["snapshot"];
    /**
     * Plugins and their bound action registries, indexed by plugin id.
     */
    readonly plugins: Schema.Struct<SnapshotFields<S>>;
  }>;
  /**
   * Namespaced durable events that advance the source cursor.
   */
  readonly event: Frames<S, "event">;
  /**
   * Namespaced ephemeral messages; these never advance durable progress.
   */
  readonly message: Frames<S, "message">;
  /**
   * Root actions bound to the $source namespace.
   */
  readonly actions: Action.Registry<BoundActions<S, "$source", S["actions"]>>;
  readonly plugins: BoundPlugins<S>;
  /**
   * Wire frames used by transports and recovery.
   */
  readonly envelope: Envelope<S>;
  /**
   * The complete typed RPC group, including root and plugin action procedures.
   */
  readonly rpc: RpcGroup.RpcGroup<
    | ActionRpcs<S>
    | Rpc.Rpc<
        "snapshot",
        Schema.toCodecJson<Schema.Struct<Header<S>>>,
        Schema.toCodecJson<Envelope<S>["snapshotFrame"]>,
        typeof ProtocolError
      >
    | Rpc.Rpc<
        "subscribe",
        Schema.toCodecJson<
          Schema.Struct<
            Header<S> & {
              readonly after: Schema.optionalKey<typeof SourcePosition>;
            }
          >
        >,
        RpcSchema.Stream<Schema.toCodecJson<Envelope<S>["outbound"]>, typeof ProtocolError>,
        Schema.Never
      >
    | Rpc.Rpc<
        "publishMessage",
        Schema.toCodecJson<
          Schema.Struct<
            Header<S> & {
              readonly message: Frames<S, "message">;
            }
          >
        >,
        Schema.Void,
        typeof ProtocolError
      >
  >;
}
