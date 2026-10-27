/**
 * Schema-typed commands shared by a source and its clients.
 *
 * An action declares its payload, success, and durable rejection. Binding it to
 * a source adds the exact-retry command, receipt, and RPC schemas.
 *
 * @since 0.1.0-beta.0
 */

import type * as Rpc from "effect/rpc/Rpc";
import * as Schema from "effect/Schema";

import { validate, type Escape } from "./internal/registration.ts";
import type {
  ActorId,
  CommandId,
  ProtocolError,
  ProtocolVersion,
  RequestFingerprint,
  SourceAuthorityGeneration,
  WireSchema,
  actionOutcome,
  resultLookup,
} from "./Model.ts";

/**
 * Schemas for an action. Each codec must encode and decode without services.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Spec {
  /**
   * Input accepted by the action, captured once before transport.
   */
  readonly payload: WireSchema;
  /**
   * Durable result returned by a successful commit.
   */
  readonly success: WireSchema;
  /**
   * Durable domain rejection. Use Schema.Never when the action cannot reject.
   */
  readonly error: WireSchema;
}

/**
 * A named action before it is bound to a source or plugin namespace.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Definition<Name extends string = string, S extends Spec = Spec> {
  readonly name: Name;
  readonly payload: S["payload"];
  readonly success: S["success"];
  readonly error: S["error"];
}

/**
 * Defines an action and retains its payload, success, and rejection types.
 *
 * An empty name throws `RegistrationError`. The error schema describes durable
 * domain rejections; transport and authorization failures use `ProtocolError`.
 *
 * @example
 * ```ts
 * import * as Action from "@yielded/sync/Action"
 * import * as Schema from "effect/Schema"
 *
 * const Rename = Action.make("rename", {
 *   payload: Schema.Struct({ name: Schema.NonEmptyString }),
 *   success: Schema.Void,
 *   error: Schema.Never
 * })
 * ```
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export const make = <const Name extends string, S extends Spec>(
  name: Name,
  spec: S,
): Definition<Name, S> => {
  validate(Schema.NonEmptyString, name, "action name");

  return Object.freeze({ name, payload: spec.payload, success: spec.success, error: spec.error });
};

/**
 * The decoded payload accepted by an action.
 *
 * @category type-level
 * @since 0.1.0-beta.0
 */
export type Payload<A extends Definition> = A["payload"]["Type"];
/**
 * The decoded result returned by a successful action.
 *
 * @category type-level
 * @since 0.1.0-beta.0
 */
export type Success<A extends Definition> = A["success"]["Type"];
/**
 * The decoded durable rejection declared by an action.
 *
 * @category type-level
 * @since 0.1.0-beta.0
 */
export type Error<A extends Definition> = A["error"]["Type"];

/**
 * An action bound to a source kind, schema version, and namespace.
 *
 * Its command schema preserves the retry identity; its RPCs use JSON codecs
 * while retaining the action-specific payload and result types.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export type Bound<
  Kind extends string,
  Version extends number,
  Namespace extends string,
  A extends Definition,
> = Readonly<A & BoundFields<Kind, Version, Namespace, A>>;

interface BoundFields<
  Kind extends string,
  Version extends number,
  Namespace extends string,
  A extends Definition,
> {
  readonly namespace: Namespace;
  readonly command: Schema.Struct<{
    readonly protocolVersion: typeof ProtocolVersion;
    readonly commandId: typeof CommandId;
    readonly address: Schema.Struct<{
      readonly kind: Schema.Literal<Kind>;
      readonly id: Schema.NonEmptyString;
    }>;
    readonly admittedGeneration: Schema.NullOr<typeof SourceAuthorityGeneration>;
    readonly namespace: Schema.Literal<Namespace>;
    readonly action: Schema.Literal<A["name"]>;
    readonly schemaVersion: Schema.Literal<Version>;
    readonly payload: A["payload"];
  }>;
  readonly outcome: ReturnType<typeof actionOutcome<A["success"], A["error"]>>;
  readonly lookup: ReturnType<
    typeof resultLookup<BoundFields<Kind, Version, Namespace, A>["outcome"]>
  >;
  readonly receipt: Schema.Struct<{
    readonly command: BoundFields<Kind, Version, Namespace, A>["command"];
    readonly actorId: typeof ActorId;
    readonly sourceAuthorityGeneration: typeof SourceAuthorityGeneration;
    readonly fingerprint: typeof RequestFingerprint;
    readonly outcome: BoundFields<Kind, Version, Namespace, A>["outcome"];
  }>;
  readonly execute: Rpc.Rpc<
    `execute/${Escape<Namespace>}/${Escape<A["name"]>}`,
    Schema.toCodecJson<BoundFields<Kind, Version, Namespace, A>["command"]>,
    Schema.toCodecJson<BoundFields<Kind, Version, Namespace, A>["outcome"]>,
    typeof ProtocolError
  >;
  readonly result: Rpc.Rpc<
    `result/${Escape<Namespace>}/${Escape<A["name"]>}`,
    Schema.toCodecJson<BoundFields<Kind, Version, Namespace, A>["command"]>,
    Schema.toCodecJson<BoundFields<Kind, Version, Namespace, A>["lookup"]>,
    typeof ProtocolError
  >;
}

/**
 * Actions indexed by their literal names, retaining each action's schemas.
 *
 * @category type-level
 * @since 0.1.0-beta.0
 */
export type Registry<Actions extends ReadonlyArray<Definition>> = {
  readonly [A in Actions[number] as A["name"]]: A;
};
