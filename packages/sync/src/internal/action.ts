import { Schema } from "effect";
import { Rpc } from "effect/unstable/rpc";

import type * as Action from "../Action.ts";
import {
  ActorId,
  CommandId,
  ProtocolError,
  ProtocolVersion,
  RequestFingerprint,
  SourceAuthorityGeneration,
  actionOutcome,
  resultLookup,
} from "../Model.ts";
import { assertUnique } from "./registration.ts";

type Escape<S extends string> = string extends S
  ? string
  : S extends `${infer Head}%${infer Tail}`
    ? `${Escape<Head>}%25${Escape<Tail>}`
    : S extends `${infer Head}/${infer Tail}`
      ? `${Escape<Head>}%2F${Escape<Tail>}`
      : S;

const escape = <const S extends string>(value: S): Escape<S> =>
  value.replaceAll("%", "%25").replaceAll("/", "%2F") as Escape<S>;

export const bind = <
  const Kind extends string,
  const Version extends number,
  const Namespace extends string,
  A extends Action.Definition,
>(
  kind: Kind,
  schemaVersion: Version,
  namespace: Namespace,
  action: A,
) => {
  const command = Schema.Struct({
    protocolVersion: ProtocolVersion,
    commandId: CommandId,
    address: Schema.Struct({ kind: Schema.Literal(kind), id: Schema.NonEmptyString }),
    admittedGeneration: Schema.NullOr(SourceAuthorityGeneration),
    namespace: Schema.Literal(namespace),
    action: Schema.Literal(action.name as A["name"]),
    schemaVersion: Schema.Literal(schemaVersion),
    payload: action.payload as A["payload"],
  });

  const outcome = actionOutcome(action.success as A["success"], action.error as A["error"]);
  const lookup = resultLookup(outcome);

  const receipt = Schema.Struct({
    command,
    actorId: ActorId,
    sourceAuthorityGeneration: SourceAuthorityGeneration,
    fingerprint: RequestFingerprint,
    outcome,
  });

  const execute = Rpc.make(`execute/${escape(namespace)}/${escape(action.name as A["name"])}`, {
    payload: Schema.toCodecJson(command),
    success: Schema.toCodecJson(outcome),
    error: ProtocolError,
  });

  const result = Rpc.make(`result/${escape(namespace)}/${escape(action.name as A["name"])}`, {
    payload: Schema.toCodecJson(command),
    success: Schema.toCodecJson(lookup),
    error: ProtocolError,
  });

  return Object.freeze({
    ...action,
    namespace,
    command,
    outcome,
    lookup,
    receipt,
    execute,
    result,
  });
};

export const registry = <const Actions extends ReadonlyArray<Action.Definition>>(
  actions: Actions,
): Action.Registry<Actions> => {
  assertUnique(
    "action name",
    actions.map((action) => action.name),
  );

  // The unique names above are the keys of the mapped registry, including names
  // such as __proto__ (fromEntries creates own data properties).
  return Object.freeze(
    Object.fromEntries(actions.map((action) => [action.name, action])),
  ) as Action.Registry<Actions>;
};
