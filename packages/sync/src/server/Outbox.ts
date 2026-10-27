/**
 * Schema-typed obligations for the server's durable outbox.
 *
 * @since 0.1.0-beta.4
 */

import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";

import { assertUnique, validate, type Names, type Unique } from "../internal/registration.ts";
import { encode as encodePayload, invalid } from "../internal/serverCodec.ts";
import type { ProtocolError, WireSchema } from "../Model.ts";
import { RegistrationError } from "../RegistrationError.ts";
import type { DeliveryDisposition, OutboxRecord } from "./SourceStorage.ts";

/**
 * A durable obligation name and its service-free payload codec.
 *
 * @category models
 * @since 0.1.0-beta.4
 */
export interface Definition<Name extends string = string, S extends WireSchema = WireSchema> {
  readonly name: Name;
  readonly payload: S;
}

/**
 * The decoded value accepted by an obligation's producer and delivery handler.
 *
 * @category type-level
 * @since 0.1.0-beta.4
 */
export type Payload<D extends Definition> = D["payload"]["Type"];

/**
 * A claimed record with its validated, decoded payload. The host retains
 * ownership of the stable id, position, attempt count, and claim timing.
 *
 * @category models
 * @since 0.1.0-beta.4
 */
export type Delivery<D extends Definition = Definition> = Omit<OutboxRecord, "payload"> & {
  readonly payload: Payload<D>;
};

/**
 * One delivery handler per declared obligation name. Applications classify
 * delivery outcomes and make destinations idempotent by the record's id.
 *
 * @category models
 * @since 0.1.0-beta.4
 */
export type Handlers<Definitions extends ReadonlyArray<Definition>, R = never> = {
  readonly [Name in Definitions[number]["name"]]: (
    record: Delivery<Extract<Definitions[number], { readonly name: Name }>>,
  ) => Effect.Effect<DeliveryDisposition, ProtocolError, R>;
};

type HandlerRequirements<H> = H extends (
  ...args: never[]
) => Effect.Effect<unknown, unknown, infer R>
  ? R
  : never;

/**
 * Defines an obligation without acquiring resources. An empty name throws
 * `RegistrationError`. Keep names and codecs compatible with pending records;
 * registering a replacement does not migrate previously committed payloads.
 *
 * @category constructors
 * @since 0.1.0-beta.4
 */
export const make = <const Name extends string, S extends WireSchema>(
  name: Name,
  payload: S,
): Definition<Name, S> => {
  validate(Schema.NonEmptyString, name, "outbox name");

  return Object.freeze({ name, payload });
};

/**
 * Validates and encodes an obligation as `{ type, payload }` JSON for
 * `Server.commit`. Schema transformations use their JSON representation.
 * Encoding failure is an `Unavailable` protocol error, not a durable rejection.
 * This performs no delivery and allocates no id; the server assigns the id when
 * committing the obligation atomically with state, events, and the receipt.
 *
 * @category encoding
 * @since 0.1.0-beta.4
 */
export const encode = <D extends Definition>(
  definition: D,
  payload: NoInfer<Payload<D>>,
): Effect.Effect<Schema.Json, ProtocolError> =>
  Effect.map(encodePayload(definition.payload, payload), (encoded) => ({
    type: definition.name,
    payload: encoded,
  }));

const decodeEnvelope = Schema.decodeUnknownEffect(
  Schema.Struct({ type: Schema.NonEmptyString, payload: Schema.Json }),
);

/**
 * Builds a delivery callback for `HostOptions.outbox.deliver`. Definitions must
 * have unique names and the handler map must match them exactly; invalid
 * registrations throw `RegistrationError` before acquiring resources.
 *
 * Malformed envelopes, unknown names, and invalid payloads fail with an
 * `UnsupportedVersion` protocol error before invoking a handler. The host
 * retries these failures. Applications can recover the callback's typed errors
 * to choose another `DeliveryDisposition`; only `Delivered` deletes a record.
 * Defects and interruption remain failures. Delivery is still at least once,
 * and destinations must deduplicate by id.
 *
 * This registry does not restrict which JSON obligations a server may commit.
 * Handler service requirements flow into the host's acquisition scope.
 *
 * @example
 * ```ts
 * import { Outbox } from "@yielded/sync/server"
 * import * as Effect from "effect/Effect"
 * import * as Schema from "effect/Schema"
 * import { deliverReindex } from "./search-index"
 *
 * const Reindex = Outbox.make("ReindexBoard", Schema.Struct({ boardId: Schema.String }))
 * const deliver = Outbox.handlers([Reindex], {
 *   ReindexBoard: ({ id, payload }) =>
 *     Effect.as(deliverReindex(id, payload.boardId), { _tag: "Delivered" })
 * })
 * ```
 *
 * @category constructors
 * @since 0.1.0-beta.4
 */
export const handlers = <
  const Definitions extends ReadonlyArray<Definition>,
  H extends Handlers<NoInfer<Definitions>, unknown>,
>(
  definitions: Definitions & Unique<Names<Definitions>>,
  implementations: H & Record<Exclude<keyof H, Definitions[number]["name"]>, never>,
): ((
  record: OutboxRecord,
) => Effect.Effect<DeliveryDisposition, ProtocolError, HandlerRequirements<H[keyof H]>>) => {
  const names = definitions.map((definition) => definition.name);

  assertUnique("outbox obligation", names);
  const expected = new Set(names);
  const actual = Object.keys(implementations);

  if (
    actual.length !== expected.size ||
    actual.some((name) => !expected.has(name)) ||
    Object.values(implementations).some((handler) => !Predicate.isFunction(handler))
  ) {
    throw RegistrationError.make({ message: "Outbox handlers must exactly match the definitions" });
  }

  type R = HandlerRequirements<H[keyof H]>;

  // Dynamic lookup erases payload types; each captured codec restores its own
  // handler's payload while this map retains the union of service requirements.
  const untyped = implementations as Readonly<
    Record<string, (record: Delivery) => Effect.Effect<DeliveryDisposition, ProtocolError, R>>
  >;

  const dispatch = new Map<
    string,
    (
      record: OutboxRecord,
      payload: Schema.Json,
    ) => Effect.Effect<DeliveryDisposition, ProtocolError, R>
  >();

  for (const definition of definitions) {
    const decode = Schema.decodeEffect(Schema.toCodecJson(definition.payload));
    const handler = untyped[definition.name];

    dispatch.set(
      definition.name,
      Effect.fnUntraced(function* (record, payload) {
        const decoded = yield* decode(payload).pipe(
          Effect.mapError(() => invalid("Outbox payload does not match its definition")),
        );

        return yield* handler({ ...record, payload: decoded });
      }),
    );
  }

  return Effect.fnUntraced(function* (record) {
    const envelope = yield* decodeEnvelope(record.payload).pipe(
      Effect.mapError(() => invalid("Invalid outbox envelope")),
    );

    const handler = dispatch.get(envelope.type);

    if (handler === undefined) return yield* invalid("Unknown outbox obligation");

    return yield* handler(record, envelope.payload);
  });
};
