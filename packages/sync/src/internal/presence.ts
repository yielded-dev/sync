import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type * as AsyncResult from "effect/reactivity/AsyncResult";
import * as Atom from "effect/reactivity/Atom";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import type { Peer, PresenceOptions as Options } from "../atom/SourceAtom.ts";
import type { Lease } from "../client/Client.ts";
import { type ClientError, type Connection } from "../client/Model.ts";
import { error } from "../internal/clientModel.ts";
import type * as Source from "../Source.ts";

type Frame<A> = {
  readonly actorId: string;
  readonly connectionId: string;
} & (
  | {
      readonly _tag: "Message";
      readonly message: { readonly namespace: string; readonly payload: A };
    }
  | { readonly _tag: "MessageLeave" }
);

const PositiveMillis = Schema.Finite.check(Schema.isGreaterThan(0));

const Settings = Schema.Struct({
  heartbeat: PositiveMillis,
  ttl: PositiveMillis,
  maxPeers: Schema.Int.check(Schema.isGreaterThan(0)),
}).check(Schema.makeFilter(({ heartbeat, ttl }) => heartbeat < ttl));

const millis = (duration: Duration.Input) =>
  Option.getOrElse(Option.map(Duration.fromInput(duration), Duration.toMillis), () => NaN);

/** @internal A mounted presence atom owns both its announcements and peer subscription. */
export const make = <S extends Source.Spec, E, A>(
  runtime: Atom.AtomRuntime<never, E>,
  session: Atom.Atom<
    AsyncResult.AsyncResult<{ readonly actorId: string; readonly lease: Lease<S> }, E | ClientError>
  >,
  status: Atom.Atom<Connection>,
  namespace: string,
  payload: Atom.Atom<A>,
  publish: (lease: Lease<S>, value: A) => Effect.Effect<void, ClientError>,
  options: Options = {},
) => {
  const settings = Schema.decodeEffect(Settings)({
    heartbeat: millis(options.heartbeatInterval ?? "8 seconds"),
    ttl: millis(options.timeToLive ?? "25 seconds"),
    maxPeers: options.maxPeers ?? 32,
  }).pipe(
    Effect.mapError(() =>
      error(
        "InvalidValue",
        "Presence requires positive finite bounds and a heartbeat shorter than its time to live",
      ),
    ),
  );

  const announce = runtime
    .atom((get) => {
      const value = get(payload);
      const current = get.result(session);
      const connected = get(status) === "live";

      return Effect.gen(function* () {
        const { heartbeat } = yield* settings;

        if (!connected) return;
        const { lease } = yield* current;

        // Ephemeral delivery may be dropped; the next heartbeat sends the latest value.
        yield* publish(lease, value).pipe(Effect.ignore, Effect.repeat(Schedule.spaced(heartbeat)));
      });
    })
    .pipe(Atom.setIdleTTL(0));

  return runtime
    .atom(
      (get) => {
        get.mount(announce);
        const current = get.result(session);
        const connected = get(status) === "live";

        return Stream.unwrap(
          Effect.gen(function* () {
            const { ttl, maxPeers } = yield* settings;

            if (!connected) return Stream.succeed<ReadonlyArray<Peer<A>>>([]);
            const { actorId, lease } = yield* current;

            return lease.messages.pipe(
              // The shared envelope has already decoded every slot's message schema.
              Stream.map((frame) => frame as Frame<A>),
              Stream.filter(
                (frame) => frame._tag === "MessageLeave" || frame.message.namespace === namespace,
              ),
              Stream.merge(
                Stream.fromSchedule(Schedule.spaced(Math.min(5_000, ttl))).pipe(
                  Stream.map(() => null),
                ),
              ),
              Stream.mapEffect((frame) =>
                Effect.map(Clock.currentTimeMillis, (now) => ({ frame, now })),
              ),
              Stream.scan(
                (): ReadonlyArray<Peer<A>> => [],
                (peers, { frame, now }) => {
                  const remaining = peers.filter(
                    (peer) =>
                      peer.expiresAtMillis > now && peer.connectionId !== frame?.connectionId,
                  );

                  if (frame === null || frame._tag === "MessageLeave" || frame.actorId === actorId)
                    return remaining;

                  return [
                    ...remaining.slice(Math.max(0, remaining.length - maxPeers + 1)),
                    {
                      actorId: frame.actorId,
                      connectionId: frame.connectionId,
                      value: frame.message.payload,
                      expiresAtMillis: now + ttl,
                    },
                  ];
                },
              ),
            );
          }),
        );
      },
      { initialValue: [] as ReadonlyArray<Peer<A>> },
    )
    .pipe(Atom.setIdleTTL(0));
};
