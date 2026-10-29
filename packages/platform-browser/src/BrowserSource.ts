/**
 * Browser services and Effect Atom bindings for scoped Sync clients.
 * Registry lifetime owns the connection; applications choose actors and persistence.
 *
 * @since 0.1.0-beta.0
 */

import { BrowserCrypto } from "@effect/platform-browser";
import { ProtocolError, type Source } from "@yielded/sync";
import { SourceAtom } from "@yielded/sync/atom";
import { Client, type ReplicaPersistence } from "@yielded/sync/client";
import type * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Atom from "effect/reactivity/Atom";
import * as Socket from "effect/socket/Socket";

type Services = Client.CurrentActor | Client.Transport | SecureRandom.SecureRandom;

/**
 * Browser source identity, actor, transport, and persistence choices.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Options<S extends Source.Spec, E = never>
  extends
    Omit<SourceAtom.BindOptions<S, Services, E>, "runtime" | "address" | "persistence">,
    Omit<Client.SourceWebSocketOptions<S>, "origin"> {
  /** Local session ownership; authentication remains part of the connection. */
  readonly actorId: string;
  /** Defaults to the current page's origin when the runtime is acquired. */
  readonly origin?: string;
  /** A supplied Layer selects persistent mode without a volatile fallback. */
  readonly persistence: "volatile" | Layer.Layer<ReplicaPersistence, E, Services>;
}

/**
 * Source Atom bindings and their shared browser runtime, including browser crypto.
 *
 * @category models
 * @since 0.1.0-beta.0
 */
export interface Binding<S extends Source.Spec, E = never> extends SourceAtom.Binding<
  S,
  E | ProtocolError,
  Services
> {
  /** Use the same browser services, including SecureRandom, in application workflow atoms. */
  readonly runtime: Atom.AtomRuntime<Services, E | ProtocolError>;
}

/**
 * Define once, or use Atom.family for parameterized IDs. Browser services and the
 * source lease are acquired lazily per registry. Dispose the old registry before
 * switching actors. Persistence, query parameters, and concurrency are app choices.
 *
 * @category constructors
 * @since 0.1.0-beta.0
 */
export const bind = <S extends Source.Spec, E = never>(
  definition: Client.Definition<S, Services>,
  options: Options<NoInfer<S>, E>,
): Binding<S, E> => {
  const { id, actorId, origin, query, persistence, ...binding } = options;
  const address = definition.contract.address(id);

  const transport = Layer.unwrap(
    Effect.gen(function* () {
      const sourceOrigin = origin ?? globalThis.location?.origin;

      if (sourceOrigin === undefined) {
        return yield* ProtocolError.make({
          reason: "UnsupportedVersion",
          message: "Supply an origin when the current page has none",
        });
      }

      return Client.layerWebSocketTransport(definition.contract, {
        origin: sourceOrigin,
        id,
        query,
      });
    }),
  ).pipe(Layer.provide(Socket.layerWebSocketConstructorGlobal));

  const platform = Layer.mergeAll(
    transport,
    BrowserCrypto.layerSecureRandom,
    Layer.succeed(Client.CurrentActor, { actorId }),
  );

  if (persistence === "volatile") {
    const runtime = Atom.runtime(platform);

    return {
      ...SourceAtom.bind(definition, {
        ...binding,
        runtime,
        address,
        persistence: { mode: "volatile" },
      }),
      runtime,
    };
  }

  const runtime = Atom.runtime(persistence.pipe(Layer.provideMerge(platform)));

  return {
    ...SourceAtom.bind(definition, {
      ...binding,
      runtime,
      address,
      persistence: { mode: "persistent" },
    }),
    runtime,
  };
};
