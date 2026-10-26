import { BrowserCrypto } from "@effect/platform-browser";
import { ProtocolError, type Source } from "@yielded/sync";
import { SourceAtom } from "@yielded/sync/atom";
import { Client, type ReplicaPersistence } from "@yielded/sync/client";
import { type Crypto, Effect, Layer } from "effect";
import { Atom } from "effect/reactivity";
import { Socket } from "effect/socket";

type Services = Client.CurrentActor | Client.Transport | Crypto.Crypto;

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

export interface Binding<S extends Source.Spec, E = never> extends SourceAtom.Binding<
  S,
  E | ProtocolError
> {
  /** Use the same browser services, including Crypto, in application workflow atoms. */
  readonly runtime: Atom.AtomRuntime<Services, E | ProtocolError>;
}

/**
 * Define once, or use Atom.family for parameterized IDs. Browser services and the
 * source lease are acquired lazily per registry. Dispose the old registry before
 * switching actors. Persistence, query parameters, and concurrency are app choices.
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
    BrowserCrypto.layer,
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
