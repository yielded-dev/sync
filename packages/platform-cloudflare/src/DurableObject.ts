import type { Source } from "@yielded/sync";
import type { Server } from "@yielded/sync/server";

import type { Options } from "./Cloudflare.ts";

/** A Sync object definition, before a framework constructs its native export. */
export interface Definition<
  S extends Source.Spec,
  R = never,
  DeliveryR = never,
  E = never,
  PlatformR = never,
> {
  readonly server: Server.Definition<S, R>;
  readonly options: Options<DeliveryR, E, PlatformR>;
  /** Construct the framework's native object; no instance storage opens here. */
  readonly provide: <A>(
    services: (definition: Definition<S, R, DeliveryR, E, PlatformR>) => A,
  ) => A;
}

/** Keep the definition independent of the framework selected by the application. */
export const make = <S extends Source.Spec, R, DeliveryR = never, E = never, PlatformR = never>(
  server: Server.Definition<S, R>,
  options: Options<DeliveryR, E, PlatformR> = {},
): Definition<S, R, DeliveryR, E, PlatformR> => {
  const definition: Definition<S, R, DeliveryR, E, PlatformR> = {
    server,
    options,
    provide: (services) => services(definition),
  };

  return definition;
};
