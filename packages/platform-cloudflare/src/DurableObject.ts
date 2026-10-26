import type { Source } from "@yielded/sync";
import type { Server } from "@yielded/sync/server";

import type { Options } from "./Cloudflare.ts";

type Constructor = abstract new (...args: never[]) => unknown;

type WithInstanceType<A extends Constructor> = A & {
  /** Type-only native instance; use `typeof Object.Type`. */
  readonly Type: InstanceType<A>;
};

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
  readonly provide: <A extends Constructor>(
    services: (definition: Definition<S, R, DeliveryR, E, PlatformR>) => A,
  ) => WithInstanceType<A>;
}

/** Keep the definition independent of the framework selected by the application. */
export const make = <S extends Source.Spec, R, DeliveryR = never, E = never, PlatformR = never>(
  server: Server.Definition<S, R>,
  options: Options<DeliveryR, E, PlatformR> = {},
): Definition<S, R, DeliveryR, E, PlatformR> => {
  const definition: Definition<S, R, DeliveryR, E, PlatformR> = {
    server,
    options,
    provide: (services) => services(definition) as WithInstanceType<ReturnType<typeof services>>,
  };

  return definition;
};
