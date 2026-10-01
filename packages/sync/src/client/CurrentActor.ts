import { Context } from "effect";

/** Local session identity, supplied by the application and captured at acquisition. */
export class CurrentActor extends Context.Service<CurrentActor, { readonly actorId: string }>()(
  "@yielded/sync/client/CurrentActor",
) {}
