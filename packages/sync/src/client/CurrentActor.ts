/**
 * The authenticated actor captured by a scoped client session.
 *
 * @since 0.1.0-beta.0
 */

import * as Context from "effect/Context";

/**
 * Supplies the actor id used to correlate commands, receipts, and persistence.
 *
 * Close the previous client scope before changing actors. This service
 * identifies the client session; the server authenticates requests independently.
 *
 * @category services
 * @since 0.1.0-beta.0
 */
export class CurrentActor extends Context.Service<CurrentActor, { readonly actorId: string }>()(
  "@yielded/sync/client/CurrentActor",
) {}
