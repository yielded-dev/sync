/**
 * Failures in synchronous source, action, plugin, or handler registration.
 *
 * @since 0.1.0-beta.0
 */

import * as Schema from "effect/Schema";

/**
 * Thrown when a definition has invalid identifiers, duplicate registrations,
 * or handlers or reducers that do not match its contract.
 *
 * These are configuration errors detected before runtime resource acquisition.
 *
 * @category errors
 * @since 0.1.0-beta.0
 */
export class RegistrationError extends Schema.TaggedError<RegistrationError>()(
  "RegistrationError",
  {
    message: Schema.String,
  },
) {}
