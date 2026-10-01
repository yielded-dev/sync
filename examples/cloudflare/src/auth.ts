import { ProtocolError } from "@yielded/sync";
import { Effect } from "effect";

// Local demonstration credentials. Replace with the application's auth provider.
export const authenticate = Effect.fn("authenticate")(function* (token: string | null | undefined) {
  const actorId =
    token === "Bearer alice-local" ? "alice" : token === "Bearer bob-local" ? "bob" : undefined;

  if (actorId === undefined)
    return yield* ProtocolError.make({
      reason: "Unauthenticated",
      message: "Use a local demo credential",
    });

  return { actorId, principal: { actorId }, expiresAtMillis: 4_000_000_000_000 };
});
