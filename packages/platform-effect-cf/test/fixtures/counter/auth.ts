import { ProtocolError } from "@yielded/sync";
import { Effect, Schema } from "effect";

export const ActorId = Schema.NonEmptyString.pipe(Schema.brand("ActorId"));
export type ActorId = typeof ActorId.Type;

export const Principal = Schema.Struct({ actorId: ActorId }).pipe(Schema.brand("Principal"));
export type Principal = typeof Principal.Type;

// Local demonstration credentials. Replace with the application's auth provider.
export const authenticate = Effect.fn("authenticate")(function* (request: Request) {
  const token = request.headers.get("authorization");

  const actorId =
    token === "Bearer alice-local" ? "alice" : token === "Bearer bob-local" ? "bob" : undefined;

  if (actorId === undefined)
    return yield* ProtocolError.make({
      reason: "Unauthenticated",
      message: "Use a local demo credential",
    });

  const principal = Principal.make({ actorId: ActorId.make(actorId) });

  const encoded = yield* Schema.encodeEffect(Principal)(principal).pipe(
    Effect.mapError(() =>
      ProtocolError.make({ reason: "Unavailable", message: "Could not encode principal" }),
    ),
  );

  return { actorId: principal.actorId, principal: encoded, expiresAtMillis: 4_000_000_000_000 };
});
