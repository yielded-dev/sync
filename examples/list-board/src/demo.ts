import { Schema } from "effect";

export const DemoActor = Schema.Literals(["alice", "bob"]);

export const actorName = (actorId: string) =>
  actorId === "alice" ? "Alice" : actorId === "bob" ? "Bob" : actorId;
