import { comparePositions } from "@yielded/sync";
import {
  SourceStorage,
  StorageError,
  type StoredEvent,
  type StoredHead,
  type StoredReceipt,
} from "@yielded/sync/server";
import { Effect, Semaphore } from "effect";

/** Demo-only storage: one authority, at most 64 commands, no restart durability. */
export const makeStorage = Effect.gen(function* () {
  const lock = yield* Semaphore.make(1);

  let saved: {
    head: StoredHead | undefined;
    events: ReadonlyArray<StoredEvent>;
    receipts: ReadonlyArray<StoredReceipt>;
  } = { head: undefined, events: [], receipts: [] };

  return SourceStorage.of({
    transaction: (body) =>
      Effect.gen(function* () {
        const draft = structuredClone(saved);
        let active = true;

        const operation = <A>(f: () => A) =>
          Effect.try({
            try: () => {
              if (!active) throw new Error("Demo transaction is closed");

              return f();
            },
            catch: (cause) =>
              StorageError.make({ message: "Demo storage rejected operation", cause }),
          });

        const value = yield* body({
          readHead: operation(() => structuredClone(draft.head)),
          initialize: (head) =>
            operation(() => {
              if (draft.head !== undefined) throw new Error("Authority already initialized");
              draft.head = structuredClone(head);
            }),
          readReceipt: (identity) =>
            operation(() =>
              structuredClone(
                draft.receipts.find(
                  (receipt) =>
                    receipt.actorId === identity.actorId &&
                    receipt.commandId === identity.commandId &&
                    (identity.sourceAuthorityGeneration === null ||
                      receipt.sourceAuthorityGeneration === identity.sourceAuthorityGeneration),
                ),
              ),
            ),
          readEvents: (after, limit) =>
            operation(() =>
              structuredClone(
                draft.events
                  .filter((event) => comparePositions(event.position, after) === 1)
                  .slice(0, limit),
              ),
            ),
          commit: (plan) =>
            operation(() => {
              if (plan.outbox.length > 0) throw new Error("Demo storage does not support outbox");
              if (
                draft.head === undefined ||
                comparePositions(draft.head.position, plan.expected) !== 0
              )
                throw new Error("Source position changed");
              if (draft.receipts.length >= 64 || draft.events.length + plan.events.length > 64)
                throw new Error("Demo storage capacity reached");
              if (
                plan.head.position.sourceAuthorityGeneration !==
                  plan.expected.sourceAuthorityGeneration ||
                plan.head.position.cursor !== plan.expected.cursor + plan.events.length ||
                plan.events.some(
                  (event, index) =>
                    event.position.sourceAuthorityGeneration !==
                      plan.expected.sourceAuthorityGeneration ||
                    event.position.cursor !== plan.expected.cursor + index + 1,
                )
              )
                throw new Error("Events must be contiguous within the authority");
              if (
                draft.receipts.some(
                  (receipt) =>
                    receipt.actorId === plan.receipt.actorId &&
                    receipt.commandId === plan.receipt.commandId &&
                    receipt.sourceAuthorityGeneration === plan.receipt.sourceAuthorityGeneration,
                )
              )
                throw new Error("Command identity already committed");

              draft.head = structuredClone(plan.head);
              draft.events = [...draft.events, ...structuredClone(plan.events)];
              draft.receipts = [...draft.receipts, structuredClone(plan.receipt)];
            }),
        }).pipe(
          Effect.ensuring(
            Effect.sync(() => {
              active = false;
            }),
          ),
        );

        saved = draft;

        return value;
      }).pipe(Semaphore.withPermit(lock)),
  });
});
