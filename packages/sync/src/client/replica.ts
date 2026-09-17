import { comparePositions, type SourcePosition } from "../Model.ts";
import type { Definition as SourceDefinition } from "../Source.ts";
import type { Slot } from "./Definition.ts";
import { error, type ClientError, type Connection, type Intent, type Replica } from "./Model.ts";

export type Snapshot = SourceDefinition["snapshot"]["Type"];
export type Frame = SourceDefinition["envelope"]["outbound"]["Type"];
type Sequenced = SourceDefinition["envelope"]["sequenced"]["Type"];

export interface State {
  readonly authoritative:
    | { readonly position: SourcePosition; readonly snapshot: Snapshot }
    | undefined;
  readonly provisional: Snapshot | undefined;
  readonly intents: ReadonlyMap<string, Intent>;
  readonly quarantined: ReadonlySet<string>;
  readonly failures: ReadonlyArray<{ readonly commandId: string; readonly error: unknown }>;
  readonly connection: Connection;
  readonly error: ClientError | undefined;
}

export const empty = (): State => ({
  authoritative: undefined,
  provisional: undefined,
  intents: new Map(),
  quarantined: new Set(),
  failures: [],
  connection: "idle",
  error: undefined,
});

export const replaceSlot = (snapshot: Snapshot, id: string, value: unknown): Snapshot =>
  id === "$source"
    ? { ...snapshot, source: value }
    : { ...snapshot, plugins: { ...snapshot.plugins, [id]: value } };

export const slotValue = (snapshot: Snapshot, id: string): unknown =>
  id === "$source" ? snapshot.source : snapshot.plugins[id];

const confirm = (state: State, actorId: string, events: ReadonlyArray<Sequenced>): State => {
  const intents = new Map(state.intents);

  for (const event of events) {
    if (event.command?.actorId !== actorId) continue;
    const intent = intents.get(event.command.commandId);

    if (
      intent === undefined ||
      (intent.boundGeneration !== null &&
        intent.boundGeneration !== event.position.sourceAuthorityGeneration)
    )
      continue;
    intents.set(intent.command.commandId, {
      ...intent,
      phase: intent.outcome === null ? "ConfirmedAwaitingResult" : "Accepted",
      boundGeneration: event.position.sourceAuthorityGeneration,
      confirmedAt: event.position,
    });
  }

  return reflected({ ...state, intents });
};

export const reflected = (state: State): State => {
  const intents = new Map(state.intents);

  for (const [id, intent] of intents) {
    if (
      intent.phase !== "Accepted" ||
      intent.confirmedAt === null ||
      state.authoritative === undefined
    )
      continue;
    const order = comparePositions(intent.confirmedAt, state.authoritative.position);

    if (order !== undefined && order <= 0) intents.delete(id);
  }

  return { ...state, intents };
};

const merge = (
  slots: ReadonlyArray<Slot>,
  current: Snapshot,
  incoming: Snapshot,
  operation: "mergeSnapshot" | "mergeHistory",
): Snapshot => {
  let value = incoming;

  for (const slot of slots) {
    const reducer = slot[operation];

    value = replaceSlot(
      value,
      slot.id,
      reducer === undefined
        ? operation === "mergeHistory"
          ? slotValue(current, slot.id)
          : slotValue(incoming, slot.id)
        : reducer(slotValue(current, slot.id), slotValue(incoming, slot.id)),
    );
  }

  return value;
};

export const history = (
  state: State,
  slots: ReadonlyArray<Slot>,
  generation: string,
  incoming: Snapshot,
): State =>
  state.authoritative?.position.sourceAuthorityGeneration !== generation
    ? state
    : {
        ...state,
        authoritative: {
          ...state.authoritative,
          snapshot: merge(slots, state.authoritative.snapshot, incoming, "mergeHistory"),
        },
      };

/** Entire batches are checked before any event or confirmation is committed. */
export const frame = (
  state: State,
  slots: ReadonlyArray<Slot>,
  actorId: string,
  input: Frame,
): State => {
  if (input._tag === "Message" || input._tag === "MessageLeave") return state;
  if (input._tag === "ResyncRequired")
    return { ...state, connection: "recovering", error: error("Gap", input.reason) };
  if (input._tag === "Snapshot" || input._tag === "Reset") {
    const previous = state.authoritative;

    const order =
      previous === undefined ? undefined : comparePositions(input.position, previous.position);

    if (previous !== undefined && order === -1) return state;
    if (previous !== undefined && order === 0)
      return { ...state, connection: "live", error: undefined };
    if (previous !== undefined && order === undefined && input._tag !== "Reset") return state;

    const snapshot =
      previous !== undefined && order !== undefined
        ? merge(slots, previous.snapshot, input.snapshot, "mergeSnapshot")
        : input.snapshot;

    return reflected({
      ...state,
      authoritative: { position: input.position, snapshot },
      provisional: undefined,
      connection: "live",
      error: undefined,
    });
  }

  const authority = state.authoritative;
  const events = input._tag === "Event" ? [input.event] : input.events;
  const head = input._tag === "Event" ? input.event.position : input.position;

  const gap = () => ({
    ...state,
    connection: "recovering" as const,
    error: error("Gap", "Non-contiguous source events require recovery"),
  });

  if (authority === undefined || comparePositions(head, authority.position) === undefined)
    return gap();
  let cursor = authority.position.cursor;
  const fresh: Array<Sequenced> = [];

  for (const event of events) {
    if (
      event.position.sourceAuthorityGeneration !== authority.position.sourceAuthorityGeneration ||
      event.position.cursor > head.cursor
    )
      return gap();
    if (event.position.cursor <= cursor) continue;
    if (event.position.cursor !== cursor + 1) return gap();
    fresh.push(event);
    cursor += 1;
  }
  if (cursor !== Math.max(head.cursor, authority.position.cursor)) return gap();
  let snapshot = authority.snapshot;

  for (const sequenced of fresh) {
    const slot = slots.find((candidate) => candidate.id === sequenced.event.namespace);

    if (slot === undefined) throw error("InvalidValue", "Missing event reducer");

    snapshot = replaceSlot(
      snapshot,
      slot.id,
      slot.applyEvent(slotValue(snapshot, slot.id), sequenced.event.payload),
    );
  }

  return confirm(
    {
      ...state,
      authoritative: { position: { ...authority.position, cursor }, snapshot },
      connection: "live",
      error: undefined,
    },
    actorId,
    events,
  );
};

export const view = (
  state: State,
  applyIntent: (snapshot: Snapshot, intent: Intent) => Snapshot,
): Replica<Snapshot, unknown> => {
  let value = state.authoritative?.snapshot ?? state.provisional;
  const generation = state.authoritative?.position.sourceAuthorityGeneration;
  const pending: Array<Replica<Snapshot>["pending"][number]> = [];

  for (const intent of Array.from(state.intents.values()).sort((a, b) => a.order - b.order)) {
    const superseded =
      generation !== undefined &&
      intent.boundGeneration !== null &&
      generation !== intent.boundGeneration;

    pending.push({
      commandId: intent.command.commandId,
      phase: superseded ? "AuthorityChanged" : intent.phase,
    });
    if (!superseded && intent.phase !== "ConfirmedAwaitingResult" && value !== undefined)
      value = applyIntent(value, intent);
  }
  for (const commandId of state.quarantined) pending.push({ commandId, phase: "Quarantined" });

  return {
    value,
    authoritative: state.authoritative,
    provisional: state.provisional,
    pending,
    failures: state.failures,
    connection: state.connection,
    error: state.error,
  };
};
