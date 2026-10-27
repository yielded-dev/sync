/**
 * Scripted Sync scenarios: three clients reserve seats from one source.
 *
 * The model follows the client replica and server turn closely enough to keep the
 * animation honest. A client shows its confirmed seats with pending reservations
 * reapplied on top, in order. The server handles one command at a time, commits an
 * event and a receipt together, and answers exact retries from the receipt.
 */

export const actors = [
  { id: "a", name: "Alice" },
  { id: "b", name: "Bob" },
  { id: "c", name: "Carol" },
] as const;

export type Actor = (typeof actors)[number]["id"];
export type Node = Actor | "server";

/** Milliseconds a message spends on the wire. */
export const travel = 700;
// Concurrent commands leave this far apart; a reply leaves this long after a request lands.
const stagger = 240;
const turn = 150;

const seatCount = 3;
const recent = 3;

/** Command IDs start with their actor's letter, such as `b1` for Bob's first command. */
const actorOf = (command: string) => command[0] as Actor;

type Seats = ReadonlyArray<Actor | null>;
type Outcome = { readonly seat: number } | { readonly error: "SoldOut" };

interface Event {
  readonly cursor: number;
  readonly command: string;
  readonly seat: number;
}

interface Server {
  readonly cursor: number;
  readonly seats: Seats;
  readonly events: ReadonlyArray<Event>;
  readonly receipts: ReadonlyArray<{ readonly command: string; readonly outcome: Outcome }>;
}

export type Connection = "idle" | "connecting" | "live" | "offline";

interface Client {
  readonly connection: Connection;
  /** The last confirmed position and seats; absent until the first snapshot. */
  readonly confirmed: { readonly cursor: number; readonly seats: Seats } | undefined;
  /** Unresolved commands in order. `seen` means the event arrived but the result has not. */
  readonly pending: ReadonlyArray<{ readonly command: string; readonly seen: boolean }>;
  readonly failed: ReadonlyArray<string>;
}

export interface State {
  readonly server: Server;
  readonly clients: Readonly<Record<Actor, Client>>;
}

export type Op =
  | { readonly connect: ReadonlyArray<Actor> }
  | { readonly disconnect: Actor }
  | { readonly reserve: ReadonlyArray<string> }
  | { readonly send: ReadonlyArray<string> }
  /** Sends committed events to live clients and results to their authors. */
  | { readonly publish: ReadonlyArray<string>; readonly drop?: Actor }
  /** Reconnects from the client's cursor and replays the events it missed. */
  | { readonly resume: Actor }
  | { readonly lookup: string };

export interface Packet {
  readonly from: Node;
  readonly to: Node;
  readonly label: string;
  /** Milliseconds after the frame starts. */
  readonly delay: number;
  /** The connection drops before the packet arrives. */
  readonly lost: boolean;
  /** The packet carries a rejected result. */
  readonly rejected: boolean;
  /** The state once the receiver has handled this packet. */
  readonly after: State;
}

/** One operation: the changes that happen at once, then the packets it puts in flight. */
export interface Frame {
  readonly start: State;
  readonly packets: ReadonlyArray<Packet>;
  readonly end: State;
}

export interface Scenario {
  readonly id: string;
  readonly label: string;
  readonly setup: string;
  /** Commands committed before the scenario starts. */
  readonly history: ReadonlyArray<string>;
  readonly connected: ReadonlyArray<Actor>;
  readonly steps: ReadonlyArray<{ readonly caption: string; readonly ops: ReadonlyArray<Op> }>;
}

export const scenarios = [
  {
    id: "updates",
    label: "Live updates",
    setup: "Three clients share one source with three seats. Nobody is connected yet.",
    history: [],
    connected: [],
    steps: [
      {
        caption: "Alice and Bob connect. Each receives a snapshot at cursor 0.",
        ops: [{ connect: ["a", "b"] }],
      },
      {
        caption: "Alice reserves a seat. Her view shows it before the server replies.",
        ops: [{ reserve: ["a1"] }],
      },
      {
        caption:
          "The server accepts `a1` and commits the seat, event `#1`, and a receipt together.",
        ops: [{ send: ["a1"] }],
      },
      {
        caption: "Every connected client applies `#1`. The result confirms Alice's edit.",
        ops: [{ publish: ["a1"] }],
      },
      {
        caption: "Carol connects later and starts from a snapshot at cursor 1.",
        ops: [{ connect: ["c"] }],
      },
    ],
  },
  {
    id: "conflict",
    label: "Conflicting edits",
    setup: "Alice and Bob each hold a seat. One seat is left.",
    history: ["a0", "b0"],
    connected: ["a", "b", "c"],
    steps: [
      {
        caption:
          "Bob and Carol reserve the last seat at the same time. Each sees their own prediction.",
        ops: [{ reserve: ["b1", "c1"] }],
      },
      {
        caption:
          "The server handles one command at a time. `b1` commits as `#3`; `c1` finds no free seat.",
        ops: [{ send: ["b1", "c1"] }],
      },
      {
        caption:
          "Every client applies `#3`. Carol's prediction rolls back, and her action fails with `SoldOut`.",
        ops: [{ publish: ["b1", "c1"] }],
      },
    ],
  },
  {
    id: "retry",
    label: "Lost response",
    setup: "Bob holds a seat, and everyone is live at cursor 1.",
    history: ["b0"],
    connected: ["a", "b", "c"],
    steps: [
      {
        caption: "Alice reserves a seat. The server commits `a1` as `#2` and saves its receipt.",
        ops: [{ reserve: ["a1"] }, { send: ["a1"] }],
      },
      {
        caption:
          "Alice's connection drops before the response arrives. Bob and Carol still get `#2`.",
        ops: [{ publish: ["a1"], drop: "a" }],
      },
      {
        caption:
          "Alice reconnects from cursor 1. Replaying `#2` confirms `a1`, but its result is missing.",
        ops: [{ resume: "a" }],
      },
      {
        caption:
          "Sync looks up `a1` by its original ID. The receipt returns the first result, so Alice keeps one seat.",
        ops: [{ lookup: "a1" }],
      },
    ],
  },
  {
    id: "offline",
    label: "Offline edits",
    setup: "Alice holds a seat, and everyone is live at cursor 1.",
    history: ["a0"],
    connected: ["a", "b", "c"],
    steps: [
      { caption: "Carol goes offline at cursor 1.", ops: [{ disconnect: "c" }] },
      {
        caption: "Bob reserves a seat as `#2`. Carol misses it.",
        ops: [{ reserve: ["b1"] }, { send: ["b1"] }, { publish: ["b1"] }],
      },
      {
        caption: "Carol reserves while offline. Sync keeps `c1` pending and predicts seat 2.",
        ops: [{ reserve: ["c1"] }],
      },
      {
        caption:
          "Carol reconnects and replays `#2`. Sync reapplies `c1` on top, moving her prediction to seat 3.",
        ops: [{ resume: "c" }],
      },
      {
        caption:
          "Before resending, Sync looks up `c1`. The server has no receipt, so it never arrived.",
        ops: [{ lookup: "c1" }],
      },
      {
        caption:
          "Sync sends `c1` with its original ID. It commits as `#3`, and every client converges.",
        ops: [{ send: ["c1"] }, { publish: ["c1"] }],
      },
    ],
  },
] as const satisfies ReadonlyArray<Scenario>;

export type ScenarioId = (typeof scenarios)[number]["id"];

/** One looping story through the main behaviors, for the landing page. */
export const tour = {
  id: "tour",
  label: "Sync",
  setup: "Three clients share one source with three seats.",
  history: [],
  connected: ["a", "b", "c"],
  steps: [
    {
      caption:
        "Alice reserves a seat. It shows at once, then the server commits `#1` for everyone.",
      ops: [{ reserve: ["a1"] }, { send: ["a1"] }, { publish: ["a1"] }],
    },
    {
      caption: "Bob reserves, but his connection drops before the reply arrives.",
      ops: [{ reserve: ["b1"] }, { send: ["b1"] }, { publish: ["b1"], drop: "b" }],
    },
    {
      caption:
        "Bob reconnects and replays `#2`. Looking up `b1` returns its receipt: one seat, not two.",
      ops: [{ resume: "b" }, { lookup: "b1" }],
    },
    {
      caption:
        "Alice and Carol race for the last seat. Carol lands first; Alice's prediction rolls back with `SoldOut`.",
      ops: [{ reserve: ["c1", "a2"] }, { send: ["c1", "a2"] }, { publish: ["c1", "a2"] }],
    },
  ],
} as const satisfies Scenario;

const execute = (server: Server, command: string): Server => {
  // An exact retry returns its receipt without running the handler again.
  if (server.receipts.some((receipt) => receipt.command === command)) return server;
  const seat = server.seats.indexOf(null);

  if (seat === -1)
    return {
      ...server,
      receipts: [...server.receipts, { command, outcome: { error: "SoldOut" } }],
    };
  const cursor = server.cursor + 1;

  return {
    cursor,
    seats: server.seats.map((holder, index) => (index === seat ? actorOf(command) : holder)),
    events: [...server.events, { cursor, command, seat }],
    receipts: [...server.receipts, { command, outcome: { seat } }],
  };
};

const update = (state: State, actor: Actor, change: (client: Client) => Client): State => ({
  ...state,
  clients: { ...state.clients, [actor]: change(state.clients[actor]) },
});

const connection =
  (value: Connection) =>
  (client: Client): Client => ({ ...client, connection: value });

const snapshot =
  (server: Server) =>
  (client: Client): Client => ({
    ...client,
    connection: "live",
    confirmed: { cursor: server.cursor, seats: server.seats },
  });

const receive =
  (events: ReadonlyArray<Event>) =>
  (client: Client): Client => {
    if (client.confirmed === undefined) throw new Error("Events need a confirmed snapshot");
    let { cursor, seats } = client.confirmed;

    for (const event of events) {
      if (event.cursor !== cursor + 1) throw new Error(`Event #${event.cursor} skips a cursor`);
      seats = seats.map((holder, index) =>
        index === event.seat ? actorOf(event.command) : holder,
      );
      cursor = event.cursor;
    }

    return {
      ...client,
      connection: "live",
      confirmed: { cursor, seats },
      pending: client.pending.map((intent) =>
        events.some((event) => event.command === intent.command)
          ? { ...intent, seen: true }
          : intent,
      ),
    };
  };

const settle =
  (command: string, outcome: Outcome) =>
  (client: Client): Client => ({
    ...client,
    pending: client.pending.filter((intent) => intent.command !== command),
    failed: "error" in outcome ? [...client.failed, command] : client.failed,
  });

const result = (command: string, outcome: Outcome) =>
  `${command} ${"error" in outcome ? "✕" : "✓"}`;

interface Draft {
  readonly from: Node;
  readonly to: Node;
  readonly label: string;
  readonly delay: number;
  readonly lost?: boolean;
  readonly rejected?: boolean;
  readonly deliver?: (state: State) => State;
}

const plan = (state: State, op: Op): { readonly start: State; readonly drafts: Array<Draft> } => {
  const { server } = state;

  if ("connect" in op)
    return {
      start: op.connect.reduce(
        (next, actor) => update(next, actor, connection("connecting")),
        state,
      ),
      drafts: op.connect.map((actor) => ({
        from: "server",
        to: actor,
        label: "snapshot",
        delay: 0,
        deliver: (next) => update(next, actor, snapshot(next.server)),
      })),
    };
  if ("disconnect" in op)
    return { start: update(state, op.disconnect, connection("offline")), drafts: [] };
  if ("reserve" in op)
    return {
      start: op.reserve.reduce(
        (next, command) =>
          update(next, actorOf(command), (client) => ({
            ...client,
            pending: [...client.pending, { command, seen: false }],
          })),
        state,
      ),
      drafts: [],
    };
  if ("send" in op)
    return {
      start: state,
      drafts: op.send.map((command, index) => ({
        from: actorOf(command),
        to: "server",
        label: command,
        delay: index * stagger,
        deliver: (next) => ({ ...next, server: execute(next.server, command) }),
      })),
    };
  if ("publish" in op) {
    const commands: ReadonlyArray<string> = op.publish;
    const events = server.events.filter((event) => commands.includes(event.command));

    const drafts = actors.flatMap(({ id }): Array<Draft> => {
      if (state.clients[id].connection !== "live") return [];

      const results = server.receipts.filter(
        (receipt) => commands.includes(receipt.command) && actorOf(receipt.command) === id,
      );

      if (events.length === 0 && results.length === 0) return [];

      return [
        {
          from: "server",
          to: id,
          label: [
            ...events.map((event) => `#${event.cursor}`),
            ...results.map((receipt) => result(receipt.command, receipt.outcome)),
          ].join(" · "),
          delay: 0,
          lost: id === op.drop,
          rejected: results.some(({ outcome }) => "error" in outcome),
          deliver: (next) =>
            results.reduce(
              (settled, receipt) => update(settled, id, settle(receipt.command, receipt.outcome)),
              update(next, id, receive(events)),
            ),
        },
      ];
    });

    return {
      start: op.drop === undefined ? state : update(state, op.drop, connection("offline")),
      drafts,
    };
  }
  if ("resume" in op) {
    const actor = op.resume;
    const cursor = state.clients[actor].confirmed?.cursor ?? 0;
    const missed = server.events.filter((event) => event.cursor > cursor);
    const [first] = missed;
    const last = missed.at(-1);

    return {
      start: update(state, actor, connection("connecting")),
      drafts: [
        { from: actor, to: "server", label: `since #${cursor}`, delay: 0 },
        {
          from: "server",
          to: actor,
          label:
            first === undefined || last === undefined
              ? "up to date"
              : first === last
                ? `#${first.cursor}`
                : `#${first.cursor}–#${last.cursor}`,
          delay: travel + turn,
          deliver: (next) => update(next, actor, receive(missed)),
        },
      ],
    };
  }
  const command = op.lookup;
  const actor = actorOf(command);
  const receipt = server.receipts.find((candidate) => candidate.command === command);

  return {
    start: state,
    drafts: [
      { from: actor, to: "server", label: `lookup ${command}`, delay: 0 },
      receipt === undefined
        ? { from: "server", to: actor, label: "not found", delay: travel + turn }
        : {
            from: "server",
            to: actor,
            label: result(command, receipt.outcome),
            delay: travel + turn,
            rejected: "error" in receipt.outcome,
            deliver: (next) => update(next, actor, settle(command, receipt.outcome)),
          },
    ],
  };
};

const frame = (state: State, op: Op): Frame => {
  const { start, drafts } = plan(state, op);
  let current = start;

  // Receivers handle packets in arrival order, so the server sees `b1` before `c1`.
  const packets = drafts
    .sort((a, b) => a.delay - b.delay)
    .map(({ deliver, lost = false, rejected = false, ...packet }) => {
      if (!lost && deliver !== undefined) current = deliver(current);

      return { ...packet, lost, rejected, after: current };
    });

  return { start, packets, end: current };
};

export const initial = (scenario: Scenario): State => {
  const server = scenario.history.reduce<Server>(execute, {
    cursor: 0,
    seats: Array.from({ length: seatCount }, () => null),
    events: [],
    receipts: [],
  });

  const idle: Client = { connection: "idle", confirmed: undefined, pending: [], failed: [] };

  return {
    server,
    clients: {
      a: scenario.connected.includes("a") ? snapshot(server)(idle) : idle,
      b: scenario.connected.includes("b") ? snapshot(server)(idle) : idle,
      c: scenario.connected.includes("c") ? snapshot(server)(idle) : idle,
    },
  };
};

/** Every frame of a scenario, with the state at the end of each step. */
export const timeline = (scenario: Scenario) => {
  const start = initial(scenario);
  let state = start;

  const steps = scenario.steps.map(({ ops }) => {
    const frames = ops.map((op) => {
      const next = frame(state, op);

      state = next.end;

      return next;
    });

    return { frames, end: state };
  });

  return { start, steps };
};

interface Seat {
  readonly holder: Actor | null;
  readonly predicted: boolean;
}

interface Item {
  readonly key: string;
  readonly text: string;
  readonly kind?: "pending" | "seen" | "failed";
}

export interface ClientView {
  readonly connection: Connection;
  readonly cursor: number | undefined;
  readonly seats: ReadonlyArray<Seat> | undefined;
  readonly chips: ReadonlyArray<Item>;
}

export interface ServerView {
  readonly cursor: number;
  readonly seats: ReadonlyArray<Seat>;
  readonly events: ReadonlyArray<Item>;
  readonly receipts: ReadonlyArray<Item>;
}

export const clientView = (state: State, actor: Actor): ClientView => {
  const client = state.clients[actor];
  const seats = client.confirmed?.seats.map((holder): Seat => ({ holder, predicted: false }));

  // Like a replica view: unconfirmed commands reapplied over confirmed state, in order.
  for (const intent of client.pending) {
    const free = seats?.findIndex((seat) => seat.holder === null) ?? -1;

    if (!intent.seen && seats !== undefined && free !== -1)
      seats[free] = { holder: actor, predicted: true };
  }

  return {
    connection: client.connection,
    cursor: client.confirmed?.cursor,
    seats,
    chips: [
      ...client.pending.map(({ command, seen }): Item => ({
        key: `${command}:${seen}`,
        text: `${command} ${seen ? "awaiting result" : "pending"}`,
        kind: seen ? "seen" : "pending",
      })),
      ...client.failed.map((command): Item => ({
        key: `${command}:failed`,
        text: `${command} SoldOut`,
        kind: "failed",
      })),
    ],
  };
};

export const serverView = ({ server }: State): ServerView => ({
  cursor: server.cursor,
  seats: server.seats.map((holder) => ({ holder, predicted: false })),
  events: server.events.slice(-recent).map((event) => ({
    key: `${event.cursor}`,
    text: `#${event.cursor} ${event.command}`,
  })),
  receipts: server.receipts
    .slice(-recent)
    .map(({ command, outcome }): Item =>
      "error" in outcome
        ? { key: command, text: `${command} ✕ SoldOut`, kind: "failed" }
        : { key: command, text: `${command} ✓ seat ${outcome.seat + 1}` },
    ),
});

const escape = (text: string) =>
  text.replace(/[&<>"]/g, (character) => `&#${character.charCodeAt(0)};`);

const tag = (
  name: string,
  attributes: Readonly<Record<string, string | boolean | undefined>>,
  content = "",
) => {
  const rendered = Object.entries(attributes).flatMap(([key, value]) =>
    value === undefined || value === false
      ? []
      : [value === true ? key : `${key}="${escape(value)}"`],
  );

  return `<${[name, ...rendered].join(" ")}>${content}</${name}>`;
};

// Changes are marked against the previous view so CSS can animate what moved.
const seatChange = (seat: Seat, before: Seat | undefined) => {
  if (
    before === undefined ||
    (before.holder === seat.holder && before.predicted === seat.predicted)
  )
    return undefined;
  if (seat.holder === null) return "release";
  if (before.holder === seat.holder) return "confirm";
  if (before.holder !== null) return "replace";

  return seat.predicted ? "predict" : "commit";
};

const seatsHtml = (seats: ReadonlyArray<Seat> | undefined, before?: ReadonlyArray<Seat>) =>
  tag(
    "div",
    { class: "seats" },
    Array.from({ length: seatCount }, (_, index) => {
      const seat = seats?.[index];

      if (seat === undefined) return tag("span", { class: "seat", "data-unknown": true });

      return tag(
        "span",
        {
          class: "seat",
          "data-holder": seat.holder ?? undefined,
          "data-predicted": seat.predicted,
          "data-change": seatChange(seat, before?.[index]),
        },
        seat.holder?.toUpperCase() ?? "",
      );
    }).join(""),
  );

const itemsHtml = (
  name: "ol" | "ul",
  className: string,
  items: ReadonlyArray<Item>,
  before?: ReadonlyArray<Item>,
) =>
  tag(
    name,
    { class: className },
    items
      .map((item) =>
        tag(
          "li",
          {
            "data-kind": item.kind,
            "data-change":
              before !== undefined && !before.some(({ key }) => key === item.key)
                ? "enter"
                : undefined,
          },
          escape(item.text),
        ),
      )
      .join(""),
  );

const changed = (value: unknown, before: unknown, present: boolean) =>
  present && value !== before ? "" : undefined;

const status = {
  idle: "not connected",
  connecting: "connecting",
  live: "live",
  offline: "offline",
};

/** A client's panel contents. Pass the previous view to mark changes for animation. */
export const clientHtml = (actor: Actor, view: ClientView, before?: ClientView) =>
  [
    tag(
      "p",
      { class: "head" },
      tag("span", { class: "name" }, actors.find(({ id }) => id === actor)?.name ?? actor) +
        tag(
          "span",
          {
            class: "status",
            "data-change": changed(view.connection, before?.connection, before !== undefined),
          },
          status[view.connection],
        ),
    ),
    tag(
      "p",
      {
        class: "cursor",
        "data-change": changed(view.cursor, before?.cursor, before !== undefined),
      },
      view.cursor === undefined ? "no snapshot" : `cursor ${view.cursor}`,
    ),
    seatsHtml(view.seats, before?.seats),
    itemsHtml("ul", "chips", view.chips, before?.chips),
  ].join("");

/** The server's panel contents. Pass the previous view to mark changes for animation. */
export const serverHtml = (view: ServerView, before?: ServerView) =>
  [
    tag(
      "p",
      { class: "head" },
      tag("span", { class: "name" }, "Sync server") +
        tag(
          "span",
          {
            class: "cursor",
            "data-change": changed(view.cursor, before?.cursor, before !== undefined),
          },
          `cursor ${view.cursor}`,
        ),
    ),
    tag(
      "div",
      { class: "columns" },
      [
        tag("p", { class: "label" }, "Seats") + seatsHtml(view.seats, before?.seats),
        tag("p", { class: "label" }, "Events") +
          itemsHtml("ol", "log", view.events, before?.events),
        tag("p", { class: "label" }, "Receipts") +
          itemsHtml("ul", "log", view.receipts, before?.receipts),
      ]
        .map((column) => tag("div", { class: "column" }, column))
        .join(""),
    ),
  ].join("");

/** Splits text with backtick `code` spans into text and code parts. */
export const inline = (text: string) =>
  text.split("`").map((part, index) => (index % 2 === 1 ? { code: part } : { text: part }));
