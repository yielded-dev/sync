import { useAtom, useAtomMount, useAtomSet, useAtomValue } from "@effect/atom-react";
import type { Client } from "@yielded/sync/client";
import { Cause, Option, Schema } from "effect";
import { AsyncResult } from "effect/reactivity";
import type { ReactNode } from "react";

import { type Card, Lane } from "../contract.ts";
import { actorName } from "../demo.ts";
import {
  actorHref,
  actorId,
  addCardAtom,
  announcePresenceAtom,
  boardAtom,
  type BoardError,
  boardId,
  cardDraftAtom,
  errorMessage,
  focusedCardAtom,
  moveCardAtom,
  otherActor,
  peersAtom,
  type Peer,
  reconnectAtom,
  renameBoardAtom,
  retryChangeAtom,
  titleDraftAtom,
} from "./atoms.ts";

const lanes = [
  { id: "todo", title: "To do", empty: "Your next idea starts here." },
  { id: "doing", title: "In progress", empty: "Choose In progress on a card to start." },
  { id: "done", title: "Done", empty: "Completed cards have a home here." },
] as const;

function Icon({ name }: { readonly name: "plus" | "arrow" | "edit" | "check" | "chevron" }) {
  const paths = {
    plus: "M12 5v14M5 12h14",
    arrow: "M7 17 17 7M7 7h10v10",
    edit: "m15 5 4 4M4 20l5-1L20 8a2.8 2.8 0 0 0-4-4L5 15l-1 5Z",
    check: "m5 12 4 4L19 6",
    chevron: "m8 10 4 4 4-4",
  };

  return (
    <svg
      aria-hidden="true"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d={paths[name]} />
    </svg>
  );
}

function Avatar({ actor, you = false }: { readonly actor: string; readonly you?: boolean }) {
  return (
    <span className={`avatar avatar-${actor}`} title={`${actorName(actor)}${you ? " (you)" : ""}`}>
      {actorName(actor).slice(0, 1)}
    </span>
  );
}

function ActionError({
  result,
}: {
  readonly result: AsyncResult.AsyncResult<unknown, BoardError>;
}) {
  if (result._tag !== "Failure" || Cause.hasInterruptsOnly(result.cause)) return null;
  const error = Option.getOrUndefined(AsyncResult.error(result));

  return (
    <p className="action-error" role="alert">
      {error === undefined ? "Something went wrong. Please reload the board." : errorMessage(error)}
    </p>
  );
}

function Topbar() {
  return (
    <header className="topbar">
      <a className="brand" href={actorHref(actorId)} aria-label="Sync board home">
        <span className="brand-mark" aria-hidden="true">
          <i />
          <i />
          <i />
        </span>
        <span>
          sync<span className="brand-period">.</span>
        </span>
      </a>
      <span className="topbar-divider" />
      <span className="workspace-label">The shared workspace</span>
      <details className="identity-picker">
        <summary>
          <Avatar actor={actorId} />
          <span>{actorName(actorId)}</span>
          <Icon name="chevron" />
        </summary>
        <nav className="identity-menu" aria-label="Demo identity">
          <span className="menu-label">Switch demo identity</span>
          {(["alice", "bob"] as const).map((actor) => (
            <a
              key={actor}
              href={actorHref(actor)}
              aria-current={actor === actorId ? "page" : undefined}
            >
              <Avatar actor={actor} />
              <span>{actorName(actor)}</span>
              {actor === actorId && <Icon name="check" />}
            </a>
          ))}
          <p>Each identity keeps its own saved session.</p>
        </nav>
      </details>
    </header>
  );
}

function BoardTitle({ title, editable }: { readonly title: string; readonly editable: boolean }) {
  const [draft, setDraft] = useAtom(titleDraftAtom);
  const [result, rename] = useAtom(renameBoardAtom);

  return (
    <div className="board-title">
      {draft === null ? (
        <div className="title-line">
          <h1>{title}</h1>
          <button
            className="icon-button rename-button"
            type="button"
            aria-label="Rename board"
            disabled={!editable}
            onClick={() => setDraft(title)}
          >
            <Icon name="edit" />
          </button>
        </div>
      ) : (
        <form
          className="rename-form"
          onSubmit={(event) => {
            event.preventDefault();
            rename();
          }}
        >
          <label className="sr-only" htmlFor="board-title">
            Board title
          </label>
          <input
            id="board-title"
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Escape") setDraft(null);
            }}
            maxLength={80}
            disabled={result.waiting}
            autoFocus
            required
          />
          <button
            className="button button-primary button-small"
            disabled={!editable || result.waiting || draft.trim().length === 0}
          >
            Save
          </button>
          <button
            className="button button-quiet button-small"
            type="button"
            disabled={result.waiting}
            onClick={() => setDraft(null)}
          >
            Cancel
          </button>
        </form>
      )}
      <ActionError result={result} />
    </div>
  );
}

function ConnectionStatus({
  connection,
  pending,
}: {
  readonly connection: Client.Connection;
  readonly pending: number;
}) {
  const labels: Record<Client.Connection, string> = {
    idle: "Connecting",
    connecting: "Connecting",
    live: pending > 0 ? "Saving changes" : "Live",
    recovering: "Reconnecting",
    parked: "Disconnected",
    closed: "Session closed",
  };

  return (
    <span className={`connection-status connection-${connection}`} role="status">
      <span className="status-dot" />
      {labels[connection]}
    </span>
  );
}

function Composer({ editable }: { readonly editable: boolean }) {
  const [draft, setDraft] = useAtom(cardDraftAtom);
  const [result, addCard] = useAtom(addCardAtom);

  return (
    <div className="composer-wrap">
      <form
        className="composer"
        onSubmit={(event) => {
          event.preventDefault();
          addCard();
        }}
      >
        <span className="composer-icon">
          <Icon name="plus" />
        </span>
        <label className="sr-only" htmlFor="card-title">
          Card title
        </label>
        <input
          id="card-title"
          placeholder="What needs to get done?"
          autoComplete="off"
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          maxLength={160}
          disabled={!editable || result.waiting}
          required
        />
        <span className="input-hint" aria-hidden="true">
          ↵
        </span>
        <button
          className="button button-primary"
          disabled={!editable || result.waiting || draft.trim().length === 0}
        >
          {result._tag !== "Initial" && result.waiting ? "Adding…" : "Add card"}
          <Icon name="plus" />
        </button>
      </form>
      <ActionError result={result} />
    </div>
  );
}

function BoardCard({
  card,
  peers,
  editable,
}: {
  readonly card: typeof Card.Type;
  readonly peers: ReadonlyArray<Peer>;
  readonly editable: boolean;
}) {
  const move = useAtomSet(moveCardAtom);
  const [focusedCard, focusCard] = useAtom(focusedCardAtom);

  const editors = [
    ...new Set(
      peers.filter((peer) => peer.editingCardId === card.id).map((peer) => actorName(peer.actorId)),
    ),
  ];

  return (
    <article
      className={`board-card${editors.length > 0 ? " card-with-presence" : ""}`}
      onFocusCapture={() => focusCard(card.id)}
      onBlurCapture={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget)) focusCard(null);
      }}
    >
      <div className="card-topline">
        <span className={`card-state card-state-${card.lane}`} aria-hidden="true">
          {card.lane === "done" && <Icon name="check" />}
        </span>
        <span>{card.lane === "done" ? "Completed" : "Task"}</span>
      </div>
      <h3>{card.title}</h3>
      <div className="card-controls">
        <span>Move to</span>
        <select
          aria-label={`Move ${card.title}`}
          value={card.lane}
          disabled={!editable}
          autoFocus={focusedCard === card.id}
          onChange={(event) => {
            const lane = Schema.decodeUnknownOption(Lane)(event.target.value);

            if (Option.isSome(lane)) move({ id: card.id, lane: lane.value });
          }}
        >
          {lanes.map((lane) => (
            <option key={lane.id} value={lane.id}>
              {lane.title}
            </option>
          ))}
        </select>
      </div>
      {editors.length > 0 && (
        <div className="card-presence" role="status">
          <span className="presence-dot" />
          {editors.join(" and ")} {editors.length === 1 ? "is" : "are"} here
        </div>
      )}
    </article>
  );
}

function Notice({ children }: { readonly children: ReactNode }) {
  return (
    <div className="notice" role="status">
      {children}
    </div>
  );
}

export function App() {
  const result = useAtomValue(boardAtom);
  const presence = useAtomValue(peersAtom);
  const moveResult = useAtomValue(moveCardAtom);
  const [reconnectResult, reconnect] = useAtom(reconnectAtom);
  const [retryResult, retry] = useAtom(retryChangeAtom);

  useAtomMount(announcePresenceAtom);
  const replica = Option.getOrUndefined(AsyncResult.value(result));
  const snapshot = replica?.value ?? replica?.provisional;
  const cards = snapshot?.source.cards ?? [];
  const connection = replica?.connection ?? "connecting";
  const editable = connection === "live" && snapshot !== undefined;

  const peers =
    connection === "live" ? Option.getOrElse(AsyncResult.value(presence), () => []) : [];

  const collaborators = [...new Set(peers.map((peer) => peer.actorId))];
  const pending = replica?.pending ?? [];
  const done = cards.filter((card) => card.lane === "done").length;
  const failure = Option.getOrUndefined(AsyncResult.error(result));

  return (
    <>
      <Topbar />
      <main>
        <section className="board-header" aria-label="Board overview">
          <div className="board-eyebrow">
            <span className="eyebrow">A little space to make things happen</span>
            <ConnectionStatus connection={connection} pending={pending.length} />
          </div>
          <BoardTitle
            title={snapshot?.plugins.board.title || "Your shared board"}
            editable={editable}
          />
          <p className="board-description">
            From the first idea to the final check. Better, together.
          </p>
          <div className="board-meta">
            <div className="collaborators">
              <div className="avatar-stack">
                <Avatar actor={actorId} you />
                {collaborators.map((actor) => (
                  <Avatar key={actor} actor={actor} />
                ))}
              </div>
              <span>
                {collaborators.length === 0
                  ? "Just you here"
                  : `${collaborators.length + 1} people here`}
              </span>
            </div>
            <a
              className="button button-secondary"
              href={actorHref(otherActor)}
              target="_blank"
              rel="noreferrer"
            >
              Open as {actorName(otherActor)}
              <Icon name="arrow" />
            </a>
          </div>
        </section>

        {result._tag === "Failure" ? (
          <Notice>
            <div>
              <strong>Couldn’t open this board</strong>
              <p>
                {failure?._tag === "ClientError"
                  ? errorMessage(failure)
                  : "Please reload to start a new session."}
              </p>
            </div>
            <button className="button button-secondary" onClick={() => window.location.reload()}>
              Reload board
            </button>
          </Notice>
        ) : connection !== "live" &&
          (snapshot !== undefined || connection === "parked" || connection === "closed") ? (
          <Notice>
            <div>
              <strong>
                {connection === "parked" ? "You’re disconnected" : "Connecting to your board"}
              </strong>
              <p>
                {snapshot === undefined
                  ? "We couldn’t connect to this board. Try reconnecting."
                  : "Your saved board is visible. Editing resumes when the connection returns."}
              </p>
            </div>
            <button
              className="button button-secondary"
              disabled={reconnectResult.waiting}
              onClick={() => reconnect()}
            >
              {reconnectResult.waiting ? "Reconnecting…" : "Reconnect"}
            </button>
          </Notice>
        ) : null}
        <ActionError result={reconnectResult} />

        <Composer editable={editable} />
        <div className="board-toolbar">
          <span>
            Board <span className="board-id">/ {boardId}</span>
          </span>
          <div className="progress-summary">
            <span>
              {done} of {cards.length} complete
            </span>
            <progress aria-label="Cards completed" value={done} max={Math.max(cards.length, 1)} />
          </div>
        </div>
        <ActionError result={moveResult} />

        <div className="board-grid" aria-busy={snapshot === undefined && result._tag !== "Failure"}>
          {lanes.map((lane) => {
            const laneCards = cards.filter((card) => card.lane === lane.id);

            return (
              <section
                className={`lane lane-${lane.id}`}
                key={lane.id}
                aria-labelledby={`lane-${lane.id}`}
              >
                <div className="lane-header">
                  <span className="lane-marker" aria-hidden="true">
                    {lane.id === "done" && <Icon name="check" />}
                  </span>
                  <h2 id={`lane-${lane.id}`}>{lane.title}</h2>
                  <span
                    className="lane-count"
                    aria-label={`${laneCards.length} ${laneCards.length === 1 ? "card" : "cards"}`}
                  >
                    {laneCards.length}
                  </span>
                </div>
                <div className="lane-content">
                  {laneCards.map((card) => (
                    <BoardCard key={card.id} card={card} peers={peers} editable={editable} />
                  ))}
                  {laneCards.length === 0 && (
                    <div className="empty-lane">
                      <span className="empty-symbol" aria-hidden="true">
                        {lane.id === "done" ? <Icon name="check" /> : <Icon name="plus" />}
                      </span>
                      <p>{snapshot === undefined ? "Opening your board…" : "No cards yet"}</p>
                      <span>
                        {snapshot === undefined ? "Getting everything in sync." : lane.empty}
                      </span>
                    </div>
                  )}
                </div>
              </section>
            );
          })}
        </div>

        {pending.length > 0 && (
          <section className="pending-changes" aria-label="Pending changes">
            <strong>
              {pending.length}{" "}
              {pending.length === 1
                ? "change awaiting confirmation"
                : "changes awaiting confirmation"}
            </strong>
            <p>These changes are saved in this browser. Retry them without creating duplicates.</p>
            {pending.map((change, index) => (
              <div className="pending-change" key={change.commandId}>
                <span>
                  Change {index + 1}
                  {change.phase === "Quarantined" || change.phase === "AuthorityChanged"
                    ? " needs review before retrying."
                    : ""}
                </span>
                <button
                  className="button button-secondary button-small"
                  disabled={
                    !editable ||
                    retryResult.waiting ||
                    change.phase === "Quarantined" ||
                    change.phase === "AuthorityChanged"
                  }
                  onClick={() => retry(change.commandId)}
                >
                  Retry change
                </button>
              </div>
            ))}
            <ActionError result={retryResult} />
          </section>
        )}

        <aside className="try-together">
          <span className="together-icon" aria-hidden="true">
            <Icon name="arrow" />
          </span>
          <div>
            <strong>Two tabs. One board.</strong>
            <p>
              Open as {actorName(otherActor)}, move a card, and watch it update here. Focus a card’s
              menu to show where you’re working.
            </p>
          </div>
          <span className="demo-label">Interactive demo</span>
        </aside>
        <footer className="page-footer">
          <span>
            Made with <strong>Sync</strong> + React + Effect Atom
          </span>
          <span>Demo identities · Changes saved as you go</span>
        </footer>
      </main>
    </>
  );
}
