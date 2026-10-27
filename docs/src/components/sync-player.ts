/**
 * Plays a scenario on a rendered `SyncStage`.
 *
 * A panel holds the stage (`data-stage`), a play button (`data-play`) with its label
 * (`data-play-text`), one button per step (`data-step`), and optional per-step captions
 * (`data-caption`). Steps play when the stage is visible; readers can pause or replay any
 * step. A looping player restarts after the last step. With reduced motion, playback waits
 * for the reader and changes state without movement.
 */
import {
  clientHtml,
  clientView,
  serverHtml,
  serverView,
  timeline,
  travel,
  type Actor,
  type ClientView,
  type Frame,
  type Node,
  type Packet,
  type Scenario,
  type ServerView,
  type State,
} from "./sync-scenarios.ts";

const reduce = window.matchMedia("(prefers-reduced-motion: reduce)");
// The pause between operations within a step, the rest before a loop restarts, and its fade.
const gap = 200;
const rest = 2000;
const fade = 350;
// Every step gets a beat. A caption that replaces the previous one also gets time to be read,
// counting the step's own animation.
const beat = 1100;
const reading = (caption: string) => 500 + caption.length * 24;

const wait = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export const mount = (
  panel: HTMLElement,
  scenario: Scenario,
  options: { readonly loop: boolean },
) => {
  const stage = panel.querySelector<HTMLElement>("[data-stage]");
  const play = panel.querySelector<HTMLButtonElement>("[data-play]");
  const label = play?.querySelector("[data-play-text]") ?? null;

  if (stage === null || play === null || label === null) return undefined;

  const { start, steps } = timeline(scenario);
  const items = [...panel.querySelectorAll<HTMLButtonElement>("[data-step]")];
  const captions = [...panel.querySelectorAll<HTMLElement>("[data-caption]")];
  const nodes = new Map<Node, HTMLElement>();
  const wires = new Map<Actor, HTMLElement>();

  for (const element of stage.querySelectorAll<HTMLElement>("[data-node]"))
    nodes.set(element.dataset.node as Node, element);
  for (const element of stage.querySelectorAll<HTMLElement>("[data-wire]"))
    wires.set(element.dataset.wire as Actor, element);

  // The views on screen, so a render can mark what changed.
  const clients = new Map<Actor, ClientView>();
  let server: ServerView | undefined;

  let completed = 0;
  let run = 0;
  let looping: number | undefined;
  let playing = false;
  let held = false;
  let active = false;
  let visible = false;

  const render = (node: Node, state: State, animate: boolean) => {
    const element = nodes.get(node);

    if (element === undefined) return;
    if (node === "server") {
      const view = serverView(state);

      if (animate && JSON.stringify(view) === JSON.stringify(server)) return;
      element.innerHTML = serverHtml(view, animate ? server : undefined);
      server = view;

      return;
    }

    const view = clientView(state, node);
    const before = clients.get(node);

    if (animate && JSON.stringify(view) === JSON.stringify(before)) return;
    element.innerHTML = clientHtml(node, view, animate ? before : undefined);
    element.dataset.connection = view.connection;
    wires.get(node)?.setAttribute("data-connection", view.connection);
    clients.set(node, view);
  };

  const show = (state: State, animate = false) => {
    for (const node of nodes.keys()) render(node, state, animate);
  };

  const pulse = (node: Node) => {
    const element = nodes.get(node);

    if (element === undefined) return;
    element.removeAttribute("data-pulse");
    void element.offsetWidth;
    element.setAttribute("data-pulse", "");
  };

  const highlight = (index: number) => {
    items.forEach((item, position) =>
      position === index
        ? item.setAttribute("aria-current", "step")
        : item.removeAttribute("aria-current"),
    );
    captions.forEach((caption, position) =>
      caption.toggleAttribute("data-current", position === index),
    );
  };

  const refresh = () => {
    const state = playing
      ? "pause"
      : completed === steps.length && !options.loop
        ? "replay"
        : "play";

    play.dataset.state = state;
    label.textContent = { play: "Play", pause: "Pause", replay: "Replay" }[state];
  };

  const fly = async (packet: Packet, id: number) => {
    const down = packet.from === "server";
    const wire = wires.get((down ? packet.to : packet.from) as Actor);

    await wait(packet.delay);
    if (id !== run || wire === undefined) return;

    const element = document.createElement("span");
    const [from, to] = down ? ["0%", "100%"] : ["100%", "0%"];

    element.className = "packet";
    element.dataset.direction = down ? "down" : "up";
    element.toggleAttribute("data-rejected", packet.rejected);
    element.textContent = packet.label;
    wire.append(element);

    try {
      if (packet.lost) {
        await element.animate(
          [
            { top: from, opacity: 0 },
            { opacity: 1, offset: 0.25 },
            { top: "50%", opacity: 1 },
          ],
          { duration: travel / 2, easing: "ease-in", fill: "forwards" },
        ).finished;
        element.dataset.lost = "";
        await element.animate([{ opacity: 1 }, { opacity: 0, scale: 0.85 }], {
          duration: 450,
          delay: 250,
          easing: "ease-in",
          fill: "forwards",
        }).finished;
      } else {
        await element.animate(
          [
            { top: from, opacity: 0 },
            { opacity: 1, offset: 0.15 },
            { opacity: 1, offset: 0.85 },
            { top: to, opacity: 0 },
          ],
          { duration: travel, easing: "cubic-bezier(0.45, 0, 0.55, 1)", fill: "forwards" },
        ).finished;
      }
    } catch {
      return;
    } finally {
      element.remove();
    }

    if (id !== run || packet.lost) return;
    render(packet.to, packet.after, true);
    pulse(packet.to);
  };

  const animate = async (frame: Frame, id: number) => {
    show(frame.start, true);
    await Promise.all(frame.packets.map((packet) => fly(packet, id)));
    if (id === run) show(frame.end, true);
  };

  const perform = async (index: number, id: number) => {
    const step = steps[index];

    if (step === undefined) return;
    highlight(index);
    if (reduce.matches) show(step.end, true);
    else {
      for (const [position, frame] of step.frames.entries()) {
        if (position > 0) await wait(gap);
        if (id !== run) return;
        await animate(frame, id);
      }
    }
    if (id === run) completed = index + 1;
  };

  const hold = (index: number, elapsed: number) => {
    const caption = scenario.steps[index]?.caption;

    return captions.length === 0 || caption === undefined
      ? beat
      : Math.max(beat, reading(caption) - elapsed);
  };

  const cancel = () => {
    run += 1;
    for (const animation of stage.getAnimations({ subtree: true })) animation.cancel();
  };

  const restart = async (id: number) => {
    stage.setAttribute("data-restarting", "");
    await wait(fade);
    if (id !== run) return;
    completed = 0;
    show(start);
    highlight(-1);
    stage.removeAttribute("data-restarting");
  };

  // Plays from the current step until the end, or until the reader pauses.
  const loop = async () => {
    const id = run;

    if (looping === id) return;
    looping = id;
    for (;;) {
      // Show the starting state before the first step moves.
      if (completed === 0) await wait(gap * 3);
      let pause = 0;

      while (playing && completed < steps.length && id === run) {
        const index = completed;
        const started = performance.now();

        await perform(index, id);
        pause = hold(index, performance.now() - started);
        if (playing && completed < steps.length && id === run) await wait(pause);
      }
      if (!options.loop || !playing || id !== run) break;
      await wait(Math.max(rest, pause));
      if (!playing || id !== run) break;
      await restart(id);
    }
    if (looping === id) looping = undefined;
    if (id === run && completed === steps.length && !options.loop) playing = false;
    refresh();
  };

  const resume = () => {
    if (playing || (completed === steps.length && !options.loop)) return;
    playing = true;
    refresh();
    void loop();
  };

  const autoplay = () => {
    if (active && visible && !held && !reduce.matches) resume();
  };

  play.hidden = false;
  play.addEventListener("click", () => {
    if (playing) {
      playing = false;
      held = true;
      refresh();

      return;
    }
    held = false;
    if (completed === steps.length) {
      cancel();
      completed = 0;
      show(start);
      highlight(-1);
    }
    resume();
  });

  items.forEach((item, index) =>
    item.addEventListener("click", () => {
      cancel();
      stage.removeAttribute("data-restarting");
      playing = false;
      held = true;
      completed = index;
      show(steps[index - 1]?.end ?? start);
      refresh();
      void perform(index, run).then(refresh);
    }),
  );

  new IntersectionObserver(
    ([entry]) => {
      visible = entry?.isIntersecting ?? false;
      if (visible) autoplay();
      else if (playing && !held) {
        playing = false;
        refresh();
      }
    },
    { threshold: 0.4 },
  ).observe(stage);

  return {
    activate: () => {
      active = true;
      autoplay();
    },
    deactivate: () => {
      active = false;
      playing = false;
      cancel();
      stage.removeAttribute("data-restarting");
      show(steps[completed - 1]?.end ?? start);
      highlight(completed - 1);
      refresh();
    },
  };
};
