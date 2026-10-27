import { SourceAtom } from "@yielded/sync/atom";
import { Client } from "@yielded/sync/client";
import { reset } from "cloudflare:test";
import { Cause, Clock, Context, Effect, Fiber, Layer, Scheduler, Stream } from "effect";
import { Atom, AtomRegistry } from "effect/reactivity";
import { TestClock } from "effect/testing";
import { afterEach, expect, it } from "vite-plus/test";

import { BoardClient } from "../src/client.ts";
import { Board, BoardId, CardRejected } from "../src/contract.ts";
import { controlled, socketProtocol } from "./transport.ts";

// Requested test seam: multi-client races, optimistic rollback, and callable-action
// cancellation. Only delivery is controlled; domain decisions and receipts use SQLite.
const participant = Effect.fnUntraced(function* (actorId: string) {
  const services = yield* Layer.build(
    Client.layerRpcTransport(Board).pipe(
      Layer.provide(socketProtocol()),
      Layer.provideMerge(Layer.succeed(Client.CurrentActor, { actorId })),
    ),
  );

  const wire = yield* controlled(Context.get(services, Client.Transport));

  const runtime = Atom.runtime(
    Layer.succeedContext(
      services.pipe(
        Context.add(Client.Transport, wire.transport),
        Context.add(Clock.Clock, yield* Clock.Clock),
        Context.add(Scheduler.Scheduler, yield* Scheduler.Scheduler),
      ),
    ),
  );

  const board = SourceAtom.bind(BoardClient, {
    runtime,
    address: Board.address(BoardId.make("shared")),
    persistence: { mode: "volatile" },
    retry: { maxAttempts: 3, initialDelay: 10, maxDelay: 10 },
    actions: { addCard: { concurrent: true } },
  });

  const registry = yield* Effect.acquireRelease(
    Effect.sync(() => AtomRegistry.make()),
    (registry) => Effect.sync(() => registry.dispose()),
  );

  yield* AtomRegistry.mount(registry, board.replica);

  const state = (predicate: (value: Atom.Success<typeof board.replica>) => boolean) =>
    AtomRegistry.toStreamResult(registry, board.replica).pipe(
      Stream.filter(predicate),
      Stream.take(1),
      Stream.runCollect,
      Effect.map((values) => values[0]!),
    );

  const run = <A, E>(effect: Effect.Effect<A, E, AtomRegistry.AtomRegistry>) =>
    Effect.provideService(effect, AtomRegistry.AtomRegistry, registry);

  const { disconnect } = yield* wire.connection;

  yield* state((value) => value.connection === "live");

  return { ...wire, board, registry, state, run, disconnect };
});

const card = (id: string, title: string) => ({ id, title, lane: "todo" });

afterEach(() => reset());

it.each(["event", "reply"] as const)(
  "rebases pending edits after rejection and a remote update, with %s arriving first",
  (first) =>
    Effect.gen(function* () {
      const alice = yield* participant("alice");
      const bob = yield* participant("bob");

      const seeded = yield* bob
        .run(bob.board.action.addCard({ id: "one", title: "Original" }))
        .pipe(Effect.forkScoped);

      const seed = yield* bob.request;

      yield* seed.reply(yield* seed.send);
      yield* alice.deliver;
      yield* bob.deliver;
      yield* Fiber.join(seeded);
      yield* alice.state((value) => value.value?.cards.length === 1);

      const rejected = yield* alice
        .run(Effect.result(alice.board.action.addCard({ id: "one", title: "Duplicate" })))
        .pipe(Effect.forkScoped);

      const duplicate = yield* alice.request;

      const accepted = yield* alice
        .run(alice.board.action.addCard({ id: "two", title: "Survivor" }))
        .pipe(Effect.forkScoped);

      expect((yield* alice.state((value) => value.pending.length === 2)).value?.cards).toEqual([
        card("one", "Original"),
        card("one", "Duplicate"),
        card("two", "Survivor"),
      ]);

      const renamed = yield* bob
        .run(bob.board.action.rename("Bob's board"))
        .pipe(Effect.forkScoped);

      const remote = yield* bob.request;

      yield* remote.reply(yield* remote.send);
      yield* alice.deliver;
      yield* bob.deliver;
      yield* Fiber.join(renamed);
      yield* alice.state((value) => value.value?.title === "Bob's board");
      yield* duplicate.reply(yield* duplicate.send);

      const rejection = yield* Fiber.join(rejected);

      expect(rejection).toMatchObject({
        _tag: "Failure",
        failure: CardRejected.make({ reason: "Duplicate" }),
      });
      const remaining = yield* alice.state((value) => value.pending.length === 1);

      const expected = {
        title: "Bob's board",
        cards: [card("one", "Original"), card("two", "Survivor")],
      };

      expect(remaining.value).toEqual(expected);
      expect(remaining.authoritative?.snapshot.cards).toEqual([card("one", "Original")]);
      expect(remaining.failures.map((failure) => failure.commandId)).toEqual([
        duplicate.command.commandId,
      ]);

      const survivor = yield* alice.request;
      const outcome = yield* survivor.send;

      if (first === "event") {
        yield* alice.deliver;

        const confirmed = yield* alice.state((value) =>
          value.pending.some((pending) => pending.phase === "ConfirmedAwaitingResult"),
        );

        expect(confirmed.value).toEqual(expected);
        expect(confirmed.authoritative?.snapshot).toEqual(expected);
        expect(accepted.pollUnsafe()).toBeUndefined();
        yield* survivor.reply(outcome);
      } else {
        yield* survivor.reply(outcome);
        expect(yield* Fiber.join(accepted)).toEqual({ id: "two", count: 2 });

        const awaitingEvent = yield* alice.state((value) =>
          value.pending.some((pending) => pending.phase === "Accepted"),
        );

        expect(awaitingEvent.value).toEqual(expected);
        expect(awaitingEvent.authoritative?.snapshot.cards).toEqual([card("one", "Original")]);
        yield* alice.deliver;
      }
      yield* bob.deliver;
      expect(yield* Fiber.join(accepted)).toEqual({ id: "two", count: 2 });
      const settled = yield* alice.state((value) => value.pending.length === 0);

      const synchronized = yield* bob.state(
        (value) => value.pending.length === 0 && value.value?.cards.length === 2,
      );

      expect(settled.value).toEqual(expected);
      expect(settled.authoritative?.snapshot).toEqual(expected);
      expect(synchronized.value).toEqual(expected);
      expect(synchronized.authoritative).toEqual(settled.authoritative);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer()), Effect.runPromise),
);

it("recovers the original receipt after a lost reply and remote edits during disconnection", () =>
  Effect.gen(function* () {
    const alice = yield* participant("alice");
    const bob = yield* participant("bob");

    const pending = yield* alice
      .run(Effect.result(alice.board.action.addCard({ id: "one", title: "Alice" })))
      .pipe(Effect.forkScoped);

    const request = yield* alice.request;
    const original = yield* request.send;

    yield* alice.deliver;
    yield* bob.deliver;
    yield* alice.state((value) => value.pending[0]?.phase === "ConfirmedAwaitingResult");
    yield* request.drop;
    expect(yield* Fiber.join(pending)).toMatchObject({
      _tag: "Failure",
      failure: { _tag: "ClientError", commandId: request.command.commandId },
    });
    yield* alice.disconnect;
    yield* alice.state((value) => value.connection === "recovering");

    const edited = yield* bob
      .run(bob.board.action.addCard({ id: "two", title: "Bob" }))
      .pipe(Effect.forkScoped);

    const remote = yield* bob.request;

    yield* remote.reply(yield* remote.send);
    yield* bob.deliver;
    expect(yield* Fiber.join(edited)).toEqual({ id: "two", count: 2 });
    yield* TestClock.adjust(10);
    yield* alice.connection;

    const lookup = yield* alice.lookup;
    const found = yield* lookup.send;

    expect(lookup.command).toEqual(request.command);
    expect(found).toEqual({ _tag: "Found", outcome: original });
    expect(original).toMatchObject({ _tag: "Succeeded", result: { id: "one", count: 1 } });
    yield* lookup.reply(found);

    const recovered = yield* alice.state(
      (value) => value.connection === "live" && value.pending.length === 0,
    );

    const synchronized = yield* bob.state((value) => value.pending.length === 0);

    expect(recovered.value).toEqual(synchronized.value);
    expect(recovered.authoritative).toEqual(synchronized.authoritative);
    expect(recovered.value?.cards).toEqual([card("one", "Alice"), card("two", "Bob")]);
    yield* alice.run(Atom.set(alice.board.retry, request.command.commandId));
    expect(
      yield* AtomRegistry.getResult(alice.registry, alice.board.retry, { suspendOnWaiting: true }),
    ).toEqual({ id: "one", count: 1 });
    expect(alice.commands).toEqual([request.command]);
  }).pipe(Effect.scoped, Effect.provide(TestClock.layer()), Effect.runPromise));

it("cancels only its own concurrent caller and retains the exact command for retry", () =>
  Effect.gen(function* () {
    const alice = yield* participant("alice");

    const cancelled = yield* alice
      .run(alice.board.action.addCard({ id: "one", title: "Cancelled caller" }))
      .pipe(Effect.forkScoped);

    const original = yield* alice.request;

    const survivor = yield* alice
      .run(alice.board.action.addCard({ id: "two", title: "Survivor" }))
      .pipe(Effect.forkScoped);

    yield* alice.state((value) => value.pending.length === 2);
    yield* Fiber.interrupt(cancelled);
    const exit = yield* Fiber.await(cancelled);

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true);
    const next = yield* alice.request;

    yield* next.reply(yield* next.send);
    yield* alice.deliver;
    expect(yield* Fiber.join(survivor)).toEqual({ id: "two", count: 1 });
    const retained = yield* alice.state((value) => value.pending.length === 1);

    expect(retained.pending[0]?.commandId).toBe(original.command.commandId);
    expect(retained.value?.cards).toEqual([
      card("two", "Survivor"),
      card("one", "Cancelled caller"),
    ]);

    const retry = yield* alice
      .run(
        Effect.gen(function* () {
          yield* Atom.set(alice.board.retry, original.command.commandId);

          return yield* AtomRegistry.getResult(alice.registry, alice.board.retry, {
            suspendOnWaiting: true,
          });
        }),
      )
      .pipe(Effect.forkScoped);

    const lookup = yield* alice.lookup;

    expect(lookup.command).toEqual(original.command);
    yield* lookup.reply(yield* lookup.send);
    const repeated = yield* alice.request;

    expect(repeated.command).toEqual(original.command);
    yield* repeated.reply(yield* repeated.send);
    yield* alice.deliver;
    expect(yield* Fiber.join(retry)).toEqual({ id: "one", count: 2 });
    expect((yield* alice.state((value) => value.pending.length === 0)).value?.cards).toEqual([
      card("two", "Survivor"),
      card("one", "Cancelled caller"),
    ]);
  }).pipe(Effect.scoped, Effect.provide(TestClock.layer()), Effect.runPromise));

it("replaces a nonconcurrent caller while a generator waits to clear its draft", () =>
  Effect.gen(function* () {
    const alice = yield* participant("alice");

    const replaced = yield* alice
      .run(alice.board.action.rename("Old draft"))
      .pipe(Effect.forkScoped);

    const original = yield* alice.request;
    const draft = Atom.make("New draft");

    yield* AtomRegistry.mount(alice.registry, draft);

    const save = alice.board.fn(function* () {
      const title = yield* Atom.get(draft);
      const result = yield* alice.board.action.rename(title);

      yield* Atom.set(draft, "");

      return result;
    });

    const saved = yield* alice
      .run(
        Effect.gen(function* () {
          yield* Atom.set(save, undefined);

          return yield* AtomRegistry.getResult(alice.registry, save, { suspendOnWaiting: true });
        }),
      )
      .pipe(Effect.forkScoped);

    const next = yield* alice.request;
    const exit = yield* Fiber.await(replaced);

    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true);
    expect(alice.registry.get(draft)).toBe("New draft");
    const optimistic = yield* alice.state((value) => value.value?.title === "New draft");

    expect(optimistic.pending.map((value) => value.commandId)).toEqual([
      original.command.commandId,
      next.command.commandId,
    ]);
    yield* next.reply(yield* next.send);
    yield* alice.deliver;
    expect(yield* Fiber.join(saved)).toBe("New draft");
    expect(alice.registry.get(draft)).toBe("");
    expect((yield* alice.state((value) => value.pending.length === 1)).pending[0]?.commandId).toBe(
      original.command.commandId,
    );
  }).pipe(Effect.scoped, Effect.provide(TestClock.layer()), Effect.runPromise));
