import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { Effect } from "effect";
import { afterAll, beforeAll, expect, it, vi } from "vite-plus/test";

import * as scenarios from "../../sync/test/persistence-scenarios.ts";
import { ExpoSqlite } from "../src/index.ts";

vi.mock("expo-sqlite", () => ({
  openDatabaseAsync: async (name: string, _options: unknown, directory: string) => {
    const db = new DatabaseSync(join(directory, name));

    return {
      execAsync: async (sql: string) => {
        db.exec(sql);
      },
      getFirstAsync: async <T>(sql: string, ...params: Array<string>) =>
        (db.prepare(sql).get(...params) ?? null) as T | null,
      runAsync: async (sql: string, ...params: Array<string>) => db.prepare(sql).run(...params),
      closeAsync: async () => {
        db.close();
      },
    };
  },
}));

let directory: string;

beforeAll(async () => {
  directory = await mkdtemp(join(tmpdir(), "sync-sqlite-"));
});
afterAll(async () => {
  await rm(directory, { recursive: true, force: true });
});

const open: scenarios.Open = (options) => ExpoSqlite.open({ ...options, directory });

it("retains exact evidence and snapshots after closing all connections", async () => {
  await Effect.runPromise(scenarios.seed(open, "reopen"));
  expect(await Effect.runPromise(scenarios.restore(open, "reopen"))).toBe("restored");
});
it("commits atomically, evicts only cache, and fences independent connections", async () => {
  expect(await Effect.runPromise(scenarios.exercise(open, "operations"))).toContain(
    "generation fencing",
  );
  expect(await Effect.runPromise(scenarios.concurrent(open, "concurrent"))).toBe(
    "conflict and interruption rollback",
  );
});
it("background flushes and recovers immutable commands on reopen", async () => {
  const before = await Effect.runPromise(scenarios.runtimeSeed(open, "client"));
  const after = await Effect.runPromise(scenarios.runtimeRestore(open, "client"));

  expect(after.resent).toEqual(before.command);
  expect(after.remaining).toEqual([]);
});

it("preserves quarantine and journal bytes through corrupt cache and format failures", async () => {
  const namespace = "corruption";

  await Effect.runPromise(scenarios.seed(open, namespace));
  expect(await Effect.runPromise(scenarios.quarantine(open, namespace))).toBe("quarantined");
  const names = ExpoSqlite.databaseNames(namespace);
  const cache = new DatabaseSync(join(directory, names.cache));

  cache.prepare("UPDATE snapshots SET value = ?").run("invalid JSON");
  cache.close();
  const restored = await Effect.runPromise(scenarios.inspect(open, namespace));

  expect(restored.cache).toBeUndefined();
  expect(restored.rows).toEqual([scenarios.evidence]);
  const futureCache = new DatabaseSync(join(directory, names.cache));

  futureCache.exec("PRAGMA user_version = 3");
  expect(await Effect.runPromise(scenarios.unavailableCache(open, namespace))).toBe(
    "journal available",
  );
  expect(futureCache.prepare("PRAGMA user_version").get()?.user_version).toBe(3);
  expect(futureCache.prepare("SELECT value FROM snapshots").get()?.value).toBe("invalid JSON");
  futureCache.close();
  const journal = new DatabaseSync(join(directory, names.journal));
  const unknownFormat = '{"format":2}';

  journal.prepare("UPDATE records SET value = ?").run(unknownFormat);
  await expect(Effect.runPromise(scenarios.inspect(open, namespace))).rejects.toThrow(
    "retain the database",
  );
  expect(journal.prepare("SELECT value FROM records").get()?.value).toBe(unknownFormat);
  journal.exec("PRAGMA user_version = 2");
  await expect(Effect.runPromise(scenarios.inspect(open, namespace))).rejects.toThrow(
    "Persistence operation failed",
  );
  expect(journal.prepare("PRAGMA user_version").get()?.user_version).toBe(2);
  journal.close();
});

it("migrates only the legacy cache and prevents old connections from restoring its rows", async () => {
  const namespace = "cache-migration";
  const names = ExpoSqlite.databaseNames(namespace);
  const journal = new DatabaseSync(join(directory, names.journal));
  const oldCache = new DatabaseSync(join(directory, names.cache));

  const evidence = JSON.stringify({
    format: 1,
    generation: 3,
    revision: 7,
    rows: [scenarios.evidence],
  });

  try {
    journal.exec(
      "CREATE TABLE records (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL); PRAGMA user_version = 1;",
    );
    journal.prepare("INSERT INTO records VALUES (?, ?)").run('["alice"]', evidence);
    oldCache.exec(
      "CREATE TABLE records (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL); PRAGMA user_version = 1;",
    );
    const oldWrite = oldCache.prepare("INSERT INTO records VALUES (?, ?)");

    oldWrite.run('["alice",1]', '{"format":1,"rows":[]}');
    oldWrite.run('["alice",3]', '{"format":1,"rows":[]}');
    const restored = await Effect.runPromise(scenarios.inspect(open, namespace));

    expect(restored.cache).toBeUndefined();
    expect(restored.rows).toEqual([scenarios.evidence]);
    expect(oldCache.prepare("PRAGMA user_version").get()?.user_version).toBe(2);
    expect(oldCache.prepare("SELECT * FROM snapshots").all()).toEqual([]);
    expect(() => oldWrite.run('["alice",4]', '{"format":1,"rows":[]}')).toThrow("no such table");
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const storage = yield* open({ namespace, actorId: "alice" });

          yield* storage.snapshotCache.put(scenarios.evidence.address, { value: 7 });
        }),
      ),
    );
    expect(await Effect.runPromise(scenarios.restore(open, namespace))).toBe("restored");
    const saved = oldCache.prepare("SELECT key, value FROM snapshots").all();

    expect(saved).toHaveLength(1);
    expect(saved[0]?.key).toBe('["alice"]');
    expect(JSON.parse(saved[0]?.value as string)).toMatchObject({ format: 2, generation: 3 });
    expect(journal.prepare("PRAGMA user_version").get()?.user_version).toBe(1);
    expect(journal.prepare("SELECT value FROM records").get()?.value).toBe(evidence);
  } finally {
    oldCache.close();
    journal.close();
  }
});
