import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { Effect } from "effect";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

import * as scenarios from "../../sync/test/persistence-scenarios.ts";
import { ExpoSqlite } from "../src/index.ts";

let directory: string;

beforeAll(async () => {
  directory = await mkdtemp(join(tmpdir(), "sync-sqlite-"));
});
afterAll(async () => {
  await rm(directory, { recursive: true, force: true });
});

const open: scenarios.Open = (options) =>
  ExpoSqlite.open({
    ...options,
    openDatabase: (name) =>
      Effect.sync(() => {
        const db = new DatabaseSync(join(directory, name));

        return {
          execAsync: async (sql) => {
            db.exec(sql);
          },
          getFirstAsync: async <T>(sql: string, ...params: Array<string>) =>
            (db.prepare(sql).get(...params) ?? null) as T | null,
          runAsync: async (sql, ...params) => db.prepare(sql).run(...params),
          closeAsync: async () => {
            db.close();
          },
        } satisfies ExpoSqlite.Database;
      }),
  });

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

  cache.prepare("UPDATE records SET value = ?").run("invalid JSON");
  cache.close();
  const restored = await Effect.runPromise(scenarios.inspect(open, namespace));

  expect(restored.cache).toBeUndefined();
  expect(restored.rows).toEqual([scenarios.evidence]);
  const futureCache = new DatabaseSync(join(directory, names.cache));

  futureCache.exec("PRAGMA user_version = 2");
  futureCache.close();
  expect(await Effect.runPromise(scenarios.unavailableCache(open, namespace))).toBe(
    "journal available",
  );
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
