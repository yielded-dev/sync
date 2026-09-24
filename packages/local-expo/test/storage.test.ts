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

it("rolls back conflicting journal writes and fences retired SQLite connections", async () => {
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
