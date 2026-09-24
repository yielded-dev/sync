import { chromium, type Browser, type Page } from "playwright";
import { createServer, type ViteDevServer } from "vite-plus";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

let server: ViteDevServer;
let browser: Browser;
let page: Page;

beforeAll(async () => {
  server = await createServer({
    configFile: false,
    root: import.meta.dirname + "/..",
    server: { host: "127.0.0.1", port: 0 },
  });
  await server.listen();
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();

  page = await context.newPage();
  await page.goto(server.resolvedUrls!.local[0]);
});
afterAll(async () => {
  await browser?.close();
  await server?.close();
});

const run = (scenario: Parameters<typeof window.persistence.run>[0], namespace: string) =>
  page.evaluate(({ scenario, namespace }) => window.persistence.run(scenario, namespace), {
    scenario,
    namespace,
  });

it("retains exact evidence and snapshots across a page reload", async () => {
  await run("seed", "reload");
  await page.reload();
  expect(await run("restore", "reload")).toBe("restored");
});

it("commits atomically, evicts only cache, and fences independent connections", async () => {
  expect(await run("exercise", "operations")).toContain("generation fencing");
  expect(await run("concurrent", "concurrent")).toBe("conflict and interruption rollback");
});

it("background flushes and retries the exact command after reload", async () => {
  const before = await run("runtimeSeed", "client");

  await page.reload();
  const after = await run("runtimeRestore", "client");

  expect((after as { resent: unknown }).resent).toEqual((before as { command: unknown }).command);
  expect((after as { remaining: unknown }).remaining).toEqual([]);
});

it("quarantines unknown intent formats and rebuilds only corrupt cache records", async () => {
  await run("seed", "corrupt-cache");
  expect(await run("quarantine", "corrupt-cache")).toBe("quarantined");
  await page.evaluate(() => window.persistence.corrupt("corrupt-cache", "cache", "invalid JSON"));
  const restored = (await run("inspect", "corrupt-cache")) as { cache: unknown; rows: unknown[] };

  expect(restored.cache).toBeUndefined();
  expect(restored.rows).toHaveLength(1);
  await page.evaluate(() => window.persistence.upgradeCache("corrupt-cache"));
  expect(await run("unavailableCache", "corrupt-cache")).toBe("journal available");
  expect(await page.evaluate(() => window.persistence.records("corrupt-cache", "cache"))).toEqual({
    version: 3,
    keys: ['["alice"]'],
    values: ["invalid JSON"],
  });
  await page.evaluate(() => window.persistence.corrupt("corrupt-cache", "journal", '{"format":2}'));
  await expect(run("inspect", "corrupt-cache")).rejects.toThrow("retain the database");
});

it("resets only the known legacy cache and retains exact journal evidence", async () => {
  const seeded = await page.evaluate(() => window.persistence.seedLegacy("cache-migration"));
  const restored = (await run("inspect", "cache-migration")) as { cache: unknown; rows: unknown[] };

  expect(restored.cache).toBeUndefined();
  expect(restored.rows).toEqual([seeded.evidence]);
  expect(await page.evaluate(() => window.persistence.records("cache-migration", "cache"))).toEqual(
    {
      version: 2,
      keys: [],
      values: [],
    },
  );
  await page.evaluate(() => window.persistence.saveSnapshot("cache-migration"));
  expect(await run("restore", "cache-migration")).toBe("restored");
  const cache = await page.evaluate(() => window.persistence.records("cache-migration", "cache"));

  expect(cache.keys).toEqual(['["alice"]']);
  expect(cache.values).toHaveLength(1);
  expect(JSON.parse(cache.values[0] as string)).toMatchObject({ format: 2, generation: 3 });
  expect(
    await page.evaluate(() => window.persistence.records("cache-migration", "journal")),
  ).toEqual({
    version: 1,
    keys: ['["alice"]'],
    values: [seeded.journal],
  });
});

it("fences a handle in another browser tab and rejects operations after scope close", async () => {
  const other = await page.context().newPage();

  try {
    await other.goto(server.resolvedUrls!.local[0]);
    await page.evaluate(() => window.persistence.hold("tabs"));
    await other.evaluate(() => window.persistence.hold("tabs"));
    await other.evaluate(() => window.persistence.wipe());
    expect(await page.evaluate(() => window.persistence.writeHeld())).toBe("StaleGeneration");
    await page.evaluate(() => window.persistence.closeHeld());
    expect(await page.evaluate(() => window.persistence.writeHeld())).toBe("Unavailable");
    await other.evaluate(() => window.persistence.closeHeld());
  } finally {
    await other.close();
  }
});
