import { chromium, type Browser, type Page } from "playwright";
import { createServer, type ViteDevServer } from "vite-plus";
import { afterAll, beforeAll, expect, it } from "vite-plus/test";

let server: ViteDevServer;
let browser: Browser;
let page: Page;

beforeAll(async () => {
  server = await createServer({
    configFile: false,
    root: import.meta.dirname + "/fixtures",
    // Vite's default dependency scan excludes test directories.
    optimizeDeps: { entries: ["host.ts"] },
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

it("rolls back conflicting IndexedDB journal writes", async () => {
  expect(await run("concurrent", "concurrent")).toBe("atomic conflict rollback");
});

it("background flushes and retries the exact command after reload", async () => {
  const before = await run("runtimeSeed", "client");

  await page.reload();
  const after = await run("runtimeRestore", "client");

  expect((after as { resent: unknown }).resent).toEqual((before as { command: unknown }).command);
  expect((after as { remaining: unknown }).remaining).toEqual([]);
});

// Requested red/green seam: cached offline admission through Atom survives a browser reload.
it("admits an offline Atom edit and settles the exact command after reload", async () => {
  const before = await run("atomOffline", "atom-offline");

  await page.reload();
  const after = await run("runtimeRestore", "atom-offline");

  expect((after as { resent: unknown }).resent).toEqual(
    (before as { command: { command: unknown } }).command.command,
  );
  expect((after as { remaining: unknown }).remaining).toEqual([]);
});

it("fences a legacy journal generation changed by another browser tab", async () => {
  const other = await page.context().newPage();

  try {
    await other.goto(server.resolvedUrls!.local[0]);
    await page.evaluate(() => window.persistence.hold("tabs"));
    await other.evaluate(() => window.persistence.hold("tabs"));
    await other.evaluate(() => window.persistence.retireLegacyGeneration("tabs"));
    expect(await page.evaluate(() => window.persistence.writeHeld())).toBe("StaleGeneration");
    await page.evaluate(() => window.persistence.closeHeld());
    await other.evaluate(() => window.persistence.closeHeld());
  } finally {
    await other.close();
  }
});

// https://github.com/yielded-dev/sync/issues/60: shared storage needs scoped source ownership.
it("rejects a second tab's source coordinator and retains its journal across owner closure", async () => {
  const other = await page.context().newPage();

  try {
    await other.goto(server.resolvedUrls!.local[0]);
    const first = await page.evaluate(() => window.persistence.ownSource("ownership", true));

    expect(first._tag).toBe("Success");
    const second = await other.evaluate(() => window.persistence.ownSource("ownership"));

    expect(second).toMatchObject({ _tag: "Failure", failure: { reason: "Storage" } });
    await other.evaluate(() => window.persistence.closeOwner());
    await page.reload();
    const next = await other.evaluate(() => window.persistence.ownSource("ownership"));

    expect(next).toEqual(first);
  } finally {
    await other.evaluate(() => window.persistence.closeOwner());
    await page.evaluate(() => window.persistence.closeOwner());
    await other.close();
  }
});
