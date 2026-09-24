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

it("rolls back conflicting journal writes and fences retired IndexedDB connections", async () => {
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

it("fences a retired persistence handle in another browser tab", async () => {
  const other = await page.context().newPage();

  try {
    await other.goto(server.resolvedUrls!.local[0]);
    await page.evaluate(() => window.persistence.hold("tabs"));
    await other.evaluate(() => window.persistence.hold("tabs"));
    await other.evaluate(() => window.persistence.wipe());
    expect(await page.evaluate(() => window.persistence.writeHeld())).toBe("StaleGeneration");
    await page.evaluate(() => window.persistence.closeHeld());
    await other.evaluate(() => window.persistence.closeHeld());
  } finally {
    await other.close();
  }
});
