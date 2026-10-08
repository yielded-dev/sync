import { cloudflare } from "@cloudflare/vite-plugin";
import { cloudflareTest } from "@cloudflare/vitest-plugin";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite-plus";

export default defineConfig(({ mode }) => ({
  plugins:
    mode === "test"
      ? [
          cloudflareTest({
            main: "./src/worker.ts",
            wrangler: { configPath: "./wrangler.jsonc" },
          }),
        ]
      : [...react(), ...cloudflare()],
  server: { host: "127.0.0.1" },
  test: {
    // Vitest v4 compatibility: preserve mock call history.
    // Remove after tests no longer rely on calls from setup or earlier tests.
    // https://viteplus.dev/guide/vitest-v5#remove-unneeded-compatibility-settings
    // https://vitest.dev/guide/migration/#clearmocks-is-enabled-by-default
    clearMocks: false,
    cache: false,
    silent: "passed-only",
  },
  run: {
    tasks: {
      "check:worker": {
        command: "wrangler deploy --dry-run",
        cache: {
          // Keep dist and .wrangler/deploy as inputs: the preceding Vite build
          // creates the Worker/config this validates. Only temporary output is ignored.
          input: [
            { auto: true },
            "*",
            "dist/**",
            ".wrangler/deploy/**",
            { pattern: "!examples/list-board", base: "workspace" },
            "!.wrangler",
            "!.wrangler/tmp",
            "!.wrangler/tmp/**",
            { pattern: "bun.lock", base: "workspace" },
            { pattern: "!**/node_modules", base: "workspace" },
            { pattern: "!**/node_modules/.vite*", base: "workspace" },
            { pattern: "!**/node_modules/.vite*/**", base: "workspace" },
          ],
          output: [],
        },
      },
    },
  },
}));
